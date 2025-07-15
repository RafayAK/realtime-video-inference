import asyncio
import os
import cv2
import tempfile
import json
import time
from fastapi import APIRouter, WebSocket, UploadFile, File, HTTPException, Request
from enhanced_pipeline.config import settings

# Create a FastAPI router for the endpoints
router = APIRouter()

def get_static_images_dir():
    """Get the path to static images directory"""
    current_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    return os.path.join(current_dir, "static", "images")

# Endpoint to upload an image (keeping for backward compatibility)
@router.post("/upload-image/")
async def upload_image(request: Request, file: UploadFile = File(...)):
    # Ensure images directory exists
    images_dir = get_static_images_dir()
    os.makedirs(images_dir, exist_ok=True)
    
    # Save the file
    file_location = os.path.join(images_dir, file.filename)
    print(f"Saving file to {file_location}")
    with open(file_location, "wb") as f:
        content = await file.read()
        f.write(content)

    # Send file name to Redpanda
    frame_data = {
        "type": "single_image",
        "filename": file.filename,
        "timestamp": time.time()
    }
    await request.app.producer.send(settings.request_topic, value=json.dumps(frame_data).encode('utf-8'))
    return {"filename": file.filename}


# Endpoint to upload a video and extract frames
@router.post("/upload-video/")
async def upload_video(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(('.mp4', '.avi', '.mov', '.mkv', '.webm')):
        raise HTTPException(status_code=400, detail="Invalid video format")

    # Save the video file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video:
        content = await file.read()
        temp_video.write(content)
        temp_video_path = temp_video.name

    try:
        # Extract frames and send them to Redpanda
        await process_video_frames(request, temp_video_path, file.filename)
        return {"message": "Video processing started", "filename": file.filename}
    finally:
        # Clean up temporary file
        os.unlink(temp_video_path)


# Endpoint to process individual frames (for webcam)
@router.post("/process-frame/")
async def process_frame(request: Request, frame: UploadFile = File(...)):
    # Ensure images directory exists
    images_dir = get_static_images_dir()
    os.makedirs(images_dir, exist_ok=True)
    
    # Save the frame
    frame_filename = f"frame_{int(time.time() * 1000)}.jpg"
    file_location = os.path.join(images_dir, frame_filename)

    with open(file_location, "wb") as f:
        content = await frame.read()
        f.write(content)

    # Send frame data to Redpanda
    frame_data = {
        "type": "webcam_frame",
        "filename": frame_filename,
        "timestamp": time.time()
    }
    await request.app.producer.send(settings.request_topic, value=json.dumps(frame_data).encode('utf-8'))
    return {"status": "frame_processed", "filename": frame_filename}


async def process_video_frames(request: Request, video_path: str, original_filename: str):
    """Extract frames from video and send them to Redpanda"""
    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_interval = max(1, int(fps / settings.video_frame_rate))  # Use configured frame rate
    
    images_dir = get_static_images_dir()
    os.makedirs(images_dir, exist_ok=True)

    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break

            # Process every nth frame to avoid overwhelming the system
            if frame_count % frame_interval == 0:
                # Save frame
                frame_filename = f"video_{original_filename}_frame_{frame_count:06d}.jpg"
                frame_path = os.path.join(images_dir, frame_filename)

                cv2.imwrite(frame_path, frame)

                # Send frame data to Redpanda
                frame_data = {
                    "type": "video_frame",
                    "filename": frame_filename,
                    "original_video": original_filename,
                    "frame_number": frame_count,
                    "timestamp": time.time()
                }
                await request.app.producer.send(settings.request_topic, value=json.dumps(frame_data).encode('utf-8'))

                # Small delay to prevent overwhelming the system
                await asyncio.sleep(0.1)

            frame_count += 1

    finally:
        cap.release()


# Cleanup endpoint to remove old frames
@router.post("/cleanup/")
async def cleanup_frames():
    """Clean up old frame files to save disk space"""
    try:
        images_dir = get_static_images_dir()
        
        if not os.path.exists(images_dir):
            return {"message": "No images directory found"}

        cleaned_count = 0
        for filename in os.listdir(images_dir):
            if filename.startswith(("frame_", "video_")):
                file_path = os.path.join(images_dir, filename)
                file_age = time.time() - os.path.getctime(file_path)

                # Remove files older than configured cleanup interval
                if file_age > settings.cleanup_interval:
                    os.remove(file_path)
                    cleaned_count += 1

        return {"message": f"Cleaned up {cleaned_count} old frame files"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")