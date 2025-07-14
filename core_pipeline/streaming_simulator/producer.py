import cv2
import time
import json
import base64
import argparse
from loguru import logger
import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import NoBrokersAvailable

# Configure logging
logger.add("producer.log", rotation="1MB", level="DEBUG")

class VideoFrameProducer:
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092", topic: str = "video_frames"):
        """Initialize the producer with Kafka settings

        Args:
            kafka_bootstrap_servers (str): Kafka bootstrap servers. Default is "localhost:9092".
            topic (str): Kafka topic to send frames to. Default is "video_frames".
        """

        self.topic = topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.frame_count = 0
        self.start_time = None

    async def initialize(self):
        """Initialize Kafka producer"""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.producer.start()
            logger.info(f"Connected to Kafka at {self.kafka_bootstrap_servers}")
        except NoBrokersAvailable:
            logger.error(f"Could not connect to Kafka at {self.kafka_bootstrap_servers}")
            raise


    @staticmethod
    def encode_frame(frame: cv2.Mat) -> str:
        """Encode frame as JPEG and convert to base64

        Args:
            frame (cv2.Mat): The video frame to encode.

        Returns:
            str: Base64 encoded JPEG image.

        """
        _, buffer = cv2.imencode('.jpg', frame)
        frame_bytes = buffer.tobytes()
        return base64.b64encode(frame_bytes).decode('utf-8')
    
    async def send_frame(self, frame: cv2.Mat, frame_id: str):
        """Send a single frame to Kafka

        Args:
            frame (cv2.Mat): The video frame to send.
            frame_id (str): Unique identifier for the frame.

        """
        try:
            # Encode frame
            encoded_frame = self.encode_frame(frame)
            
            # Create message
            message = {
                "frame_id": frame_id,
                "timestamp": time.time(),
                "image_data": encoded_frame,
                "format": "jpeg_base64"
            }
            
            # Send to Kafka
            await self.producer.send_and_wait(self.topic, value=message)
            self.frame_count += 1
            
            if self.frame_count % 10 == 0:
                logger.info(f"Sent {self.frame_count} frames")
                
        except Exception as e:
            logger.error(f"Error sending frame {frame_id}: {str(e)}")
    
    async def stream_video_file(self, video_path: str, max_frames: int | None = None, fps_limit: int = 10)-> None:
        """Stream frames from a video file

        Args:
            video_path (str): Path to the video file.
            max_frames (int | None): Maximum number of frames to send. If None, send all frames.
            fps_limit (int): Frames per second limit for streaming. Default is 10.

        """
        logger.info(f"Starting to stream video: {video_path}")
        self.start_time = time.time()
        
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            logger.error(f"Could not open video file: {video_path}")
            return
        
        frame_interval = 1.0 / fps_limit
        last_frame_time = 0
        
        try:
            frame_id = 0
            while cap.isOpened():
                ret, frame = cap.read()
                if not ret:
                    break
                
                current_time = time.time()
                
                # Respect FPS limit
                if current_time - last_frame_time >= frame_interval:
                    await self.send_frame(frame, f"video_frame_{frame_id}")
                    last_frame_time = current_time
                    frame_id += 1
                
                # Check max frames limit
                if max_frames and frame_id >= max_frames:
                    break
                    
        finally:
            cap.release()
            await self.close()
            
        total_time = time.time() - self.start_time
        logger.info(f"Streaming complete. Sent {self.frame_count} frames in {total_time:.2f} seconds")
        logger.info(f"Average FPS: {self.frame_count / total_time:.2f}")
    
    async def stream_webcam(self, camera_id: int = 0, duration_seconds: int = 30, fps_limit: int = 10)-> None:
        """Stream frames from webcam

        Args:
            camera_id (int): Camera ID to use. Default is 0.
            duration_seconds (int): Duration to stream in seconds. Default is 30.
            fps_limit (int): Frames per second limit for streaming. Default is 10.

        """
        logger.info(f"Starting webcam stream for {duration_seconds} seconds")
        self.start_time = time.time()
        print(f"start time: {self.start_time}")
        
        cap = cv2.VideoCapture(camera_id)
        if not cap.isOpened():
            logger.error(f"Could not open camera {camera_id}")
            return
        
        frame_interval = 1.0 / fps_limit
        last_frame_time = 0
        
        try:
            frame_id = 0
            while time.time() - self.start_time < duration_seconds:
                ret, frame = cap.read()
                if not ret:
                    continue
                
                current_time = time.time()
                
                # Respect FPS limit
                if current_time - last_frame_time >= frame_interval:
                    await self.send_frame(frame, f"webcam_frame_{frame_id}")
                    last_frame_time = current_time
                    frame_id += 1
                    
        except KeyboardInterrupt:
            logger.info("Webcam streaming interrupted by user")
        finally:
            cap.release()
            await self.close()
            
        total_time = time.time() - self.start_time
        logger.info(f"Webcam streaming complete. Sent {self.frame_count} frames in {total_time:.2f} seconds")
        logger.info(f"Average FPS: {self.frame_count / total_time:.2f}")
    
    async def close(self):
        """Close Kafka producer"""
        if self.producer:
            await self.producer.stop()

async def main():
    parser = argparse.ArgumentParser(description="Video frame producer for Kafka")
    parser.add_argument("--mode", choices=["video", "webcam"], required=True,
                      help="Streaming mode: video file or webcam")
    parser.add_argument("--video-path", type=str,
                      help="Path to video file (required for video mode)")
    parser.add_argument("--camera-id", type=int, default=0,
                      help="Camera ID for webcam mode (default: 0)")
    parser.add_argument("--duration", type=int, default=30,
                      help="Duration in seconds for webcam mode (default: 30)")
    parser.add_argument("--fps", type=int, default=10,
                      help="FPS limit for streaming (default: 10)")
    parser.add_argument("--max-frames", type=int,
                      help="Maximum number of frames to send")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092",
                      help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", type=str, default="video_frames",
                      help="Kafka topic name (default: video_frames)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == "video" and not args.video_path:
        parser.error("--video-path is required for video mode")
    
    # Create producer
    try:
        producer = VideoFrameProducer(
            kafka_bootstrap_servers=args.kafka_servers,
            topic=args.topic
        )
        
        # Initialize producer
        await producer.initialize()
        
        # Start streaming
        if args.mode == "video":
            await producer.stream_video_file(
                video_path=args.video_path,
                max_frames=args.max_frames,
                fps_limit=args.fps
            )
        elif args.mode == "webcam":
            await producer.stream_webcam(
                camera_id=args.camera_id,
                duration_seconds=args.duration,
                fps_limit=args.fps
            )
            
    except Exception as e:
        logger.error(f"Producer failed: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(asyncio.run(main()))
