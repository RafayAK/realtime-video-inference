import os
import logging
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from enhanced_pipeline.config import settings
from enhanced_pipeline.api.endpoints import router
from enhanced_pipeline.api.websockets import websocket_endpoint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def startup(app: FastAPI):
    """Startup and shutdown event handler"""
    logger.info("Starting up enhanced pipeline...")
    
    # Initialize Kafka producer
    app.producer = AIOKafkaProducer(
        bootstrap_servers=settings.redpanda_server,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
    )
    await app.producer.start()
    logger.info("Kafka producer started")

    # Initialize Kafka consumer
    app.consumer = AIOKafkaConsumer(
        settings.reply_topic,
        bootstrap_servers=settings.redpanda_server,
        group_id="frame-reply-group",
        auto_offset_reset="latest",
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
    )
    await app.consumer.start()
    logger.info("Kafka consumer started")

    try:
        yield
    finally:
        logger.info("Shutting down enhanced pipeline...")
        await app.producer.stop()
        await app.consumer.stop()
        logger.info("Kafka connections closed")

app = FastAPI(
    title="Enhanced Video Inference Pipeline",
    description="Real-time video inference with Triton and Kafka streaming",
    version="1.0.0",
    lifespan=startup
)

# Ensure static directory exists
static_dir = os.path.join(os.path.dirname(__file__), "static")
images_dir = os.path.join(static_dir, "images")
os.makedirs(images_dir, exist_ok=True)

# Mount the static directory to serve static files
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Include API routes
app.include_router(router, prefix="/api", tags=["inference"])

# Root endpoint - redirect to the index.html file
@app.get("/")
async def root():
    return FileResponse(os.path.join(static_dir, "index.html"))

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_handler(websocket: WebSocket):
    await websocket_endpoint(websocket, app)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check if Kafka connections are alive
        producer_ready = app.producer is not None and not app.producer._closed
        consumer_ready = app.consumer is not None and not app.consumer._closed
        
        return {
            "status": "healthy" if producer_ready and consumer_ready else "unhealthy",
            "producer_ready": producer_ready,
            "consumer_ready": consumer_ready,
            "settings": {
                "redpanda_server": settings.redpanda_server,
                "request_topic": settings.request_topic,
                "reply_topic": settings.reply_topic
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(
#         "main:app",
#         host="0.0.0.0",
#         port=8000,
#         reload=True,
#         log_level="info"
#     )