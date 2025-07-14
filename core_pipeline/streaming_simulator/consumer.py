import grpc
import json
import base64
import time
from loguru import logger
import argparse
import asyncio
from collections import defaultdict
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import NoBrokersAvailable

# Import generated gRPC classes
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../inference_service'))
import inference_pb2
import inference_pb2_grpc

# Configure logging
logger.add("consumer.log", rotation="1MB", level="DEBUG")

class InferenceConsumer:
    def __init__(self, kafka_bootstrap_servers:str="localhost:9092",
                 kafka_topic:str="video_frames", grpc_server:str="localhost:50051"):
        """Initialize the consumer
        Args:
            kafka_bootstrap_servers (str): Kafka bootstrap servers
            kafka_topic (str): Kafka topic to consume
            grpc_server (str): gRPC inference server address
        """

        self.kafka_topic = kafka_topic
        self.grpc_server = grpc_server
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.consumer = None
        self.grpc_channel = None
        self.inference_stub = None
        
        # Performance metrics
        self.metrics = {
            "frames_processed": 0,
            "total_inference_time": 0,
            "start_time": None,
            "inference_times": [],
            "errors": 0,
            "active_time": 0,
            "last_frame_time": None
        }
    
    async def initialize(self):
        """Initialize async components"""
        # Initialize Kafka consumer
        await self._init_kafka_consumer(self.kafka_bootstrap_servers)
        
        # Initialize gRPC client
        self._init_grpc_client()
    
    async def _init_kafka_consumer(self, bootstrap_servers:str) -> None:
        """Initialize Kafka consumer

        Args:
            bootstrap_servers (str): Kafka bootstrap servers

        """
        try:
            self.consumer = AIOKafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='latest',
                group_id='inference_consumer_group'
            )
            await self.consumer.start()
            logger.info(f"Connected to Kafka topic '{self.kafka_topic}' at {bootstrap_servers}")
        except NoBrokersAvailable:
            logger.error(f"Could not connect to Kafka at {bootstrap_servers}")
            raise
    
    def _init_grpc_client(self):
        """Initialize gRPC client"""
        try:
            self.grpc_channel = grpc.insecure_channel(self.grpc_server)
            self.inference_stub = inference_pb2_grpc.InferenceServiceStub(self.grpc_channel)
            
            # Test connection with health check
            health_request = inference_pb2.HealthRequest()
            response = self.inference_stub.HealthCheck(health_request, timeout=5)
            
            if response.healthy:
                logger.info(f"Connected to gRPC inference service at {self.grpc_server}")
                logger.info(f"Service status: {response.status}")
            else:
                logger.warning(f"gRPC service not healthy: {response.status}")
                
        except grpc.RpcError as e:
            logger.error(f"Could not connect to gRPC server at {self.grpc_server}: {e}")
            raise

    @staticmethod
    def decode_frame(image_data: str) -> bytes | None:
        """Decode base64 image data to bytes

        Args:
            image_data (str): Base64 encoded image data

        Returns:
            bytes: Decoded image bytes, or None if decoding fails

        """
        try:
            return base64.b64decode(image_data)
        except Exception as e:
            logger.error(f"Error decoding image data: {e}")
            return None
    
    def classify_frame(self, frame_data: dict) -> dict | None:
        """Send frame to gRPC service for classification

        Args:
            frame_data (dict): Frame data containing image and metadata

        Returns:
            dict: Classification result containing predicted class, confidence, and timing info,
                  or None if an error occurs

        """
        try:
            # Decode image
            image_bytes = self.decode_frame(frame_data["image_data"])
            if image_bytes is None:
                return None
            
            # Create gRPC request
            request = inference_pb2.ImageRequest(
                image_data=image_bytes,
                image_id=frame_data["frame_id"]
            )
            
            # Call inference service
            start_time = time.time()
            response = self.inference_stub.ClassifyImage(request, timeout=10)
            end_time = time.time()
            
            # Update metrics
            inference_time = end_time - start_time
            self.update_metrics(inference_time)
            
            result = {
                "frame_id": frame_data["frame_id"],
                "predicted_class": response.predicted_class,
                "confidence": response.confidence,
                "inference_time": response.inference_time,
                "total_time": inference_time,
                "timestamp": frame_data["timestamp"]
            }
            
            logger.info(f"Frame {frame_data['frame_id']}: {response.predicted_class} "
                       f"(confidence: {response.confidence:.3f}, "
                       f"inference: {response.inference_time:.3f}s, "
                       f"total: {inference_time:.3f}s)")
            
            return result
            
        except grpc.RpcError as e:
            logger.error(f"gRPC error for frame {frame_data['frame_id']}: {e}")
            self.metrics["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Error processing frame {frame_data['frame_id']}: {e}")
            self.metrics["errors"] += 1
            return None
    
    def update_metrics(self, inference_time: float) -> None:
        """Update performance metrics

        Updates the metrics dictionary with the latest inference time, frame count,
        and active time. It also calculates the average inference time and keeps track
        of the last 100 inference times for moving averages.

        Args:
            inference_time (float): Time taken for inference in seconds

        """

        current_time = time.time()

        # Track active time vs idle time
        if self.metrics["last_frame_time"] is not None:
            gap = current_time - self.metrics["last_frame_time"]
            if gap < 1.0:  # Consider gaps less than 1 second as active time
                self.metrics["active_time"] += gap

        self.metrics["last_frame_time"] = current_time

        self.metrics["frames_processed"] += 1
        self.metrics["total_inference_time"] += inference_time
        self.metrics["inference_times"].append(inference_time)
        
        # Keep only last 100 measurements for moving averages
        if len(self.metrics["inference_times"]) > 100:
            self.metrics["inference_times"].pop(0)
    
    def print_metrics(self):
        """Print current performance metrics"""
        if self.metrics["start_time"] is None or self.metrics["frames_processed"] == 0:
            return
            
        total_time = time.time() - self.metrics["start_time"]
        frames = self.metrics["frames_processed"]
        active_time = self.metrics["active_time"]

        # Calculate derived metrics
        avg_fps = frames / total_time if total_time > 0 else 0
        active_fps = frames / active_time if active_time > 0 else 0
        avg_inference_time = self.metrics["total_inference_time"] / frames

        recent_times = self.metrics["inference_times"]
        recent_avg = sum(recent_times) / len(recent_times) if recent_times else 0

        logger.info("=" * 50)
        logger.info("PERFORMANCE METRICS")
        logger.info("=" * 50)

        # Throughput metrics
        logger.info("THROUGHPUT:")
        logger.info(f"  Frames processed: {frames}")
        logger.info(f"  Overall FPS: {avg_fps:.2f} (across {total_time:.1f}s total runtime)")
        logger.info(f"  Effective FPS: {active_fps:.2f} (across {active_time:.1f}s active time)")

        # Processing time metrics
        logger.info("PROCESSING TIME:")
        logger.info(f"  Average inference time: {avg_inference_time * 1000:.1f}ms")
        logger.info(f"  Recent average (last {len(recent_times)} frames): {recent_avg * 1000:.1f}ms")

        if recent_times:
            logger.info(f"  Range: {min(recent_times) * 1000:.1f}ms - {max(recent_times) * 1000:.1f}ms")

        # Error metrics
        logger.info("ERRORS:")
        logger.info(f"  Total errors: {self.metrics['errors']}")
        error_rate = self.metrics["errors"] / frames if frames > 0 else 0
        logger.info(f"  Error rate: {error_rate * 100:.2f}%")

        logger.info("=" * 50)
    
    async def consume_and_classify(self, duration_seconds:int=30)-> None:
        """Main consumer loop

        Continuously consumes frames from Kafka, sends them to the gRPC service for classification,
        and prints performance metrics periodically.

        Args:
            duration_seconds (int): Duration to run the consumer in seconds

        """
        logger.info(f"Starting frame consumption for {duration_seconds} seconds...")
        self.metrics["start_time"] = time.time()
        
        last_metrics_print = time.time()
        metrics_interval = 30  # Print metrics every 30 seconds
        
        try:
            end_time = time.time() + duration_seconds
            
            async for message in self.consumer:
                if time.time() >= end_time:
                    break
                    
                try:
                    # Decode message value
                    frame_data = json.loads(message.value.decode('utf-8'))
                    result = self.classify_frame(frame_data)
                    
                    # Print metrics periodically
                    current_time = time.time()
                    if current_time - last_metrics_print >= metrics_interval:
                        self.print_metrics()
                        last_metrics_print = current_time
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.metrics["errors"] += 1
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            await self.close()
        
        # Final metrics
        self.print_metrics()
        logger.info("Frame consumption complete")
    
    async def close(self):
        """Close connections"""
        if self.consumer:
            await self.consumer.stop()
        if self.grpc_channel:
            self.grpc_channel.close()

async def main():
    parser = argparse.ArgumentParser(description="Kafka consumer with gRPC inference")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092",
                      help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--kafka-topic", type=str, default="video_frames",
                      help="Kafka topic to consume (default: video_frames)")
    parser.add_argument("--grpc-server", type=str, default="localhost:50051",
                      help="gRPC inference server (default: localhost:50051)")
    parser.add_argument("--duration", type=int, default=30,
                      help="Duration to run consumer in seconds (default: 30)")
    
    args = parser.parse_args()
    
    try:
        consumer = InferenceConsumer(
            kafka_bootstrap_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            grpc_server=args.grpc_server
        )
        
        # Initialize async components
        await consumer.initialize()
        
        # Start consuming
        await consumer.consume_and_classify(duration_seconds=args.duration)
        
    except Exception as e:
        logger.error(f"Consumer failed: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(asyncio.run(main()))
