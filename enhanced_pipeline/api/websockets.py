import asyncio
import json
import logging
from fastapi import WebSocket, WebSocketDisconnect
from typing import List
from enhanced_pipeline.config import settings

logger = logging.getLogger(__name__)

class ConnectionManager:
    """Manages WebSocket connections and handles broadcasting"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.consumer_tasks = {}
        
    async def connect(self, websocket: WebSocket, app):
        """Accept a new WebSocket connection and start consuming messages"""
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # Start a consumer task for this connection
        task = asyncio.create_task(
            self._consume_messages_for_connection(websocket, app)
        )
        self.consumer_tasks[websocket] = task
        logger.info(f"New WebSocket connection established. Total connections: {len(self.active_connections)}")
        
    async def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection and stop its consumer task"""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            
        # Cancel the consumer task for this connection
        if websocket in self.consumer_tasks:
            task = self.consumer_tasks[websocket]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.consumer_tasks[websocket]
            
        logger.info(f"WebSocket connection closed. Total connections: {len(self.active_connections)}")
        
    async def _consume_messages_for_connection(self, websocket: WebSocket, app):
        """Consume messages from Kafka and send to the specific WebSocket connection"""
        try:
            async for msg in app.consumer:
                if websocket not in self.active_connections:
                    # Connection was closed, stop consuming
                    break
                    
                try:
                    # Try to parse as JSON first
                    data = json.loads(msg.value.decode('utf-8'))
                    if isinstance(data, dict):
                        # Send the full JSON data for rich client-side processing
                        await websocket.send_text(json.dumps(data))
                    else:
                        # Fallback for non-JSON messages
                        await websocket.send_text(msg.value.decode('utf-8'))
                except (json.JSONDecodeError, KeyError):
                    # Handle legacy string messages
                    await websocket.send_text(msg.value.decode('utf-8'))
                except Exception as e:
                    logger.error(f"Error sending message to WebSocket: {e}")
                    break
                    
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled for WebSocket connection")
        except Exception as e:
            logger.error(f"Error in consumer task: {e}")

# Global connection manager instance
manager = ConnectionManager()

async def websocket_endpoint(websocket: WebSocket, app):
    """WebSocket endpoint to receive processed frames"""
    try:
        await manager.connect(websocket, app)
        
        # Keep the connection alive and handle disconnection
        while True:
            try:
                # Wait for any message from client (ping/pong, etc.)
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.error(f"Error in WebSocket endpoint: {e}")
                break
                
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected normally")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(websocket)
