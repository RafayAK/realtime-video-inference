from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Config(BaseSettings):
    """
    Configuration settings for the enhanced pipeline.
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Kafka/Redpanda settings
    redpanda_server: str = "localhost:19092"
    request_topic: str = "frame-request"
    reply_topic: str = "frame-reply"
    
    # Triton settings
    triton_url: str = "localhost:8001"
    model_name: str = "resnet18"
    
    # Application settings
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    
    # File processing settings
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    cleanup_interval: int = 300  # 5 minutes
    video_frame_rate: int = 10  # Process ~10 frames per second
    
    # Logging
    log_level: str = "INFO"


settings = Config()