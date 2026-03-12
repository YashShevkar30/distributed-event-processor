"""
Configuration management for the Distributed Event Processor.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Consumer
    consumer_group: str = "event-processors"
    consumer_name: str = "worker-1"
    stream_name: str = "events"
    dlq_stream_name: str = "events-dlq"
    batch_size: int = 10
    block_ms: int = 5000
    max_retries: int = 3

    # Metrics
    metrics_port: int = 9100

    # Logging
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()

# TODO: Add more robust error handling here
