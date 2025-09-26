"""
Simple configuration for consumer
"""

import os
from typing import Optional

from pydantic import BaseSettings, Field


class Config(BaseSettings):
    """Simple configuration with environment variables"""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")
    KAFKA_GROUP_ID: str = Field(..., env="KAFKA_GROUP_ID")
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    # Flink Configuration
    FLINK_PARALLELISM: int = Field(default=1, env="FLINK_PARALLELISM")

    class Config:
        env_file = ".env"
        case_sensitive = True


# Global config instance
config = Config()