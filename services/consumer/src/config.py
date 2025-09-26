"""
Simple configuration for consumer
"""

import os
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field


class Config(BaseSettings):
    """Simple configuration with environment variables"""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")
    KAFKA_GROUP_ID: str = Field(..., env="KAFKA_GROUP_ID")
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    # ClickHouse Configuration
    CLICKHOUSE_HOST: str = Field(..., env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(..., env="CLICKHOUSE_PORT")
    CLICKHOUSE_DATABASE: str = Field(..., env="CLICKHOUSE_DATABASE")
    CLICKHOUSE_USER: str = Field(..., env="CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = Field(..., env="CLICKHOUSE_PASSWORD")
    CLICKHOUSE_SECURE: bool = Field(default=True, env="CLICKHOUSE_SECURE")

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


# Global config instance
config = Config()