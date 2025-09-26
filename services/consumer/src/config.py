"""
Configuration management for consumer service
"""
import os
from typing import Optional
from pydantic import BaseSettings, Field


class ConsumerConfig(BaseSettings):
    """Consumer configuration with validation"""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., description="Kafka bootstrap servers")
    KAFKA_TOPIC: str = Field(..., description="Kafka topic to consume from")
    KAFKA_GROUP_ID: str = Field(..., description="Kafka consumer group ID")
    KAFKA_API_KEY: str = Field(..., description="Kafka API key")
    KAFKA_API_SECRET: str = Field(..., description="Kafka API secret")

    # Flink Configuration
    FLINK_PARALLELISM: int = Field(default=2, ge=1, le=10, description="Flink parallelism")
    FLINK_CHECKPOINT_INTERVAL: int = Field(default=60000, ge=1000, description="Checkpoint interval (ms)")
    FLINK_WATERMARK_DELAY: int = Field(default=5000, ge=1000, description="Watermark delay (ms)")

    # ClickHouse Configuration
    CLICKHOUSE_HOST: str = Field(..., description="ClickHouse host")
    CLICKHOUSE_PORT: int = Field(default=8123, ge=1, le=65535, description="ClickHouse port")
    CLICKHOUSE_DATABASE: str = Field(default="ecommerce", description="ClickHouse database")
    CLICKHOUSE_USER: str = Field(default="default", description="ClickHouse user")
    CLICKHOUSE_PASSWORD: str = Field(default="", description="ClickHouse password")

    # Processing Configuration
    WINDOW_SIZE_MINUTES: int = Field(default=1, ge=1, le=60, description="Window size in minutes")
    SLIDING_INTERVAL_SECONDS: int = Field(default=30, ge=1, le=300, description="Sliding interval in seconds")

    # Monitoring
    METRICS_PORT: int = Field(default=8081, ge=1024, le=65535, description="Metrics server port")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")

    # Advanced Settings
    ENABLE_EXACTLY_ONCE: bool = Field(default=True, description="Enable exactly-once processing")
    BACKPRESSURE_BUFFER_SIZE: int = Field(default=1000, ge=100, le=10000, description="Backpressure buffer size")

    class Config:
        env_prefix = "CONSUMER_"
        env_file = ".env"
        case_sensitive = False


def get_config() -> ConsumerConfig:
    """Get configuration instance"""
    return ConsumerConfig()


# Global config instance
config = get_config()