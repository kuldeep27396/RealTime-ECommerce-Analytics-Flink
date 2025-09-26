"""
Configuration management for producer service
Uses environment variables with sensible defaults
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class ProducerConfig(BaseSettings):
    """Producer configuration with validation"""

    # API Configuration
    API_URL: str = Field(
        default="https://clickstream-datagenerator-production.up.railway.app/stream/interactions",
        description="Clickstream data API endpoint"
    )
    API_RATE: int = Field(
        default=10000,
        ge=1,
        le=100000,
        description="Events per second to request from API"
    )
    API_DURATION: int = Field(
        default=60,
        ge=10,
        le=300,
        description="Duration in seconds for each API call"
    )

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        ...,
        description="Kafka bootstrap servers"
    )
    KAFKA_TOPIC: str = Field(
        default="clickstream_ecommerce_kuldeep_pal",
        description="Kafka topic to publish to"
    )
    KAFKA_API_KEY: str = Field(
        ...,
        description="Kafka API key"
    )
    KAFKA_API_SECRET: str = Field(
        ...,
        description="Kafka API secret"
    )

    # Producer Settings
    KAFKA_ACKS: str = Field(
        default="all",
        description="Producer acks setting"
    )
    KAFKA_RETRIES: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of retry attempts"
    )
    KAFKA_MAX_IN_FLIGHT: int = Field(
        default=1,
        ge=1,
        le=5,
        description="Maximum in-flight requests"
    )
    KAFKA_REQUEST_TIMEOUT_MS: int = Field(
        default=30000,
        ge=5000,
        le=120000,
        description="Request timeout in milliseconds"
    )

    # Monitoring
    METRICS_PORT: int = Field(
        default=8080,
        ge=1024,
        le=65535,
        description="Metrics server port"
    )
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Logging level"
    )

    # Batch Settings
    BATCH_SIZE: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Batch size for producing messages"
    )
    BATCH_TIMEOUT_MS: int = Field(
        default=1000,
        ge=100,
        le=10000,
        description="Batch timeout in milliseconds"
    )

    class Config:
        env_prefix = "PRODUCER_"
        env_file = ".env"
        case_sensitive = False


def get_config() -> ProducerConfig:
    """Get configuration instance"""
    return ProducerConfig()


# Global config instance
config = get_config()