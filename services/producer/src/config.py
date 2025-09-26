"""
Simple configuration for producer
"""

from pydantic import BaseSettings, Field


class Config(BaseSettings):
    """Simple configuration with environment variables"""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    # API Configuration
    API_URL: str = Field(..., env="API_URL")

    class Config:
        env_file = ".env"
        case_sensitive = True


config = Config()