"""
Simple configuration for consumer - Manages environment variables for Kafka and ClickHouse
"""

import os
from typing import Optional

from pydantic_settings import BaseSettings
from pydantic import Field


class Config(BaseSettings):
    """
    Configuration class using Pydantic for type-safe environment variable handling.
    This class defines all required configuration parameters for the Kafka consumer
    and ClickHouse database connection.

    ARCHITECTURAL PATTERN: Configuration Management Pattern
    Centralized configuration management using Pydantic for validation and type safety.
    """

    # Kafka Configuration - Connection settings for consuming messages
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")

    # The Kafka topic to consume messages from
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")

    # Consumer group ID for offset management and load balancing
    # WHY CONSUMER GROUPS?
    # - Load balancing: Multiple consumers can share the workload
    # - Fault tolerance: If one consumer fails, others can take over
    # - Offset management: Kafka tracks which messages each group has consumed
    # - Scalability: Easy to add more consumers as load increases
    # ALTERNATIVES: Simple consumer (no group management), independent consumers
    KAFKA_GROUP_ID: str = Field(..., env="KAFKA_GROUP_ID")

    # API key for authenticating with Confluent Cloud
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    # API secret for authenticating with Confluent Cloud
    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    # ClickHouse Configuration - Database connection settings
    CLICKHOUSE_HOST: str = Field(..., env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(..., env="CLICKHOUSE_PORT")
    CLICKHOUSE_DATABASE: str = Field(..., env="CLICKHOUSE_DATABASE")
    CLICKHOUSE_USER: str = Field(..., env="CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = Field(..., env="CLICKHOUSE_PASSWORD")

    # Whether to use secure connection (SSL/TLS) for ClickHouse
    # WHY CLICKHOUSE SECURE DEFAULT TRUE?
    # - Security: Encrypts data in transit
    # - Compliance: Required by many security policies
    # - Best practice: Secure by default
    # ALTERNATIVES: False (insecure, faster in trusted networks)
    CLICKHOUSE_SECURE: bool = Field(default=True, env="CLICKHOUSE_SECURE")

    # WHY CLICKHOUSE FOR ANALYTICS?
    # - Columnar storage: Optimized for analytical queries
    # - High performance: Can process billions of rows quickly
    # - Real-time: Supports real-time data ingestion
    # - Cost-effective: Open source and efficient
    # - SQL interface: Familiar query language
    #
    # ALTERNATIVES CONSIDERED:
    # - PostgreSQL: Great for transactions but slower for analytics
    # - TimescaleDB: Good for time-series but less general-purpose
    # - BigQuery: Serverless but vendor lock-in and cost at scale
    # - Snowflake: Excellent but expensive and vendor lock-in
    # - Druid: Specialized for time-series but more complex
    # - Elasticsearch: Great for search but expensive for pure analytics
    # - InfluxDB: Excellent for time-series but less general-purpose

    class Config:
        # Load environment variables from .env file
        env_file = ".env"

        # WHY CASE SENSITIVE?
        # - Consistency: Environment variables are typically case-sensitive
        # - Prevention: Avoids subtle bugs from case mismatches
        # - Standards: Follows common conventions
        case_sensitive = True

        # WHY IGNORE EXTRA VARIABLES?
        # - Flexibility: Allows different environments to have additional configuration
        # - Security: Won't fail if secrets are present but not used
        # - Compatibility: Works across different deployment environments
        # - Evolution: Easy to add new configuration without breaking existing code
        extra = "ignore"


# Create a global configuration instance that can be imported throughout the application
# WHY SINGLETON PATTERN?
# - Consistency: All modules use the same configuration
# - Performance: Configuration is loaded once and reused
# - Simplicity: Easy to import and use throughout the application
# - Testing: Can be easily mocked for unit tests
# - Caching: Avoids repeated file I/O operations
config = Config()