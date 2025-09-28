"""
Simple configuration for producer - Manages environment variables and settings
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Config(BaseSettings):
    """
    Configuration class using Pydantic for type-safe environment variable handling.

    WHY PYDANTIC?
    - Type safety: Automatic validation and type conversion
    - Environment variable management: Clean separation of config from code
    - Documentation: Self-documenting configuration
    - IDE support: Autocompletion and type hints

    ALTERNATIVES CONSIDERED:
    - Plain os.getenv(): No type safety, no validation
    - configparser.ini: More complex, requires separate files
    - JSON/YAML config files: Additional file management overhead
    - Hardcoded values: Bad practice, no environment separation
    """

    # Kafka Configuration - Connection settings for Confluent Cloud
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")

    # WHY KAFKA FOR MESSAGE BROKER?
    # - High throughput: Handles millions of messages per second
    # - Durability: Messages persist even if consumers are offline
    # - Scalability: Horizontal scaling with partitions
    # - Ecosystem: Rich set of tools and integrations
    #
    # ALTERNATIVES CONSIDERED:
    # - RabbitMQ: Better for complex routing but lower throughput
    # - AWS SQS: Serverless but higher latency and cost at scale
    # - Redis Streams: Fast but less durable and feature-rich
    # - Google Pub/Sub: Good alternative but vendor lock-in
    # - NATS: Ultra-low latency but fewer enterprise features

    # The Kafka topic where messages will be published
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")
    # API key for authenticating with Confluent Cloud
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    # API secret for authenticating with Confluent Cloud

    # WHY CONFLUENT CLOUD?
    # - Managed service: No infrastructure maintenance
    # - Enterprise features: Schema registry, monitoring, security
    # - Global availability: Multi-region deployments
    # - Cost-effective: Pay-as-you-go pricing
    #
    # ALTERNATIVES:
    # - Self-hosted Kafka: More control but operational overhead
    # - Amazon MSK: AWS-managed but less feature-rich
    # - Other cloud providers: Azure Event Hubs, Google Pub/Sub

    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    # API Configuration - External data source
    # URL of the API that provides ecommerce event data
    API_URL: str = Field(..., env="API_URL")

    # WHY EXTERNAL API FOR DATA SOURCE?
    # - Realistic simulation: Mimics real production scenarios
    # - Controllable rate: Can simulate different load patterns
    # - Variety of events: Generates diverse ecommerce interactions
    # - No local data storage: Lightweight and portable
    #
    # ALTERNATIVES CONSIDERED:
    # - Local CSV/JSON files: Simpler but less realistic
    # - Database query: More realistic but requires DB setup
    # - Mock data generator: More control but more complex code
    # - Real production API: Most realistic but may have rate limits

    class Config:
        # Load environment variables from .env file
        env_file = ".env"

        # WHY CASE SENSITIVE?
        # - Consistency with environment variable conventions
        # - Prevents subtle bugs from case mismatches
        # - Best practice for configuration management
        case_sensitive = True

        # WHY IGNORE EXTRA VARIABLES?
        # - Flexibility: Allows different environments to have additional vars
        # - Security: Won't fail if secrets are present but not used
        # - Compatibility: Works across different deployment environments
        extra = "ignore"


# Create a global configuration instance that can be imported throughout the application
# WHY SINGLETON PATTERN?
# - Consistency: All modules use the same configuration
# - Performance: Loaded once, reused everywhere
# - Simplicity: Easy to import and use
# - Testing: Can be mocked when needed
config = Config()