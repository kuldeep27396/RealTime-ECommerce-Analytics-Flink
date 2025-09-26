#!/usr/bin/env python3
"""
Debug script to check Kafka connection and messages
"""

from kafka import KafkaConsumer
from pydantic_settings import BaseSettings
from pydantic import Field

class Config(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC: str = Field(..., env="KAFKA_TOPIC")
    KAFKA_API_KEY: str = Field(..., env="KAFKA_API_KEY")
    KAFKA_API_SECRET: str = Field(..., env="KAFKA_API_SECRET")

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

config = Config()

def main():
    """Check Kafka connection and read some messages"""
    print("Checking Kafka connection...")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"Bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")

    try:
        # Create consumer
        consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=config.KAFKA_API_KEY,
            sasl_plain_password=config.KAFKA_API_SECRET,
            value_deserializer=lambda x: x.decode('utf-8'),
            group_id='debug-consumer',
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

        print("Connected to Kafka successfully!")
        print("Listening for messages (10 second timeout)...")

        # Read for 10 seconds
        import time
        start_time = time.time()
        message_count = 0

        for message in consumer:
            message_count += 1
            print(f"Message {message_count}: {message.value[:200]}")

            if time.time() - start_time > 10:
                break

        print(f"Total messages received: {message_count}")

        if message_count == 0:
            print("No messages received. Checking topic partitions...")

            # Check topic info
            from kafka import KafkaAdminClient
            admin_client = KafkaAdminClient(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                security_protocol='SASL_SSL',
                sasl_mechanism='PLAIN',
                sasl_plain_username=config.KAFKA_API_KEY,
                sasl_plain_password=config.KAFKA_API_SECRET
            )

            try:
                metadata = admin_client.list_topics()
                if config.KAFKA_TOPIC in metadata.topics:
                    print(f"Topic {config.KAFKA_TOPIC} exists")
                    partitions = metadata.topics[config.KAFKA_TOPIC]
                    print(f"Partitions: {len(partitions)}")
                else:
                    print(f"Topic {config.KAFKA_TOPIC} not found!")
            except Exception as e:
                print(f"Error checking topic: {e}")

        consumer.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()