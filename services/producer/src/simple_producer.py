"""
Simple producer to read from API and push to Confluent Cloud
"""

import json
import time
import requests
from kafka import KafkaProducer
from config import config


def main():
    """Main producer function"""
    print("Starting Simple Producer...")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"API: {config.API_URL}")

    # Setup Kafka producer with optimized settings for high throughput
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.KAFKA_API_KEY,
        sasl_plain_password=config.KAFKA_API_SECRET,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        batch_size=16384,  # 16KB batch size
        linger_ms=5,       # Wait up to 5ms to batch messages
        compression_type='gzip',  # Compress messages
        max_in_flight_requests_per_connection=10,  # Allow more in-flight requests
        acks=1,           # Only wait for leader ack
        retries=3,        # Retry 3 times
        buffer_memory=33554432,  # 32MB buffer
        max_block_ms=10000  # Block up to 10 seconds
    )

    # Fetch data from API
    url = f"{config.API_URL}?rate=50000&duration=3600"
    sent_count = 0
    start_time = time.time()


    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        for line in response.iter_lines():
            if line:
                try:
                    event = json.loads(line.decode('utf-8'))
                    producer.send(config.KAFKA_TOPIC, value=event)
                    sent_count += 1

                    if sent_count % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = sent_count / elapsed if elapsed > 0 else 0
                        print(f"Sent {sent_count} events, rate: {rate:.0f}/sec")

                except json.JSONDecodeError:
                    continue

    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        print(f"Finished. Total events sent: {sent_count}")


if __name__ == "__main__":
    main()