"""
Simple producer to read from API and push to Confluent Cloud
"""

import json
import time
import requests
from kafka import KafkaProducer
from config import config


class SimpleProducer:
    def __init__(self):
        self.producer = None
        self.setup_producer()

    def setup_producer(self):
        """Setup Kafka producer for Confluent Cloud"""
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=config.KAFKA_API_KEY,
            sasl_plain_password=config.KAFKA_API_SECRET,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )

    def fetch_from_api(self, rate=10000, duration=60):
        """Fetch data from clickstream API"""
        url = f"{config.API_URL}?rate={rate}&duration={duration}"

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            for line in response.iter_lines():
                if line:
                    try:
                        event = json.loads(line.decode('utf-8'))
                        yield event
                    except json.JSONDecodeError:
                        continue

        except requests.RequestException as e:
            print(f"Error fetching from API: {e}")

    def send_to_kafka(self, event):
        """Send event to Kafka"""
        try:
            self.producer.send(
                config.KAFKA_TOPIC,
                key=event.get('user_id'),
                value=event
            )
            return True
        except Exception as e:
            print(f"Error sending to Kafka: {e}")
            return False

    def run(self, rate=10000, duration=60):
        """Main run loop"""
        print(f"Starting producer - sending to topic: {config.KAFKA_TOPIC}")
        print(f"API URL: {config.API_URL}")
        print(f"Rate: {rate} events/second, Duration: {duration} seconds")

        sent_count = 0
        start_time = time.time()

        try:
            for event in self.fetch_from_api(rate, duration):
                if self.send_to_kafka(event):
                    sent_count += 1

                if sent_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate_actual = sent_count / elapsed if elapsed > 0 else 0
                    print(f"Sent {sent_count} events, rate: {rate_actual:.0f} events/sec")

                # Check if duration exceeded
                if time.time() - start_time > duration:
                    break

        except KeyboardInterrupt:
            print("Producer stopped by user")
        finally:
            self.producer.flush()
            print(f"Producer finished. Total events sent: {sent_count}")


if __name__ == "__main__":
    producer = SimpleProducer()
    producer.run(rate=10000, duration=60)