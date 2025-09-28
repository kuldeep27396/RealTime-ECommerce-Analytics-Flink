"""
Simple producer to read from API and push to Confluent Cloud
This script demonstrates a real-time data pipeline producer that:
1. Connects to an external API to stream ecommerce events
2. Publishes events to Kafka with optimized settings
3. Handles errors and provides performance metrics

ARCHITECTURAL PATTERN: Producer Pattern in Event Streaming
This implements the classic producer pattern where data is published to a message broker
for asynchronous processing by consumers.
"""

import json
import time
import requests
from kafka import KafkaProducer
from config import config


def main():
    """
    Main producer function that orchestrates the data flow from API to Kafka.

    ARCHITECTURAL DECISIONS:
    - SYNCHRONOUS PROCESSING: Simple and reliable, but could be async for higher throughput
    - SINGLE-THREADED: Simple to understand, but could be multi-threaded for parallel processing
    - MEMORY-EFFICIENT STREAMING: Processes data line by line rather than loading all in memory
    - ERROR TOLERANCE: Skips malformed data rather than failing the entire pipeline

    ALTERNATIVE APPROACHES:
    1. ASYNC PRODUCER: Would provide higher throughput but more complex error handling
    2. MULTI-THREADED: Could process multiple API streams in parallel
    3. BATCH PROCESSING: Could collect multiple events before sending to Kafka
    4. BACKPRESSURE HANDLING: Could implement proper backpressure when Kafka is slow
    """
    print("Starting Simple Producer...")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"API: {config.API_URL}")

    # Setup Kafka producer with optimized settings for high throughput
    # These settings are tuned for high-volume event streaming
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,

        # WHY SASL_SSL?
        # - Security: Encrypts data in transit
        # - Compliance: Required by Confluent Cloud
        # - Authentication: Provides identity verification
        # ALTERNATIVES: PLAINTEXT (insecure), SSL (without SASL), SASL_PLAINTEXT (no encryption)
        security_protocol='SASL_SSL',

        # WHY PLAIN MECHANISM?
        # - Simplicity: Easy to implement
        # - Wide support: Works with most systems
        # - Confluent Cloud requirement
        # ALTERNATIVES: SCRAM-SHA-256/512 (more secure), OAuth (better for enterprises)
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.KAFKA_API_KEY,
        sasl_plain_password=config.KAFKA_API_SECRET,

        # WHY JSON SERIALIZATION?
        # - Human readable: Easy to debug
        # - Universal: Supported by most languages
        # - Flexible: Can handle complex nested data
        # ALTERNATIVES: Avro (schema evolution, more compact), Protobuf (very compact, fast),
        #              MessagePack (binary JSON), CBOR (compact binary representation)
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),

        # PERFORMANCE TUNING SETTINGS:

        # WHY 16KB BATCH SIZE?
        # - Throughput: Larger batches reduce network overhead
        # - Memory: Reasonable memory footprint
        # - Latency: Not too large to cause unacceptable delays
        # ALTERNATIVES: 32KB (higher throughput, more memory), 8KB (lower latency, less throughput)
        batch_size=16384,

        # WHY 5ms LINGER?
        # - Batching: Allows multiple messages to be batched together
        # - Latency: Small enough to not cause noticeable delays
        # - Throughput: Significantly improves throughput vs. 0ms
        # ALTERNATIVES: 0ms (immediate send, lower throughput), 10ms (higher throughput, more latency)
        linger_ms=5,

        # WHY GZIP COMPRESSION?
        # - Bandwidth: Reduces network usage by 70-90%
        # - Cost: Lower data transfer costs
        # - CPU: Modern CPUs handle compression efficiently
        # ALTERNATIVES: None (no compression), Snappy (faster but less compression), LZ4 (fast, decent compression)
        compression_type='gzip',

        # WHY 10 IN-FLIGHT REQUESTS?
        # - Parallelism: Allows multiple concurrent requests
        # - Throughput: Improves overall throughput
        # - Reliability: Kafka handles ordering within partitions
        # ALTERNATIVES: 1 (strict ordering, lower throughput), 5 (balanced), 20 (higher throughput, more memory)
        max_in_flight_requests_per_connection=10,

        # WHY ACKS=1?
        # - Performance: Leader ack is much faster than waiting for all replicas
        # - Reliability: Still provides good durability (leader ack)
        # - Balance: Good balance between speed and reliability
        # ALTERNATIVES: acks=0 (fastest, no durability), acks=all (most durable, slowest)
        acks=1,

        # WHY 3 RETRIES?
        # - Resilience: Handles transient network issues
        # - Performance: Not so many that it causes excessive delays
        # - Balance: Good balance between reliability and performance
        # ALTERNATIVES: 0 (fail fast), 5 (more resilient), 10 (very resilient but slower)
        retries=3,

        # WHY 32MB BUFFER?
        # - Memory: Reasonable memory usage for modern systems
        # - Backpressure: Provides buffer when Kafka is slow
        # - Performance: Prevents blocking under normal conditions
        # ALTERNATIVES: 16MB (less memory, more sensitive to Kafka speed), 64MB (more memory, more resilient)
        buffer_memory=33554432,

        # WHY 10 SECOND BLOCK?
        # - Resilience: Allows time for Kafka to recover from temporary issues
        # - User experience: Not so long that the application appears frozen
        # - Monitoring: Gives time to detect and respond to issues
        # ALTERNATIVES: 1s (fail fast), 30s (more resilient, worse UX), 60s (very resilient, poor UX)
        max_block_ms=10000
    )

    # Fetch data from API - this simulates real-time ecommerce event generation
    # rate=50000 means 50,000 events per second
    # duration=3600 means the API will generate events for 1 hour
    url = f"{config.API_URL}?rate=50000&duration=3600"
    sent_count = 0
    start_time = time.time()

    try:
        # WHY STREAMING REQUEST?
        # - Memory efficiency: Processes data as it arrives, doesn't load everything into memory
        # - Real-time: Can start processing immediately
        # - Scalability: Can handle unlimited data size
        # ALTERNATIVES: Download entire response (simple but memory intensive), chunked processing
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Process each line as a separate JSON event
        # This is memory-efficient for high-volume data streams
        for line in response.iter_lines():
            if line:  # Skip empty lines
                try:
                    # Parse JSON event from the API response
                    event = json.loads(line.decode('utf-8'))
                    # Send event to Kafka topic
                    producer.send(config.KAFKA_TOPIC, value=event)
                    sent_count += 1

                    # Print progress every 1000 events
                    # WHY PROGRESS REPORTING?
                    # - Monitoring: Shows the system is working
                    # - Performance: Helps identify throughput issues
                    # - Debugging: Helps identify when things go wrong
                    if sent_count % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = sent_count / elapsed if elapsed > 0 else 0
                        print(f"Sent {sent_count} events, rate: {rate:.0f}/sec")

                except json.JSONDecodeError:
                    # Skip malformed JSON lines - important for robustness
                    # WHY SKIP INSTEAD OF FAIL?
                    # - Resilience: Bad data shouldn't stop the entire pipeline
                    # - Production reality: Real-world data often has issues
                    # - Monitoring: Could log these for later analysis
                    continue

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure all pending messages are sent before exiting
        # WHY FLUSH?
        # - Reliability: Ensures all buffered messages are sent
        # - Graceful shutdown: Important for clean application termination
        # - Data integrity: Prevents data loss on shutdown
        producer.flush()
        print(f"Finished. Total events sent: {sent_count}")


if __name__ == "__main__":
    main()