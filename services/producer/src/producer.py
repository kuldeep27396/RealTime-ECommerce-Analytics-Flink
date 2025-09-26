"""
Main producer service with robust error handling and monitoring
"""

import json
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from models import HealthStatus, ProducerMetrics, TransformedClickstreamEvent
from transformer import ClickstreamTransformer

from config import config

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ClickstreamProducer:
    """
    Robust producer service for streaming clickstream data to Kafka

    Learning: This class demonstrates production-ready patterns:
    - Connection management with retry logic
    - Graceful shutdown handling
    - Metrics collection and health checks
    - Batch processing for performance
    """

    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.transformer = ClickstreamTransformer()
        self.metrics = ProducerMetrics()
        self.start_time = datetime.now()
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=4)

        # Learning: Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._setup_producer()

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, stopping producer...")
        self.stop()

    def _setup_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                    security_protocol="SASL_SSL",
                    sasl_mechanism="PLAIN",
                    sasl_plain_username=config.KAFKA_API_KEY,
                    sasl_plain_password=config.KAFKA_API_SECRET,
                    value_serializer=lambda v: json.dumps(v.__dict__).encode("utf-8"),
                    acks=config.KAFKA_ACKS,
                    retries=config.KAFKA_RETRIES,
                    max_in_flight_requests_per_connection=config.KAFKA_MAX_IN_FLIGHT,
                    request_timeout_ms=config.KAFKA_REQUEST_TIMEOUT_MS,
                    # Learning: Batch configuration for better throughput
                    batch_size=config.BATCH_SIZE,
                    linger_ms=config.BATCH_TIMEOUT_MS,
                    # Learning: Compression for network efficiency
                    compression_type="gzip",
                    # Learning: Enable idempotent producer for exactly-once semantics
                    enable_idempotence=True,
                )
                logger.info("Kafka producer initialized successfully")
                return

            except Exception as e:
                logger.error(
                    f"Failed to initialize Kafka producer (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                else:
                    raise

    def fetch_and_process_stream(self):
        """
        Main processing loop that fetches from API and produces to Kafka

        Learning: This demonstrates a robust streaming pipeline with:
        - Continuous operation with API polling
        - Error handling and retry logic
        - Backpressure handling
        - Resource management
        """
        self.running = True
        consecutive_errors = 0
        max_consecutive_errors = 5

        logger.info("Starting stream processing...")

        while self.running and consecutive_errors < max_consecutive_errors:
            try:
                # Learning: API call with timeout and error handling
                response = self._make_api_request()
                if response is None:
                    consecutive_errors += 1
                    time.sleep(min(consecutive_errors * 2, 30))  # Exponential backoff
                    continue

                # Learning: Process stream in chunks for memory efficiency
                events_processed = self._process_stream_chunk(response)
                if events_processed > 0:
                    consecutive_errors = 0  # Reset on success
                else:
                    consecutive_errors += 1

            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"Error in processing loop (error {consecutive_errors}/{max_consecutive_errors}): {e}"
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        "Maximum consecutive errors reached, stopping producer"
                    )
                    break

                time.sleep(min(consecutive_errors * 2, 30))

        logger.info("Stream processing stopped")

    def _make_api_request(self) -> Optional[requests.Response]:
        """Make API request with proper error handling"""
        try:
            params = {"rate": config.API_RATE, "duration": config.API_DURATION}

            logger.info(f"Making API request to {config.API_URL} with params: {params}")

            response = requests.get(
                config.API_URL,
                params=params,
                stream=True,
                timeout=120,  # 2 minute timeout
            )
            response.raise_for_status()

            self.metrics.api_calls_made += 1
            return response

        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None

    def _process_stream_chunk(self, response: requests.Response) -> int:
        """Process a chunk of streaming data"""
        events_processed = 0
        batch_events = []
        start_time = time.time()

        try:
            for line in response.iter_lines():
                if not self.running:
                    break

                if line:
                    try:
                        raw_event = json.loads(line.decode("utf-8"))
                        batch_events.append(raw_event)

                        # Learning: Process in batches for better performance
                        if len(batch_events) >= config.BATCH_SIZE:
                            events_in_batch = self._process_batch(batch_events)
                            events_processed += events_in_batch
                            batch_events = []

                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON: {e}")
                        self.metrics.events_failed += 1
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                        self.metrics.events_failed += 1

            # Process remaining events in batch
            if batch_events:
                events_in_batch = self._process_batch(batch_events)
                events_processed += events_in_batch

            # Flush any remaining messages
            if self.producer:
                self.producer.flush()

            processing_time = time.time() - start_time
            logger.info(
                f"Processed {events_processed} events in {processing_time:.2f}s "
                f"({events_processed/processing_time:.2f} events/sec)"
            )

            return events_processed

        except Exception as e:
            logger.error(f"Error processing stream chunk: {e}")
            return 0

    def _process_batch(self, raw_events: List[Dict[str, Any]]) -> int:
        """Process a batch of events"""
        try:
            # Learning: Transform events in parallel for better performance
            transformed_events = self.transformer.batch_transform(raw_events)

            # Produce to Kafka
            futures = []
            for event in transformed_events:
                future = self.producer.send(config.KAFKA_TOPIC, value=event)
                future.add_callback(self._on_send_success, event)
                future.add_errback(self._on_send_error, event)
                futures.append(future)

            # Wait for all futures to complete
            for future in as_completed(futures, timeout=30):
                try:
                    future.result()  # Will raise exception if send failed
                except Exception as e:
                    logger.error(f"Batch send failed: {e}")
                    return 0

            events_sent = len(transformed_events)
            self.metrics.events_processed += events_sent
            self.metrics.kafka_produces += 1
            self.metrics.last_event_time = datetime.now()
            self.metrics.bytes_produced += sum(
                len(json.dumps(event.__dict__)) for event in transformed_events
            )

            return events_sent

        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            self.metrics.events_failed += len(raw_events)
            return 0

    def _on_send_success(self, metadata, event: TransformedClickstreamEvent):
        """Callback for successful message delivery"""
        # Learning: Could add more detailed logging here for debugging
        pass

    def _on_send_error(self, exception, event: TransformedClickstreamEvent):
        """Callback for failed message delivery"""
        logger.error(f"Failed to send event: {exception}")
        self.metrics.events_failed += 1

    def get_health_status(self) -> HealthStatus:
        """Get current health status"""
        uptime = (datetime.now() - self.start_time).total_seconds()
        return HealthStatus(
            status="healthy" if self.running else "stopped",
            timestamp=datetime.now(),
            uptime_seconds=uptime,
            metrics=self.metrics,
        )

    def start(self):
        """Start the producer service"""
        if not self.producer:
            raise RuntimeError("Producer not initialized")

        logger.info("Starting Clickstream Producer Service")
        logger.info(
            f"Configuration: API URL={config.API_URL}, Topic={config.KAFKA_TOPIC}"
        )

        try:
            self.fetch_and_process_stream()
        except Exception as e:
            logger.error(f"Producer service failed: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """Stop the producer service gracefully"""
        logger.info("Stopping producer service...")
        self.running = False

        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
            except Exception as e:
                logger.error(f"Error closing producer: {e}")

        if self.executor:
            self.executor.shutdown(wait=True)

        logger.info("Producer service stopped")


def main():
    """Main entry point for the producer service"""
    try:
        producer = ClickstreamProducer()
        producer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Producer service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
