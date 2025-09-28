"""
Simple Kafka consumer to read from Confluent Cloud and push to ClickHouse
This script demonstrates a complete consumer pattern in event streaming architectures:
1. Connects to Kafka to consume ecommerce events
2. Processes and transforms event data
3. Batch inserts into ClickHouse for analytics
4. Provides monitoring and statistics

ARCHITECTURAL PATTERN: Consumer Pattern with Batch Processing
This implements a consumer that reads from Kafka and writes to ClickHouse,
demonstrating best practices for real-time data pipelines.
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import clickhouse_connect
from config import config


def create_clickhouse_table():
    """
    Create ClickHouse database and table if they don't exist.
    This function demonstrates database schema setup for time-series analytics.

    ARCHITECTURAL DECISIONS:
    - IDEMPOTENT OPERATIONS: Safe to run multiple times
    - SEPARATE DATABASE CREATION: Allows for multi-tenant setups
    - MERGETREE ENGINE: Optimized for time-series analytics
    - COMPOSITE PRIMARY KEY: Efficient for time-based queries

    Returns:
        clickhouse_connect.client: Connected ClickHouse client instance
    """
    # Connect to ClickHouse server without specifying a database first
    # This allows us to create the database if it doesn't exist
    client = clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD
    )

    # Create database if it doesn't exist
    # This is idempotent - safe to run multiple times
    # WHY SEPARATE DATABASE?
    # - Multi-tenancy: Can support multiple applications
    # - Security: Better isolation and access control
    # - Management: Easier to backup and manage separately
    # ALTERNATIVES: Use default database (less organized), schema-based separation
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}")
        print(f"Database {config.CLICKHOUSE_DATABASE} created/verified")
    except Exception as e:
        print(f"Database creation error: {e}")

    # Switch to the target database for subsequent operations
    client.database = config.CLICKHOUSE_DATABASE

    # Create table optimized for time-series analytics
    # MergeTree engine is ClickHouse's default for high-performance analytics

    # WHY MERGETREE ENGINE?
    # - Performance: Optimized for analytical queries and time-series data
    # - Compression: Excellent compression ratios
    # - Scalability: Handles billions of rows efficiently
    # - Merging: Automatically merges data parts for optimization
    # ALTERNATIVES: ReplacingMergeTree (deduplication), SummingMergeTree (aggregation),
    #              AggregatingMergeTree (pre-aggregation), ReplicatedMergeTree (HA clusters)

    # WHY COMPOSITE PRIMARY KEY (timestamp, user_id)?
    # - Time-based queries: Most analytics queries filter by time ranges
    # - User journeys: Supports user behavior analysis
    # - Partitioning: Enables efficient time-based partitioning
    # - Performance: Optimal for common query patterns
    # ALTERNATIVES: timestamp only (simpler), user_id only (user-centric),
    #              event_type (category-based), random distribution (even writes)

    client.command("""
        CREATE TABLE IF NOT EXISTS clickstream_events (
            user_id String,                    -- User identifier
            session_id String,                 -- Session identifier for user journeys
            timestamp UInt64,                  -- Event timestamp in milliseconds
            event_type String,                 -- Type of interaction (view, cart, purchase)
            product_id String,                 -- Product identifier
            product_category String,           -- Product category for analytics
            price Float64,                     -- Product price
            quantity Int32,                    -- Quantity involved in the event
            source String,                     -- Source of the event
            received_time DateTime DEFAULT now() -- When we received the event
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, user_id)         -- Primary key for efficient queries
    """)

    print("ClickHouse table created/verified")
    return client


def main():
    """
    Main consumer function that orchestrates the data flow from Kafka to ClickHouse.

    ARCHITECTURAL DECISIONS:
    - BATCH PROCESSING: Improves database write performance
    - TIME-BASED FLUSHING: Ensures data is written even if batch size isn't reached
    - ERROR HANDLING: Graceful handling of malformed data and connection issues
    - MONITORING: Provides visibility into system performance
    - GRACEFUL SHUTDOWN: Ensures no data loss on termination

    ALTERNATIVE APPROACHES:
    1. STREAMING INSERTS: Immediate writes (simpler but slower)
    2. ASYNC PROCESSING: Higher throughput but more complex
    3. MULTI-THREADED: Parallel processing for higher throughput
    4. EXACTLY-ONCE SEMANTICS: Using transactions for perfect reliability
    """
    print("Starting Simple Kafka Consumer with ClickHouse...")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"ClickHouse: {config.CLICKHOUSE_HOST}")

    # Setup ClickHouse database and table
    ch_client = create_clickhouse_table()

    # Setup Kafka consumer with specific configuration
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,                   # Topic to consume from
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',         # Secure connection
        sasl_mechanism='PLAIN',               # Authentication mechanism
        sasl_plain_username=config.KAFKA_API_KEY,
        sasl_plain_password=config.KAFKA_API_SECRET,
        # Automatically deserialize JSON messages to Python objects
        # WHY JSON DESERIALIZATION?
        # - Simplicity: Easy to work with in Python
        # - Debugging: Human-readable format
        # - Flexibility: Handles complex nested structures
        # ALTERNATIVES: Avro (schema evolution), Protobuf (performance), MessagePack (compact)
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='clickhouse-consumer-group', # Consumer group for offset management

        # WHY AUTO_OFFSET_RESET='LATEST'?
        # - Real-time processing: Only process new messages
        # - Fresh starts: Good for development and testing
        # - Performance: Avoids reprocessing historical data
        # ALTERNATIVES: 'earliest' (process all available), 'none' (fail if no offset)
        auto_offset_reset='latest',

        # WHY ENABLE_AUTO_COMMIT?
        # - Simplicity: Kafka handles offset management
        # - Reliability: Less chance of losing processed messages
        # - Performance: Reduces coordination overhead
        # ALTERNATIVES: Manual commit (more control, but complex error handling)
        enable_auto_commit=True
    )

    # Batch insert settings for performance optimization
    # WHY BATCH SIZE 1000?
    # - Performance: Optimal balance between throughput and latency
    # - Memory: Reasonable memory usage per batch
    # - Database: Well within ClickHouse's batch processing capabilities
    # - Network: Efficient use of network bandwidth
    # ALTERNATIVES: 100 (lower latency, less throughput), 5000 (higher throughput, more latency)
    batch_size = 1000                        # Insert in batches of 1000 events
    batch_data = []                          # Accumulate events here
    total_events = 0                         # Track total processed events
    last_insert_time = time.time()           # Track time for periodic inserts

    try:
        # Main consumption loop - runs until interrupted
        for message in consumer:
            event = message.value

            # Parse and normalize timestamp for consistent storage
            timestamp_str = event.get('timestamp', '0')
            try:
                # Try to parse ISO format timestamp (e.g., "2023-01-01T12:00:00Z")
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp_ms = int(dt.timestamp() * 1000)  # Convert to milliseconds
            except:
                # Fallback to current time if parsing fails
                # WHY FALLBACK TO CURRENT TIME?
                # - Resilience: Ensures data is still stored even with timestamp issues
                # - Analytics: Better than missing data or invalid timestamps
                # - Debugging: Can identify and fix timestamp issues later
                # ALTERNATIVES: Skip event (loses data), store as null (breaks queries)
                timestamp_ms = int(time.time() * 1000)

            # Prepare record for ClickHouse insertion
            # Note: API uses 'interaction_type' but we store as 'event_type'
            # WHY FIELD NAME MAPPING?
            # - Standardization: Consistent naming convention
            # - Compatibility: Handles different API versions
            # - Clarity: More descriptive field names
            record = [
                event.get('user_id', 'unknown'),           # User identifier
                event.get('session_id', 'unknown'),        # Session identifier
                timestamp_ms,                              # Normalized timestamp
                event.get('interaction_type', 'unknown'),  # Event type (normalized)
                event.get('product_id', 'unknown'),       # Product identifier
                event.get('product_category', 'unknown'),  # Product category
                float(event.get('price', 0.0)),           # Price as float
                int(event.get('quantity', 0)),           # Quantity as integer
                event.get('source', 'unknown')            # Event source
            ]

            batch_data.append(record)
            total_events += 1

            # Insert batch when full or every 5 seconds (time-based batching)
            # This ensures data is written even if batch size isn't reached
            # WHY TIME-BASED FLUSHING?
            # - Freshness: Ensures data is available for queries promptly
            # - Reliability: Prevents data loss if application crashes
            # - Performance: Balances batch efficiency with data availability
            # - Monitoring: Provides regular progress updates
            # ALTERNATIVES: Size-only batching (simpler, less timely),
            #               time-only batching (less efficient), adaptive batching
            if len(batch_data) >= batch_size or (time.time() - last_insert_time) >= 5:
                if batch_data:
                    # Batch insert into ClickHouse for better performance
                    # WHY BATCH INSERTS?
                    # - Performance: Reduces database overhead
                    # - Throughput: Higher overall write throughput
                    # - Resource efficiency: Better use of database resources
                    # - Network efficiency: Fewer network round trips
                    # ALTERNATIVES: Individual inserts (simpler, slower),
                    #               prepared statements (more complex, slightly faster)
                    ch_client.insert('clickstream_events', batch_data,
                                    column_names=['user_id', 'session_id', 'timestamp', 'event_type',
                                                 'product_id', 'product_category', 'price', 'quantity', 'source'])
                    print(f"Inserted {len(batch_data)} events (Total: {total_events})")
                    batch_data = []
                    last_insert_time = time.time()

    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C - ensure we don't lose any buffered events
        # WHY GRACEFUL SHUTDOWN?
        # - Data integrity: Ensures all buffered data is saved
        # - User experience: Clean shutdown without data loss
        # - Monitoring: Final statistics before exit
        # - Best practices: Professional application behavior
        if batch_data:
            ch_client.insert('clickstream_events', batch_data,
                            column_names=['user_id', 'session_id', 'timestamp', 'event_type',
                                         'product_id', 'product_category', 'price', 'quantity', 'source'])
            print(f"Inserted final {len(batch_data)} events")

        print(f"\nTotal events processed: {total_events}")

        # Show analytics from ClickHouse - demonstrates the value of stored data
        # WHY SHOW ANALYTICS ON SHUTDOWN?
        # - Validation: Confirms data was properly stored
        # - Value demonstration: Shows the purpose of the pipeline
        # - Debugging: Helps identify data quality issues
        # - User feedback: Provides immediate value to users
        result = ch_client.query("SELECT COUNT(*) as total_events FROM clickstream_events")
        print(f"Total events in ClickHouse: {result.first_row[0]}")

        # Show event type distribution - useful for business analytics
        # WHY EVENT TYPE DISTRIBUTION?
        # - Business insights: Shows user behavior patterns
        # - Data quality: Validates that different event types are captured
        # - Use case demonstration: Shows how to use the stored data
        # - Performance: Simple aggregation that runs quickly
        result = ch_client.query("""
            SELECT event_type, COUNT(*) as count
            FROM clickstream_events
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT 10
        """)
        print("Event type distribution:")
        for row in result.result_rows:
            print(f"  {row[0]}: {row[1]}")

    finally:
        # Clean up resources - important for proper connection management
        # WHY PROPER RESOURCE CLEANUP?
        # - Connection management: Prevents connection leaks
        # - Database efficiency: Allows ClickHouse to free resources
        # - Network efficiency: Properly closes network connections
        # - Best practices: Professional application behavior
        consumer.close()
        ch_client.close()


if __name__ == "__main__":
    main()