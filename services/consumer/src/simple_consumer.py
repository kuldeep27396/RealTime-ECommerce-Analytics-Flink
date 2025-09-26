"""
Simple Kafka consumer to read from Confluent Cloud and push to ClickHouse
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import clickhouse_connect
from config import config


def create_clickhouse_table():
    """Create ClickHouse database and table if they don't exist"""
    # Connect without database first
    client = clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD
    )

    # Create database if it doesn't exist
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}")
        print(f"Database {config.CLICKHOUSE_DATABASE} created/verified")
    except Exception as e:
        print(f"Database creation error: {e}")

    # Switch to the database
    client.database = config.CLICKHOUSE_DATABASE

    # Create table
    client.command("""
        CREATE TABLE IF NOT EXISTS clickstream_events (
            user_id String,
            session_id String,
            timestamp UInt64,
            event_type String,
            product_id String,
            product_category String,
            price Float64,
            quantity Int32,
            source String,
            received_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, user_id)
    """)

    print("ClickHouse table created/verified")
    return client


def main():
    """Main consumer function"""
    print("Starting Simple Kafka Consumer with ClickHouse...")
    print(f"Topic: {config.KAFKA_TOPIC}")
    print(f"ClickHouse: {config.CLICKHOUSE_HOST}")

    # Setup ClickHouse
    ch_client = create_clickhouse_table()

    # Setup Kafka consumer
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=config.KAFKA_API_KEY,
        sasl_plain_password=config.KAFKA_API_SECRET,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='clickhouse-consumer-group',
        auto_offset_reset='latest',
        enable_auto_commit=True
    )

    # Batch insert settings
    batch_size = 1000
    batch_data = []
    total_events = 0
    last_insert_time = time.time()

    try:
        for message in consumer:
            event = message.value

            # Prepare data for ClickHouse
            timestamp_str = event.get('timestamp', '0')
            try:
                # Try to parse ISO format timestamp
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp_ms = int(dt.timestamp() * 1000)
            except:
                # Fallback to current time
                timestamp_ms = int(time.time() * 1000)

            record = [
                event.get('user_id', 'unknown'),
                event.get('session_id', 'unknown'),
                timestamp_ms,
                event.get('interaction_type', 'unknown'),  # API uses interaction_type
                event.get('product_id', 'unknown'),
                event.get('product_category', 'unknown'),
                float(event.get('price', 0.0)),
                int(event.get('quantity', 0)),
                event.get('source', 'unknown')
            ]

            batch_data.append(record)
            total_events += 1

            # Insert batch when full or every 5 seconds
            if len(batch_data) >= batch_size or (time.time() - last_insert_time) >= 5:
                if batch_data:
                    ch_client.insert('clickstream_events', batch_data,
                                    column_names=['user_id', 'session_id', 'timestamp', 'event_type',
                                                 'product_id', 'product_category', 'price', 'quantity', 'source'])
                    print(f"Inserted {len(batch_data)} events (Total: {total_events})")
                    batch_data = []
                    last_insert_time = time.time()

    except KeyboardInterrupt:
        # Insert remaining events
        if batch_data:
            ch_client.insert('clickstream_events', batch_data,
                            column_names=['user_id', 'session_id', 'timestamp', 'event_type',
                                         'product_id', 'product_category', 'price', 'quantity', 'source'])
            print(f"Inserted final {len(batch_data)} events")

        print(f"\nTotal events processed: {total_events}")

        # Show some stats from ClickHouse
        result = ch_client.query("SELECT COUNT(*) as total_events FROM clickstream_events")
        print(f"Total events in ClickHouse: {result.first_row[0]}")

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
        consumer.close()
        ch_client.close()


if __name__ == "__main__":
    main()