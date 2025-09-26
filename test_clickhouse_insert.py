#!/usr/bin/env python3
"""
Test ClickHouse connection and insert sample data
"""

import clickhouse_connect
import json
import time

def main():
    """Test ClickHouse connection and insert sample data"""
    print("Connecting to ClickHouse...")

    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host='xzl912rng6.asia-southeast1.gcp.clickhouse.cloud',
        user='default',
        password='opXD0E4bTu_W3',
        secure=True
    )

    print("Connected successfully!")

    # Create database and table
    try:
        client.command("CREATE DATABASE IF NOT EXISTS ecommerce")
        print("Database created/verified")
    except Exception as e:
        print(f"Database error: {e}")

    client.database = 'ecommerce'

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
    print("Table created/verified")

    # Insert sample data
    sample_data = [
        [
            'user_123',
            'session_456',
            int(time.time() * 1000),
            'page_view',
            'prod_789',
            'electronics',
            299.99,
            1,
            'web'
        ],
        [
            'user_456',
            'session_789',
            int(time.time() * 1000),
            'add_to_cart',
            'prod_012',
            'books',
            19.99,
            2,
            'mobile'
        ]
    ]

    client.insert('clickstream_events', sample_data,
                column_names=['user_id', 'session_id', 'timestamp', 'event_type',
                              'product_id', 'product_category', 'price', 'quantity', 'source'])
    print(f"Inserted {len(sample_data)} sample records")

    # Query the data
    result = client.query("SELECT COUNT(*) FROM clickstream_events")
    total = result.first_row[0]
    print(f"Total events in ClickHouse: {total}")

    # Show sample data
    result = client.query("""
        SELECT event_type, user_id, price, received_time
        FROM clickstream_events
        ORDER BY received_time DESC
        LIMIT 5
    """)

    print("\nSample data:")
    for row in result.result_rows:
        print(f"  {row[0]} by {row[1]} - ${row[2]} at {row[3]}")

    client.close()
    print("\nTest completed successfully!")

if __name__ == "__main__":
    main()