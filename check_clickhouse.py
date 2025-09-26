#!/usr/bin/env python3
"""
Simple script to check ClickHouse data
"""

import clickhouse_connect
import os
from pydantic_settings import BaseSettings
from pydantic import Field

class Config(BaseSettings):
    CLICKHOUSE_HOST: str = Field(..., env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(..., env="CLICKHOUSE_PORT")
    CLICKHOUSE_DATABASE: str = Field(..., env="CLICKHOUSE_DATABASE")
    CLICKHOUSE_USER: str = Field(..., env="CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = Field(..., env="CLICKHOUSE_PASSWORD")
    CLICKHOUSE_SECURE: bool = Field(default=True, env="CLICKHOUSE_SECURE")

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

config = Config()

def main():
    """Check ClickHouse data"""
    print("Connecting to ClickHouse...")

    client = clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DATABASE
    )

    # Check total count
    result = client.query("SELECT COUNT(*) FROM clickstream_events")
    total = result.first_row[0]
    print(f"Total events in ClickHouse: {total}")

    if total > 0:
        # Check event types
        result = client.query("""
            SELECT event_type, COUNT(*) as count
            FROM clickstream_events
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT 10
        """)
        print("\nEvent type distribution:")
        for row in result.result_rows:
            print(f"  {row[0]}: {row[1]}")

        # Check recent events
        result = client.query("""
            SELECT event_type, user_id, received_time
            FROM clickstream_events
            ORDER BY received_time DESC
            LIMIT 5
        """)
        print("\nRecent events:")
        for row in result.result_rows:
            print(f"  {row[0]} by {row[1]} at {row[2]}")

        # Check revenue
        result = client.query("""
            SELECT SUM(price * quantity) as total_revenue
            FROM clickstream_events
        """)
        revenue = result.first_row[0]
        print(f"\nTotal revenue: ${revenue:.2f}")

    client.close()

if __name__ == "__main__":
    main()