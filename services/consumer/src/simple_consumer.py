"""
Simple consumer using Flink to read from Confluent Cloud
"""

import json
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col
from config import config


class SimpleFlinkConsumer:
    def __init__(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(self.env)
        self.setup_kafka_source()

    def setup_kafka_source(self):
        """Setup Kafka source table"""
        self.t_env.execute_sql(f"""
            CREATE TABLE clickstream_events (
                user_id STRING,
                session_id STRING,
                timestamp BIGINT,
                event_type STRING,
                product_id STRING,
                product_category STRING,
                price DOUBLE,
                quantity INT,
                source STRING,
                user_agent STRING,
                ip_address STRING,
                interaction_duration INT,
                revenue DOUBLE,
                hour_of_day INT,
                day_of_week INT,
                is_weekend BOOLEAN,
                price_category STRING,
                event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000)),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{config.KAFKA_TOPIC}',
                'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
                'properties.group.id' = '{config.KAFKA_GROUP_ID}',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.username' = '{config.KAFKA_API_KEY}',
                'properties.sasl.password' = '{config.KAFKA_API_SECRET}',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )
        """)

    def process_events(self):
        """Process events with Flink and print results"""
        print("Starting Flink consumer...")
        print(f"Reading from topic: {config.KAFKA_TOPIC}")

        # Simple aggregation - count events by type
        result = self.t_env.sql_query("""
            SELECT
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(price) as avg_price,
                SUM(revenue) as total_revenue
            FROM clickstream_events
            GROUP BY event_type
        """)

        # Print results to console
        result.execute().print()

    def run(self):
        """Run the Flink job"""
        try:
            self.process_events()
        except Exception as e:
            print(f"Error in Flink job: {e}")


if __name__ == "__main__":
    consumer = SimpleFlinkConsumer()
    consumer.run()