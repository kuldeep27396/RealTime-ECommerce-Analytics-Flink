"""
Simple consumer using Flink to read from Confluent Cloud
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from config import config


def main():
    """Main consumer function"""
    print("Starting Simple Flink Consumer...")
    print(f"Topic: {config.KAFKA_TOPIC}")

    # Setup Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Create Kafka source table
    t_env.execute_sql(f"""
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

    # Simple aggregation query
    result = t_env.sql_query("""
        SELECT
            event_type,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(price) as avg_price,
            SUM(price * quantity) as total_revenue
        FROM clickstream_events
        GROUP BY event_type
    """)

    # Execute and print results
    print("Starting Flink job...")
    result.execute().print()


if __name__ == "__main__":
    main()