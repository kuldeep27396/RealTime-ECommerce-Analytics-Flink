"""
Flink stream processor with comprehensive analytics transformations
Designed for learning real-time data processing patterns
"""
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.expressions import col, lit, row_number
from pyflink.table.window import Tumble, Slide, Session
from pyflink.table.udf import udf, udtf
from pyflink.common import Row
from pyflink.common.typeinfo import Types

from .config import config
from .models import ClickstreamEvent, TimeWindowMetrics, FunnelMetrics


class FlinkClickstreamProcessor:
    """
    Advanced Flink processor for real-time clickstream analytics

    Learning: This class demonstrates:
    - Multiple windowing strategies (Tumble, Slide, Session)
    - Complex event processing (CEP) patterns
    - Stateful operations
    - Advanced aggregations
    - UDFs for custom processing
    """

    def __init__(self):
        self.env = None
        self.t_env = None
        self.setup_environment()

    def setup_environment(self):
        """Initialize Flink execution environment with proper configuration"""
        # Learning: Configure streaming environment with checkpointing for exactly-once semantics
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(config.FLINK_PARALLELISM)

        # Enable checkpointing for fault tolerance
        if config.ENABLE_EXACTLY_ONCE:
            self.env.enable_checkpointing(config.FLINK_CHECKPOINT_INTERVAL)
            # Learning: Configure exactly-once semantics
            self.env.get_checkpoint_config().set_preferred_checkpoint_location("file:///tmp/flink-checkpoints")

        # Setup Table Environment
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        self.t_env = StreamTableEnvironment.create(self.env, environment_settings=settings)

        # Learning: Load required JARs for Kafka and ClickHouse connectivity
        self._load_jars()

    def _load_jars(self):
        """Load required JAR files for external connectors"""
        # Learning: In production, these would be properly managed
        jar_dir = os.path.join(os.path.dirname(__file__), "..", "..", "jars")
        if os.path.exists(jar_dir):
            jar_files = []
            for file in os.listdir(jar_dir):
                if file.endswith(".jar"):
                    jar_files.append(f"file://{os.path.join(jar_dir, file)}")

            if jar_files:
                self.t_env.get_config().get_configuration().set_string(
                    "pipeline.jars", ";".join(jar_files)
                )

    def create_kafka_source_table(self):
        """
        Create Kafka source table with proper schema and watermarks

        Learning: This shows how to properly configure:
        - Schema definition
        - Watermark strategy for event time processing
        - Format configuration
        - Connection security
        """
        source_ddl = f"""
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
            page_url STRING,
            user_agent STRING,
            ip_address STRING,
            device_type STRING,
            interaction_duration INT,
            revenue DOUBLE,
            hour_of_day INT,
            day_of_week INT,
            is_weekend BOOLEAN,
            price_category STRING,
            -- Learning: Event time processing with watermarks
            event_time AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000, 'yyyy-MM-dd HH:mm:ss')),
            WATERMARK FOR event_time AS event_time - INTERVAL '{config.FLINK_WATERMARK_DELAY}ms' MILLI
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{config.KAFKA_GROUP_ID}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{config.KAFKA_API_KEY}\" password=\"{config.KAFKA_API_SECRET}\";',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
        self.t_env.execute_sql(source_ddl)
        print("✓ Kafka source table created")

    def create_clickhouse_sink_table(self):
        """
        Create ClickHouse sink table for storing processed analytics

        Learning: Shows how to configure:
        - Table schema optimization for ClickHouse
        - Connection configuration
        - Sink properties
        """
        sink_ddl = f"""
        CREATE TABLE clickhouse_analytics (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            metric_name STRING,
            metric_value BIGINT,
            product_category STRING,
            event_type STRING,
            device_type STRING,
            processing_time TIMESTAMP
        ) WITH (
            'connector' = 'clickhouse',
            'url' = 'jdbc:clickhouse://{config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}/{config.CLICKHOUSE_DATABASE}',
            'table-name' = 'ecommerce_analytics',
            'username' = '{config.CLICKHOUSE_USER}',
            'password' = '{config.CLICKHOUSE_PASSWORD}',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '10s'
        )
        """
        self.t_env.execute_sql(sink_ddl)
        print("✓ ClickHouse sink table created")

    # Learning: User Defined Functions for custom processing
    @udf(result_type=Types.STRING())
    def categorize_user_behavior(event_count: int, revenue: float) -> str:
        """
        Categorize user behavior based on activity and spending
        Learning: Simple business logic implementation as UDF
        """
        if event_count < 5 and revenue < 50:
            return "casual_browser"
        elif event_count < 10 and revenue < 200:
            return "engaged_shopper"
        elif revenue >= 200:
            return "high_value_customer"
        else:
            return "active_explorer"

    @udf(result_type=Types.DOUBLE())
    def calculate_conversion_score(add_to_carts: int, purchases: int, views: int) -> float:
        """
        Calculate conversion score with smoothing
        Learning: Mathematical operations in UDFs
        """
        if views == 0:
            return 0.0
        cart_rate = add_to_carts / views
        purchase_rate = purchases / max(1, add_to_carts)
        return (cart_rate * 0.6 + purchase_rate * 0.4) * 100

    def create_temporal_views(self):
        """
        Create temporal views for different analytical perspectives

        Learning: Demonstrates multiple analytical patterns:
        - Time-based aggregations
        - Window functions
        - Complex joins
        """

        # Learning: Real-time event counts with tumbling windows
        real_time_counts = f"""
        CREATE VIEW real_time_event_counts AS
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
            event_type,
            device_type,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            SUM(revenue) as total_revenue
        FROM clickstream_events
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), event_type, device_type
        """
        self.t_env.execute_sql(real_time_counts)

        # Learning: Session-based analytics
        session_analytics = f"""
        CREATE VIEW user_session_analytics AS
        SELECT
            session_id,
            user_id,
            device_type,
            MIN(event_time) as session_start,
            MAX(event_time) as session_end,
            COUNT(*) as event_count,
            COUNT(DISTINCT product_id) as products_viewed,
            SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END) as session_revenue,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart_additions,
            AVG(price) as avg_price_viewed
        FROM clickstream_events
        GROUP BY session_id, user_id, device_type
        """
        self.t_env.execute_sql(session_analytics)

        # Learning: Product performance with sliding windows
        product_performance = f"""
        CREATE VIEW product_performance_metrics AS
        SELECT
            product_id,
            product_category,
            SLIDE_START(event_time, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE) as window_start,
            SLIDE_END(event_time, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as view_count,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as add_to_cart_count,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
            SUM(revenue) as total_revenue,
            COUNT(DISTINCT user_id) as unique_buyers
        FROM clickstream_events
        GROUP BY
            product_id,
            product_category,
            SLIDE(event_time, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)
        """
        self.t_env.execute_sql(product_performance)

    def create_funnel_analysis(self):
        """
        Create conversion funnel analysis

        Learning: Demonstrates complex event processing patterns
        """
        funnel_sql = """
        CREATE VIEW conversion_funnel AS
        SELECT
            TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
            TUMBLE_END(event_time, INTERVAL '5' MINUTE) as window_end,
            COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) as unique_viewers,
            COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) as cart_adders,
            COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as purchasers,
            COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) as funnel_start
        FROM clickstream_events
        WHERE event_type IN ('view', 'add_to_cart', 'purchase')
        GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE)
        """
        self.t_env.execute_sql(funnel_sql)

    def create_anomaly_detection(self):
        """
        Create anomaly detection queries

        Learning: Statistical processing for real-time monitoring
        """
        anomaly_sql = """
        CREATE VIEW traffic_anomalies AS
        SELECT
            window_start,
            window_end,
            event_count,
            LAG(event_count, 1) OVER (ORDER BY window_start) as prev_count,
            (event_count - LAG(event_count, 1) OVER (ORDER BY window_start)) * 100.0 /
                NULLIF(LAG(event_count, 1) OVER (ORDER BY window_start), 0) as percentage_change,
            CASE
                WHEN (event_count - LAG(event_count, 1) OVER (ORDER BY window_start)) * 100.0 /
                     NULLIF(LAG(event_count, 1) OVER (ORDER BY window_start), 0) > 100 THEN 'SPIKE'
                WHEN (event_count - LAG(event_count, 1) OVER (ORDER BY window_start)) * 100.0 /
                     NULLIF(LAG(event_count, 1) OVER (ORDER BY window_start), 0) < -50 THEN 'DROP'
                ELSE 'NORMAL'
            END as anomaly_type
        FROM (
            SELECT
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end,
                COUNT(*) as event_count
            FROM clickstream_events
            GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE)
        )
        """
        self.t_env.execute_sql(anomaly_sql)

    def sink_to_clickhouse(self):
        """
        Sink processed data to ClickHouse

        Learning: Shows how to format and sink data
        """
        # Prepare data for ClickHouse
        metrics_for_clickhouse = f"""
        INSERT INTO clickhouse_analytics
        SELECT
            window_start,
            window_end,
            CONCAT(event_type, '_', device_type) as metric_name,
            event_count as metric_value,
            product_category,
            event_type,
            device_type,
            NOW() as processing_time
        FROM real_time_event_counts
        """

        self.t_env.execute_sql(metrics_for_clickhouse)
        print("✓ Started sinking to ClickHouse")

    def create_dashboard_views(self):
        """
        Create views for dashboard and monitoring

        Learning: Aggregate queries for real-time dashboards
        """
        dashboard_sql = """
        CREATE VIEW dashboard_metrics AS
        SELECT
            NOW() as calculation_time,
            COUNT(*) as total_events_last_5min,
            COUNT(DISTINCT user_id) as active_users_last_5min,
            SUM(revenue) as total_revenue_last_5min,
            AVG(price) as avg_order_value,
            COUNT(DISTINCT product_id) as products_viewed_last_5min,
            COUNT(DISTINCT session_id) as active_sessions_last_5min
        FROM clickstream_events
        WHERE event_time >= NOW() - INTERVAL '5' MINUTE
        """
        self.t_env.execute_sql(dashboard_sql)

    def print_sample_output(self):
        """
        Print sample output for learning and debugging

        Learning: Shows how to inspect data in Flink
        """
        print("\\n" + "="*60)
        print("📊 REAL-TIME ANALYTICS OUTPUT (Sample)")
        print("="*60)

        # Sample event counts
        print("\\n🔄 Real-time Event Counts:")
        try:
            sample_counts = self.t_env.sql_query("""
                SELECT * FROM real_time_event_counts
                ORDER BY window_start DESC
                LIMIT 5
            """).execute().collect()

            for row in sample_counts:
                print(f"  {row.window_start} | {row.event_type} | {row.device_type} | "
                      f"Events: {row.event_count} | Users: {row.unique_users} | Revenue: ${row.total_revenue:.2f}")
        except Exception as e:
            print(f"  Error getting event counts: {e}")

        # Sample session analytics
        print("\\n👤 Sample Session Analytics:")
        try:
            sample_sessions = self.t_env.sql_query("""
                SELECT * FROM user_session_analytics
                ORDER BY session_start DESC
                LIMIT 3
            """).execute().collect()

            for row in sample_sessions:
                duration = (row.session_end - row.session_start).total_seconds() if row.session_end and row.session_start else 0
                print(f"  Session: {row.session_id[:8]}... | Duration: {duration:.1f}s | "
                      f"Events: {row.event_count} | Revenue: ${row.session_revenue:.2f}")
        except Exception as e:
            print(f"  Error getting session analytics: {e}")

        # Sample funnel metrics
        print("\\n🎯 Conversion Funnel (Latest):")
        try:
            funnel_data = self.t_env.sql_query("""
                SELECT * FROM conversion_funnel
                ORDER BY window_start DESC
                LIMIT 1
            """).execute().collect()

            for row in funnel_data:
                if row.funnel_start > 0:
                    view_to_cart = (row.cart_adders / row.funnel_start * 100)
                    cart_to_purchase = (row.purchasers / NULLIF(row.cart_adders, 0) * 100)
                    overall = (row.purchasers / row.funnel_start * 100)

                    print(f"  Views: {row.unique_viewers}")
                    print(f"  → Add to Cart: {row.cart_adders} ({view_to_cart:.1f}%)")
                    print(f"  → Purchases: {row.purchasers} ({cart_to_purchase:.1f}%)")
                    print(f"  Overall Conversion: {overall:.1f}%")
        except Exception as e:
            print(f"  Error getting funnel metrics: {e}")

        print("\\n" + "="*60)

    def execute_processing(self):
        """
        Execute the complete processing pipeline

        Learning: Shows the complete flow from source to sink
        """
        print("🚀 Starting Flink Processing Pipeline")
        print("="*50)

        try:
            # Create source table
            self.create_kafka_source_table()

            # Create sink table
            self.create_clickhouse_sink_table()

            # Create analytical views
            self.create_temporal_views()

            # Advanced analytics
            self.create_funnel_analysis()
            self.create_anomaly_detection()
            self.create_dashboard_views()

            # Start sinking to ClickHouse
            self.sink_to_clickhouse()

            # Print sample output for learning
            print("\\n📈 Processing pipeline started successfully!")
            print("💡 Sample outputs will be printed every 30 seconds for learning...")

            # Learning: Continuous output for monitoring
            while True:
                self.print_sample_output()
                time.sleep(30)  # Print every 30 seconds

        except Exception as e:
            print(f"❌ Error in processing pipeline: {e}")
            raise

    def stop(self):
        """Stop the Flink environment gracefully"""
        if self.t_env:
            self.t_env.execute("STOP JOB")
        print("🛑 Flink processor stopped")