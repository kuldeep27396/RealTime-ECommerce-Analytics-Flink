"""
ClickHouse database manager for analytics storage

Learning: This demonstrates:
- Database schema design for analytics
- Efficient data insertion strategies
- Query optimization patterns
- Connection management
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import structlog

try:
    from clickhouse_connect import get_client
    from clickhouse_driver import Client

    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False
    print(
        "Warning: ClickHouse libraries not available. Install with: pip install clickhouse-connect clickhouse-driver"
    )

from .config import config
from .models import FunnelMetrics, RealTimeMetrics, TimeWindowMetrics

# Configure structured logging
logger = structlog.get_logger(__name__)


class ClickHouseManager:
    """
    ClickHouse manager for analytics data storage and retrieval

    Learning: Shows production patterns for:
    - Connection pooling
    - Schema management
    - Batch operations
    - Query optimization
    """

    def __init__(self):
        self.client = None
        self.driver_client = None
        self.connected = False
        self.setup_connection()

    def setup_connection(self):
        """Setup ClickHouse connection with retry logic"""
        if not CLICKHOUSE_AVAILABLE:
            logger.error("ClickHouse libraries not available")
            return

        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Learning: Use both clients for different operations
                # clickhouse-connect for modern operations
                self.client = get_client(
                    host=config.CLICKHOUSE_HOST,
                    port=config.CLICKHOUSE_PORT,
                    username=config.CLICKHOUSE_USER,
                    password=config.CLICKHOUSE_PASSWORD,
                    database=config.CLICKHOUSE_DATABASE,
                )

                # clickhouse-driver for compatibility
                self.driver_client = Client(
                    host=config.CLICKHOUSE_HOST,
                    port=config.CLICKHOUSE_PORT,
                    user=config.CLICKHOUSE_USER,
                    password=config.CLICKHOUSE_PASSWORD,
                    database=config.CLICKHOUSE_DATABASE,
                )

                # Test connection
                self.client.query("SELECT 1")
                self.connected = True
                logger.info("✅ ClickHouse connection established")
                self.create_tables()
                return

            except Exception as e:
                logger.error(f"ClickHouse connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff

        logger.error("❌ Failed to connect to ClickHouse")

    def create_tables(self):
        """
        Create optimized ClickHouse tables for analytics

        Learning: Shows ClickHouse-specific optimizations:
        - Engine selection (MergeTree for time series)
        - Partitioning strategies
        - Indexing
        - Compression settings
        """

        # Learning: Main analytics table with MergeTree engine
        main_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}.ecommerce_analytics (
            window_start DateTime,
            window_end DateTime,
            metric_name LowCardinality(String),
            metric_value Int64,
            product_category LowCardinality(String),
            event_type LowCardinality(String),
            device_type LowCardinality(String),
            processing_time DateTime,
            -- Learning: Additional fields for filtering and aggregation
            revenue Float32,
            user_count UInt32,
            -- Learning: Partitioning and ordering keys
            date Date DEFAULT toDate(window_start)
        ) ENGINE = MergeTree()
        PARTITION BY date
        ORDER BY (window_start, metric_name, product_category)
        SETTINGS index_granularity = 8192
        """
        self.execute_query(main_table_sql)

        # Learning: Raw events table for detailed analysis
        raw_events_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}.raw_clickstream_events (
            timestamp DateTime,
            user_id String,
            session_id String,
            event_type LowCardinality(String),
            product_id String,
            product_category LowCardinality(String),
            price Float32,
            quantity UInt16,
            source LowCardinality(String),
            device_type LowCardinality(String),
            revenue Float32,
            hour_of_day UInt8,
            day_of_week UInt8,
            is_weekend UInt8,
            price_category LowCardinality(String),
            processing_time DateTime,
            date Date DEFAULT toDate(timestamp)
        ) ENGINE = MergeTree()
        PARTITION BY date
        ORDER BY (timestamp, user_id, session_id)
        SETTINGS index_granularity = 16384
        """
        self.execute_query(raw_events_table_sql)

        # Learning: Aggregated metrics table for fast queries
        aggregated_metrics_sql = f"""
        CREATE TABLE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}.aggregated_metrics (
            date Date,
            hour UInt8,
            metric_name LowCardinality(String),
            product_category LowCardinality(String),
            device_type LowCardinality(String),
            event_count UInt64,
            unique_users UInt32,
            total_revenue Float64,
            avg_order_value Float64,
            conversion_rate Float32
        ) ENGINE = SummingMergeTree()
        PARTITION BY date
        ORDER BY (date, hour, metric_name, product_category, device_type)
        """
        self.execute_query(aggregated_metrics_sql)

        # Learning: Materialized view for real-time aggregation
        materialized_view_sql = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {config.CLICKHOUSE_DATABASE}.metrics_mv
        TO {config.CLICKHOUSE_DATABASE}.aggregated_metrics
        AS SELECT
            toDate(timestamp) as date,
            toHour(timestamp) as hour,
            event_type as metric_name,
            product_category,
            device_type,
            count() as event_count,
            countDistinct(user_id) as unique_users,
            sum(revenue) as total_revenue,
            avg(revenue) as avg_order_value,
            sumIf(revenue, event_type = 'purchase') / countIf(event_type = 'view') as conversion_rate
        FROM {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
        GROUP BY date, hour, event_type, product_category, device_type
        """
        self.execute_query(materialized_view_sql)

        logger.info("✅ ClickHouse tables created")

    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """Execute ClickHouse query with error handling"""
        if not self.connected:
            logger.error("Not connected to ClickHouse")
            return None

        try:
            if params:
                result = self.client.query(query, parameters=params)
            else:
                result = self.client.query(query)
            return result
        except Exception as e:
            logger.error(f"ClickHouse query failed: {e}")
            logger.error(f"Query: {query[:200]}...")
            return None

    def insert_analytics_batch(self, metrics: List[Dict[str, Any]]):
        """
        Insert batch of analytics metrics

        Learning: Batch insertion for performance
        """
        if not self.connected or not metrics:
            return

        try:
            # Learning: Use batch insertion for better performance
            data = []
            for metric in metrics:
                data.append(
                    (
                        metric.get("window_start", datetime.now()),
                        metric.get("window_end", datetime.now()),
                        metric.get("metric_name", "unknown"),
                        metric.get("metric_value", 0),
                        metric.get("product_category", "unknown"),
                        metric.get("event_type", "unknown"),
                        metric.get("device_type", "unknown"),
                        datetime.now(),
                        metric.get("revenue", 0.0),
                        metric.get("user_count", 0),
                    )
                )

            insert_sql = f"""
            INSERT INTO {config.CLICKHOUSE_DATABASE}.ecommerce_analytics
            (window_start, window_end, metric_name, metric_value, product_category, event_type, device_type, processing_time, revenue, user_count)
            VALUES
            """

            # Learning: Use driver client for batch inserts
            self.driver_client.execute(insert_sql, data)
            logger.info(f"✅ Inserted {len(data)} analytics records")

        except Exception as e:
            logger.error(f"Failed to insert analytics batch: {e}")

    def insert_raw_events_batch(self, events: List[Dict[str, Any]]):
        """
        Insert batch of raw events for detailed analysis

        Learning: Raw event storage for reproducibility and reprocessing
        """
        if not self.connected or not events:
            return

        try:
            data = []
            for event in events:
                data.append(
                    (
                        datetime.fromtimestamp(event.get("timestamp", 0) / 1000),
                        event.get("user_id", ""),
                        event.get("session_id", ""),
                        event.get("event_type", ""),
                        event.get("product_id", ""),
                        event.get("product_category", ""),
                        float(event.get("price", 0.0)),
                        int(event.get("quantity", 1)),
                        event.get("source", ""),
                        event.get("device_type", ""),
                        float(event.get("revenue", 0.0)),
                        int(event.get("hour_of_day", 0)),
                        int(event.get("day_of_week", 0)),
                        1 if event.get("is_weekend", False) else 0,
                        event.get("price_category", ""),
                        datetime.now(),
                    )
                )

            insert_sql = f"""
            INSERT INTO {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            (timestamp, user_id, session_id, event_type, product_id, product_category, price, quantity, source, device_type, revenue, hour_of_day, day_of_week, is_weekend, price_category, processing_time)
            VALUES
            """

            self.driver_client.execute(insert_sql, data)
            logger.info(f"✅ Inserted {len(data)} raw events")

        except Exception as e:
            logger.error(f"Failed to insert raw events batch: {e}")

    def get_real_time_dashboard(self, minutes: int = 5) -> Dict[str, Any]:
        """
        Get real-time dashboard data

        Learning: Optimized queries for dashboards
        """
        if not self.connected:
            return {}

        try:
            # Learning: Use materialized view for fast aggregation
            dashboard_query = f"""
            SELECT
                count() as total_events,
                countDistinct(user_id) as unique_users,
                sum(revenue) as total_revenue,
                avg(revenue) as avg_order_value,
                countDistinct(product_id) as unique_products,
                countDistinct(session_id) as active_sessions,
                any(processing_time) as last_update
            FROM {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            WHERE timestamp >= now() - INTERVAL {minutes} MINUTE
            """

            result = self.execute_query(dashboard_query)
            if result and result.result_rows:
                row = result.result_rows[0]
                return {
                    "total_events": row[0],
                    "unique_users": row[1],
                    "total_revenue": row[2],
                    "avg_order_value": row[3],
                    "unique_products": row[4],
                    "active_sessions": row[5],
                    "last_update": row[6],
                }

        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")

        return {}

    def get_conversion_funnel(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get conversion funnel data

        Learning: Complex aggregation for funnel analysis
        """
        if not self.connected:
            return []

        try:
            funnel_query = f"""
            SELECT
                toStartOfHour(timestamp) as hour,
                countIf(event_type = 'view') as views,
                countIf(event_type = 'add_to_cart') as add_to_carts,
                countIf(event_type = 'purchase') as purchases,
                countIf(event_type = 'view') as funnel_start
            FROM {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            WHERE timestamp >= now() - INTERVAL {hours} HOUR
            GROUP BY hour
            ORDER BY hour
            """

            result = self.execute_query(funnel_query)
            funnel_data = []

            if result and result.result_rows:
                for row in result.result_rows:
                    funnel_data.append(
                        {
                            "hour": row[0],
                            "views": row[1],
                            "add_to_carts": row[2],
                            "purchases": row[3],
                            "funnel_start": row[4],
                        }
                    )

            return funnel_data

        except Exception as e:
            logger.error(f"Failed to get funnel data: {e}")
            return []

    def get_top_products(
        self, hours: int = 24, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get top performing products

        Learning: Ranking and sorting operations
        """
        if not self.connected:
            return []

        try:
            top_products_query = f"""
            SELECT
                product_id,
                product_category,
                countIf(event_type = 'view') as views,
                countIf(event_type = 'purchase') as purchases,
                sumIf(revenue, event_type = 'purchase') as revenue,
                (purchases * 100.0) / views as conversion_rate
            FROM {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            WHERE timestamp >= now() - INTERVAL {hours} HOUR
               AND product_id != ''
            GROUP BY product_id, product_category
            HAVING views > 0
            ORDER BY revenue DESC
            LIMIT {limit}
            """

            result = self.execute_query(top_products_query)
            products = []

            if result and result.result_rows:
                for row in result.result_rows:
                    products.append(
                        {
                            "product_id": row[0],
                            "product_category": row[1],
                            "views": row[2],
                            "purchases": row[3],
                            "revenue": row[4],
                            "conversion_rate": row[5],
                        }
                    )

            return products

        except Exception as e:
            logger.error(f"Failed to get top products: {e}")
            return []

    def get_user_behavior_analysis(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get user behavior analytics

        Learning: Advanced behavioral analytics
        """
        if not self.connected:
            return {}

        try:
            behavior_query = f"""
            SELECT
                countDistinct(user_id) as total_users,
                avg(length(groupArrayIf(event_type, timestamp >= now() - INTERVAL {hours} HOUR))) as avg_events_per_user,
                avg(avgIf(price, event_type = 'purchase')) as avg_purchase_value,
                quantileExact(0.5)(length(groupArrayIf(event_type, timestamp >= now() - INTERVAL {hours} HOUR))) as median_events_per_user,
                sumIf(revenue, event_type = 'purchase') / countDistinct(user_id) as revenue_per_user
            FROM {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            WHERE timestamp >= now() - INTERVAL {hours} HOUR
            """

            result = self.execute_query(behavior_query)
            if result and result.result_rows:
                row = result.result_rows[0]
                return {
                    "total_users": row[0],
                    "avg_events_per_user": row[1],
                    "avg_purchase_value": row[2],
                    "median_events_per_user": row[3],
                    "revenue_per_user": row[4],
                }

        except Exception as e:
            logger.error(f"Failed to get user behavior analysis: {e}")

        return {}

    def cleanup_old_data(self, days_to_keep: int = 30):
        """
        Cleanup old data to manage storage costs

        Learning: Data retention policies
        """
        if not self.connected:
            return

        try:
            cleanup_query = f"""
            ALTER TABLE {config.CLICKHOUSE_DATABASE}.raw_clickstream_events
            DELETE WHERE date < today() - {days_to_keep}
            """
            self.execute_query(cleanup_query)
            logger.info(f"✅ Cleaned up data older than {days_to_keep} days")

        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")

    def get_table_stats(self) -> Dict[str, Any]:
        """
        Get table statistics for monitoring

        Learning: Database monitoring
        """
        if not self.connected:
            return {}

        try:
            stats = {}
            tables = [
                "ecommerce_analytics",
                "raw_clickstream_events",
                "aggregated_metrics",
            ]

            for table in tables:
                table_name = f"{config.CLICKHOUSE_DATABASE}.{table}"
                count_query = f"SELECT count() FROM {table_name}"
                result = self.execute_query(count_query)
                if result and result.result_rows:
                    stats[table] = result.result_rows[0][0]

            return stats

        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}

    def close(self):
        """Close ClickHouse connections"""
        if self.client:
            self.client.close()
        if self.driver_client:
            self.driver_client.close()
        self.connected = False
        logger.info("ClickHouse connections closed")
