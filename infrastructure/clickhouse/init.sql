-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce;

-- Use the database
USE ecommerce;

-- Create raw clickstream events table with optimized schema
CREATE TABLE IF NOT EXISTS raw_clickstream_events (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    event_date Date CODEC(Delta, ZSTD(1)),
    user_id String CODEC(ZSTD(1)),
    session_id String CODEC(ZSTD(1)),
    event_type LowCardinality(String) CODEC(ZSTD(1)),
    product_id String CODEC(ZSTD(1)),
    device_type LowCardinality(String) CODEC(ZSTD(1)),
    source_type LowCardinality(String) CODEC(ZSTD(1)),
    url String CODEC(ZSTD(1)),
    referrer String CODEC(ZSTD(1)),
    user_agent String CODEC(ZSTD(1)),
    ip_address String CODEC(ZSTD(1)),
    country LowCardinality(String) CODEC(ZSTD(1)),
    city LowCardinality(String) CODEC(ZSTD(1)),
    price Float32 CODEC(Gorilla, ZSTD(1)),
    revenue Float32 CODEC(Gorilla, ZSTD(1)),
    is_weekend UInt8 CODEC(Delta, ZSTD(1)),
    hour_of_day UInt8 CODEC(Delta, ZSTD(1)),
    day_of_week UInt8 CODEC(Delta, ZSTD(1)),
    price_category LowCardinality(String) CODEC(ZSTD(1)),
    user_segment LowCardinality(String) CODEC(ZSTD(1)),
    conversion_probability Float32 CODEC(Gorilla, ZSTD(1)),
    engagement_score Float32 CODEC(Gorilla, ZSTD(1)),
    processed_at DateTime CODEC(Delta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (user_id, session_id, timestamp)
SETTINGS index_granularity = 8192;

-- Create aggregated analytics table for faster queries
CREATE TABLE IF NOT EXISTS analytics_events_hourly (
    event_date Date CODEC(Delta, ZSTD(1)),
    hour UInt8 CODEC(Delta, ZSTD(1)),
    event_type LowCardinality(String) CODEC(ZSTD(1)),
    device_type LowCardinality(String) CODEC(ZSTD(1)),
    source_type LowCardinality(String) CODEC(ZSTD(1)),
    country LowCardinality(String) CODEC(ZSTD(1)),
    price_category LowCardinality(String) CODEC(ZSTD(1)),
    user_segment LowCardinality(String) CODEC(ZSTD(1)),
    events_count UInt64 CODEC(Delta, ZSTD(1)),
    unique_users UInt32 CODEC(Delta, ZSTD(1)),
    unique_sessions UInt32 CODEC(Delta, ZSTD(1)),
    total_revenue Float64 CODEC(Gorilla, ZSTD(1)),
    avg_price Float32 CODEC(Gorilla, ZSTD(1)),
    conversion_rate Float32 CODEC(Gorilla, ZSTD(1)),
    avg_engagement_score Float32 CODEC(Gorilla, ZSTD(1))
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, hour, event_type, device_type, source_type, country, price_category, user_segment)
SETTINGS index_granularity = 8192;

-- Create real-time dashboard data table
CREATE TABLE IF NOT EXISTS dashboard_metrics (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    metric_name LowCardinality(String) CODEC(ZSTD(1)),
    metric_value Float64 CODEC(Gorilla, ZSTD(1)),
    dimensions Map(String, String) CODEC(ZSTD(1)),
    updated_at DateTime CODEC(Delta, ZSTD(1))
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (metric_name, timestamp, dimensions)
SETTINGS index_granularity = 8192;

-- Create user behavior patterns table
CREATE TABLE IF NOT EXISTS user_behavior_patterns (
    user_id String CODEC(ZSTD(1)),
    session_id String CODEC(ZSTD(1)),
    event_sequence Array(LowCardinality(String)) CODEC(ZSTD(1)),
    sequence_length UInt16 CODEC(Delta, ZSTD(1)),
    pattern_start DateTime CODEC(Delta, ZSTD(1)),
    pattern_end DateTime CODEC(Delta, ZSTD(1)),
    time_spent_seconds UInt32 CODEC(Delta, ZSTD(1)),
    pages_viewed UInt16 CODEC(Delta, ZSTD(1)),
    products_added UInt16 CODEC(Delta, ZSTD(1)),
    products_purchased UInt16 CODEC(Delta, ZSTD(1)),
    total_revenue Float32 CODEC(Gorilla, ZSTD(1)),
    pattern_type LowCardinality(String) CODEC(ZSTD(1)),
    conversion_probability Float32 CODEC(Gorilla, ZSTD(1)),
    created_at DateTime CODEC(Delta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, created_at)
SETTINGS index_granularity = 8192;

-- Create anomaly detection results table
CREATE TABLE IF NOT EXISTS anomaly_detection (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    anomaly_type LowCardinality(String) CODEC(ZSTD(1)),
    severity LowCardinality(String) CODEC(ZSTD(1)),
    metric_name String CODEC(ZSTD(1)),
    current_value Float64 CODEC(Gorilla, ZSTD(1)),
    expected_value Float64 CODEC(Gorilla, ZSTD(1)),
    deviation_percentage Float32 CODEC(Gorilla, ZSTD(1)),
    confidence_score Float32 CODEC(Gorilla, ZSTD(1)),
    dimensions Map(String, String) CODEC(ZSTD(1)),
    description String CODEC(ZSTD(1)),
    created_at DateTime CODEC(Delta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (timestamp, anomaly_type, severity)
SETTINGS index_granularity = 8192;

-- Create materialized views for real-time aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics_events_hourly_mv
TO analytics_events_hourly
AS SELECT
    toDate(timestamp) AS event_date,
    toHour(timestamp) AS hour,
    event_type,
    device_type,
    source_type,
    country,
    price_category,
    user_segment,
    count() AS events_count,
    count(DISTINCT user_id) AS unique_users,
    count(DISTINCT session_id) AS unique_sessions,
    sumIf(revenue, event_type = 'purchase') AS total_revenue,
    avgIf(price, event_type = 'purchase') AS avg_price,
    (sumIf(revenue, event_type = 'purchase') * 100.0) / NULLIF(countIf(event_type, 'view'), 0) AS conversion_rate,
    avg(engagement_score) AS avg_engagement_score
FROM raw_clickstream_events
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY event_date, hour, event_type, device_type, source_type, country, price_category, user_segment;

-- Create distributed table for large-scale deployments (if needed)
-- CREATE TABLE IF NOT EXISTS raw_clickstream_events_all AS raw_clickstream_events
-- ENGINE = Distributed('cluster_name', 'ecommerce', 'raw_clickstream_events', rand());

-- Create optimizations for common query patterns
CREATE PROJECTION IF NOT EXISTS raw_clickstream_events_projection
(
    SELECT
        user_id,
        session_id,
        timestamp,
        event_type,
        device_type,
        source_type,
        country,
        price_category,
        user_segment,
        revenue,
        conversion_probability,
        engagement_score
    ORDER BY (user_id, timestamp)
);

-- Grant permissions
GRANT SELECT, INSERT ON ecommerce.* TO default;

-- Show created tables
SHOW TABLES;