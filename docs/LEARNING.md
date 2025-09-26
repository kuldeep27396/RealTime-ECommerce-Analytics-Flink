# Learning Guide: Real-Time Data Processing

This guide explains key concepts and provides hands-on exercises for learning real-time data processing with this project.

## 🎯 Learning Objectives

By the end of this guide, you'll understand:
- Event-driven architecture patterns
- Apache Flink stream processing
- Real-time analytics with ClickHouse
- Production deployment strategies
- Monitoring and observability

## 🏗️ Architecture Deep Dive

### 1. Data Flow Pipeline

```
API → Producer → Kafka → Flink → ClickHouse → Dashboards
```

**Learning Points:**
- Each component has a specific responsibility
- Data flows through transformation stages
- Fault tolerance at each layer
- Scalability considerations

### 2. Component Responsibilities

#### Producer Service
- **Data Ingestion**: Fetch data from external APIs
- **Transformation**: Clean and enrich data
- **Publishing**: Send to message queue
- **Error Handling**: Retry failed operations

#### Consumer Service
- **Stream Processing**: Real-time computations
- **Window Operations**: Time-based aggregations
- **State Management**: Track processing state
- **Storage**: Persist results

## 🔄 Core Concepts

### Event Time vs Processing Time

```python
# In transformer.py
def _parse_timestamp(self, timestamp_str: str) -> int:
    """Event time: When the event actually occurred"""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except ValueError:
        # Fallback to processing time
        return int(datetime.now().timestamp() * 1000)
```

**Why it matters:**
- Accurate analytics require event time
- Handle out-of-order events
- Compute correct windows

### Windowing Strategies

#### Tumbling Windows
```python
# Fixed-size, non-overlapping windows
TUMBLE(event_time, INTERVAL '1' MINUTE)
```

**Use cases:**
- Hourly summaries
- Daily aggregations
- Fixed reporting periods

#### Sliding Windows
```python
# Overlapping windows for smoother analytics
SLIDE(event_time, INTERVAL '5' MINUTE, INTERVAL '1' MINUTE)
```

**Use cases:**
- Real-time dashboards
- Moving averages
- Smooth trend analysis

#### Session Windows
```python
# Dynamic windows based on activity gaps
SESSION(event_time, INTERVAL '10' MINUTE)
```

**Use cases:**
- User session analysis
- Behavior pattern detection
- Activity clustering

## 🧪 Hands-on Exercises

### Exercise 1: Basic Data Transformation

**Goal**: Add a new field to track mobile vs desktop engagement

1. Open `services/producer/src/transformer.py`
2. Add new field to `TransformedClickstreamEvent` model:
```python
engagement_score: float = Field(ge=0.0, le=1.0)
```

3. Implement scoring logic:
```python
def calculate_engagement_score(self, event: TransformedClickstreamEvent) -> float:
    """Calculate engagement based on multiple factors"""
    score = 0.0

    # Points for different event types
    event_scores = {
        'view': 0.1,
        'add_to_cart': 0.3,
        'purchase': 0.5
    }

    score += event_scores.get(event.event_type, 0.0)

    # Bonus for weekend activity
    if event.is_weekend:
        score += 0.1

    return min(score, 1.0)
```

4. Update `transform_event` method to use the new scoring

### Exercise 2: Real-time Anomaly Detection

**Goal**: Detect unusual traffic patterns

1. Open `services/consumer/src/flink_processor.py`
2. Add anomaly detection query:
```python
def create_traffic_anomaly_detection(self):
    """Detect traffic spikes and drops"""
    anomaly_sql = """
    CREATE VIEW traffic_anomalies AS
    SELECT
        window_start,
        event_count,
        LAG(event_count, 1) OVER (ORDER BY window_start) as prev_count,
        (event_count - prev_count) * 100.0 / NULLIF(prev_count, 0) as percentage_change,
        CASE
            WHEN percentage_change > 200 THEN 'SPIKE'
            WHEN percentage_change < -70 THEN 'DROP'
            ELSE 'NORMAL'
        END as anomaly_type
    FROM (
        SELECT
            TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
            COUNT(*) as event_count
        FROM clickstream_events
        GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE')
    )
    """
    self.t_env.execute_sql(anomaly_sql)
```

### Exercise 3: User Behavior Analytics

**Goal**: Track user behavior patterns over time

1. Open `services/consumer/src/clickhouse_manager.py`
2. Add behavior analysis method:
```python
def get_user_behavior_patterns(self, days: int = 7) -> Dict[str, Any]:
    """Analyze user behavior patterns"""

    query = f"""
    SELECT
        user_id,
        count(*) as total_events,
        count(DISTINCT session_id) as session_count,
        avg(price) as avg_price_paid,
        sumIf(revenue, event_type = 'purchase') as total_spent,
        countIf(event_type = 'purchase') as purchase_count,
        countIf(event_type = 'view') as view_count,
        (purchase_count * 100.0) / view_count as conversion_rate
    FROM raw_clickstream_events
    WHERE timestamp >= now() - INTERVAL {days} DAY
    GROUP BY user_id
    HAVING view_count > 10
    ORDER BY total_spent DESC
    LIMIT 1000
    """

    result = self.execute_query(query)
    return self._format_behavior_patterns(result)
```

### Exercise 4: Advanced Window Operations

**Goal**: Implement custom windowing for business hours

1. Create business hours filter:
```python
def create_business_hours_analytics(self):
    """Analytics for business hours only (9 AM - 6 PM)"""
    business_hours_sql = """
    CREATE VIEW business_hours_analytics AS
    SELECT
        TUMBLE_START(event_time, INTERVAL '1' HOUR) as hour_start,
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        SUM(revenue) as total_revenue
    FROM clickstream_events
    WHERE
        -- Business hours: 9 AM to 6 PM (hour 9 to 17)
        toHour(event_time) BETWEEN 9 AND 17
        -- Weekdays only
        toDayOfWeek(event_time) BETWEEN 1 AND 5
    GROUP BY
        TUMBLE(event_time, INTERVAL '1' HOUR),
        event_type
    """
    self.t_env.execute_sql(business_hours_sql)
```

### Exercise 5: Performance Optimization

**Goal**: Optimize ClickHouse queries with proper indexing

1. Analyze query patterns:
```python
def get_query_performance_stats(self):
    """Get performance statistics for common queries"""

    queries = [
        "SELECT COUNT(*) FROM raw_clickstream_events WHERE date = today()",
        "SELECT event_type, COUNT(*) FROM raw_clickstream_events GROUP BY event_type",
        "SELECT user_id, COUNT(*) FROM raw_clickstream_events GROUP BY user_id LIMIT 100"
    ]

    stats = {}
    for query in queries:
        start_time = time.time()
        result = self.execute_query(query)
        duration = time.time() - start_time

        stats[query[:50] + "..."] = {
            'duration_seconds': duration,
            'rows_returned': len(result.result_rows) if result else 0
        }

    return stats
```

## 📊 Monitoring Exercises

### Exercise 6: Custom Metrics

**Goal**: Add business metrics to monitoring

1. Add custom metrics collection:
```python
class BusinessMetrics:
    def __init__(self):
        self.metrics = {
            'conversion_rate': 0.0,
            'average_order_value': 0.0,
            'cart_abandonment_rate': 0.0,
            'user_engagement_score': 0.0
        }

    def update_metrics(self, clickhouse_manager):
        """Update business metrics from database"""
        # Get conversion rate
        funnel = clickhouse_manager.get_conversion_funnel(hours=1)
        if funnel:
            latest = funnel[-1]
            self.metrics['conversion_rate'] = self._calculate_conversion_rate(latest)

        # Get average order value
        dashboard = clickhouse_manager.get_real_time_dashboard(minutes=60)
        if dashboard:
            self.metrics['average_order_value'] = dashboard.get('avg_order_value', 0.0)
```

### Exercise 7: Alerting System

**Goal**: Implement automated alerts for anomalies

1. Create alerting system:
```python
class AlertManager:
    def __init__(self):
        self.alerts = []
        self.thresholds = {
            'high_error_rate': 5.0,      # 5% error rate
            'low_throughput': 100,       # < 100 events/min
            'high_latency': 5000,       # > 5 seconds
            'conversion_drop': 50.0     # 50% drop in conversion
        }

    def check_alerts(self, metrics):
        """Check metrics against thresholds"""

        # Check error rate
        if metrics.get('error_rate', 0) > self.thresholds['high_error_rate']:
            self._create_alert('high_error_rate', metrics['error_rate'])

        # Check throughput
        if metrics.get('events_per_minute', 0) < self.thresholds['low_throughput']:
            self._create_alert('low_throughput', metrics['events_per_minute'])

        return self.alerts
```

## 🔧 Debugging Techniques

### Common Issues and Solutions

#### 1. Kafka Connection Issues
```python
# Debug connection problems
def debug_kafka_connection(config):
    print("Testing Kafka connection...")
    print(f"Bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {config.KAFKA_TOPIC}")

    try:
        # Test connection
        producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=config.KAFKA_API_KEY,
            sasl_plain_password=config.KAFKA_API_SECRET,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Kafka connection successful")
        producer.close()
    except Exception as e:
        print(f"❌ Kafka connection failed: {e}")
```

#### 2. Flink Job Monitoring
```python
# Monitor Flink job status
def monitor_flink_job(t_env):
    """Check Flink job health"""
    try:
        # Get job status
        jobs = t_env.list_jobs()
        for job in jobs:
            print(f"Job ID: {job.job_id}")
            print(f"Status: {job.status}")
            print(f"Start time: {job.start_time}")

            # Check for errors
            if job.status == 'FAILED':
                print(f"Error: {job.error}")
    except Exception as e:
        print(f"Failed to monitor Flink job: {e}")
```

#### 3. ClickHouse Query Optimization
```python
# Optimize ClickHouse queries
def optimize_clickhouse_query(query):
    """Apply common optimizations"""

    # Add LIMIT for large result sets
    if 'LIMIT' not in query.upper():
        query += ' LIMIT 1000'

    # Use PREWHERE for filtering
    if 'WHERE' in query.upper() and 'PREWHERE' not in query.upper():
        query = query.replace('WHERE', 'PREWHERE', 1)

    # Add sampling for large tables
    if 'FROM raw_clickstream_events' in query and 'SAMPLE' not in query.upper():
        query = query.replace('FROM raw_clickstream_events', 'FROM raw_clickstream_events SAMPLE 0.1', 1)

    return query
```

## 🚀 Advanced Topics

### 1. Stateful Processing
```python
# Stateful aggregation in Flink
def create_stateful_aggregation(self):
    """Track user state over time"""
    stateful_sql = """
    CREATE TABLE user_state (
        user_id STRING,
        session_count INT,
        total_events INT,
        total_revenue DOUBLE,
        last_event_time TIMESTAMP,
        PRIMARY KEY (user_id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'user_state_topic',
        'properties.bootstrap.servers' = 'kafka:9092',
        'key.format' = 'raw',
        'value.format' = 'json'
    )
    """
    self.t_env.execute_sql(stateful_sql)
```

### 2. Complex Event Processing
```python
# Pattern detection with CEP
def detect_user_patterns(self):
    """Detect complex user behavior patterns"""
    pattern_sql = """
    CREATE VIEW user_patterns AS
    SELECT
        user_id,
        session_id,
        LISTAGG(event_type) WITHIN GROUP (ORDER BY event_time) as event_sequence,
        COUNT(*) as sequence_length,
        MIN(event_time) as pattern_start,
        MAX(event_time) as pattern_end
    FROM (
        SELECT
            user_id,
            session_id,
            event_time,
            event_type,
            LAG(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time) as prev_time
        FROM clickstream_events
        WHERE event_time >= NOW() - INTERVAL '1' HOUR
    )
    WHERE prev_time IS NOT NULL
      AND event_time - prev_time < INTERVAL '5' MINUTE
    GROUP BY user_id, session_id
    HAVING COUNT(*) >= 3
    """
    self.t_env.execute_sql(pattern_sql)
```

### 3. Machine Learning Integration
```python
# Real-time ML predictions
def integrate_ml_predictions(self):
    """Add ML predictions to analytics"""
    ml_sql = """
    CREATE VIEW ml_predictions AS
    SELECT
        user_id,
        session_id,
        event_type,
        -- Simple rule-based ML model
        CASE
            WHEN COUNT(*) OVER (PARTITION BY user_id) > 10 THEN 'HIGH_ENGAGEMENT'
            WHEN SUM(revenue) OVER (PARTITION BY user_id) > 100 THEN 'HIGH_VALUE'
            WHEN COUNT(DISTINCT product_id) OVER (PARTITION BY user_id) > 5 THEN 'BROWSER'
            ELSE 'NEW_USER'
        END as user_segment,
        -- Predict conversion probability
        (COUNTIf(event_type = 'add_to_cart') * 1.0 /
         NULLIF(COUNTIf(event_type = 'view'), 0)) * 100 as conversion_probability
    FROM clickstream_events
    WHERE event_time >= NOW() - INTERVAL '30' MINUTE
    """
    self.t_env.execute_sql(ml_sql)
```

## 📚 Best Practices

### 1. Code Organization
- Separate concerns: ingestion, processing, storage
- Use configuration management
- Implement comprehensive error handling
- Add extensive logging and monitoring

### 2. Performance Optimization
- Use appropriate windowing strategies
- Implement batching for I/O operations
- Optimize database queries
- Monitor resource usage

### 3. Production Readiness
- Add health checks and monitoring
- Implement graceful shutdown
- Use circuit breakers for external dependencies
- Plan for failure scenarios

### 4. Learning Approach
- Start with simple transformations
- Gradually add complexity
- Experiment with different windowing strategies
- Monitor performance impact of changes

## 🎯 Next Steps

After completing these exercises, you'll be ready to:
1. Build your own real-time analytics pipeline
2. Optimize for performance and cost
3. Deploy to production environments
4. Monitor and troubleshoot issues
5. Extend with custom analytics

Remember to experiment, break things, and learn from the results!