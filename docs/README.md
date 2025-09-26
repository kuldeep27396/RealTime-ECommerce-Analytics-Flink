# Real-Time E-commerce Analytics with Flink and ClickHouse

## 🎯 Project Overview

This project demonstrates a production-ready real-time analytics pipeline that processes e-commerce clickstream data using Apache Flink and stores results in ClickHouse. The architecture is designed for scalability, maintainability, and learning.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Clickstream   │───▶│   Producer      │───▶│  Confluent      │───▶│   Consumer      │
│     API         │    │   Service       │    │     Cloud        │    │   Service       │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
                                                                │
                                                                ▼
                                                       ┌─────────────────┐
                                                       │   ClickHouse    │
                                                       │   Analytics     │
                                                       └─────────────────┘
```

## 🚀 Features

### Producer Service (`services/producer/`)
- **Real-time data ingestion** from clickstream API
- **Data transformation** and validation
- **Kafka integration** with Confluent Cloud
- **Error handling** and retry mechanisms
- **Monitoring** and health checks

### Consumer Service (`services/consumer/`)
- **Apache Flink** stream processing
- **Advanced analytics** with multiple windowing strategies
- **ClickHouse integration** for analytics storage
- **Real-time dashboards** and metrics
- **Anomaly detection** and monitoring

## 📊 Learning Opportunities

This project is designed for learning real-time data processing concepts:

### Data Processing Patterns
- **Event time processing** with watermarks
- **Windowing strategies** (Tumble, Slide, Session)
- **Stateful operations** and aggregations
- **Complex event processing** (CEP)

### Production Patterns
- **Exactly-once semantics** with checkpointing
- **Backpressure handling**
- **Graceful shutdown** and error recovery
- **Monitoring** and observability

### Analytics and ML
- **Real-time aggregations**
- **Conversion funnel analysis**
- **User behavior analytics**
- **Anomaly detection**

## 🔧 Quick Start

### Prerequisites
- Python 3.11+
- Confluent Cloud account
- ClickHouse instance
- Railway account (for deployment)

### 1. Clone and Setup
```bash
git clone <repository-url>
cd RealTime-ECommerce-Analytics-Flink

# Create virtual environments
python -m venv producer-env
python -m venv consumer-env

# Activate environments
source producer-env/bin/activate  # For producer
source consumer-env/bin/activate  # For consumer
```

### 2. Configure Secrets
Add the following secrets to your GitHub repository:

**Kafka Configuration:**
- `KAFKA_BOOTSTRAP_SERVERS`: Confluent Cloud bootstrap server
- `KAFKA_TOPIC`: Your Kafka topic name
- `KAFKA_API_KEY`: Confluent Cloud API key
- `KAFKA_API_SECRET`: Confluent Cloud API secret

**ClickHouse Configuration:**
- `CLICKHOUSE_HOST`: ClickHouse server address
- `CLICKHOUSE_PORT`: ClickHouse port (default: 8123)
- `CLICKHOUSE_DATABASE`: Database name
- `CLICKHOUSE_USER`: Username
- `CLICKHOUSE_PASSWORD`: Password

**Railway Configuration:**
- `RAILWAY_TOKEN`: Railway API token
- `RAILWAY_PROJECT_ID`: Railway project ID
- `RAILWAY_PRODUCER_SERVICE_ID`: Producer service ID
- `RAILWAY_CONSUMER_SERVICE_ID`: Consumer service ID

### 3. Local Development

#### Start Producer
```bash
cd services/producer
source ../../producer-env/bin/activate
pip install -r requirements.txt
python src/main.py
```

#### Start Consumer
```bash
cd services/consumer
source ../../consumer-env/bin/activate
pip install -r requirements.txt
python src/main.py
```

### 4. Railway Deployment

#### Deploy Producer
```bash
cd services/producer
railway login
railway up
```

#### Deploy Consumer
```bash
cd services/consumer
railway up
```

## 📈 Analytics Examples

### Real-time Metrics
- Event counts by type and device
- Revenue by product category
- User session analytics
- Conversion funnel metrics

### Advanced Analytics
- User behavior patterns
- Product performance ranking
- Traffic anomaly detection
- Cohort analysis

## 🔍 Code Structure

```
services/
├── producer/
│   ├── src/
│   │   ├── config.py          # Configuration management
│   │   ├── models.py          # Data models and validation
│   │   ├── transformer.py     # Data transformation logic
│   │   ├── producer.py        # Main producer service
│   │   └── main.py           # Entry point
│   └── requirements.txt      # Dependencies
│
├── consumer/
│   ├── src/
│   │   ├── config.py          # Configuration management
│   │   ├── models.py          # Analytics models
│   │   ├── flink_processor.py # Flink stream processing
│   │   ├── clickhouse_manager.py # ClickHouse integration
│   │   ├── consumer.py        # Main consumer service
│   │   └── main.py           # Entry point
│   └── requirements.txt      # Dependencies
│
└── infrastructure/
    └── railway/
        ├── producer/Dockerfile
        └── consumer/Dockerfile
```

## 🧪 Testing

```bash
# Run producer tests
cd services/producer
python -m pytest tests/

# Run consumer tests
cd services/consumer
python -m pytest tests/

# Run integration tests
./scripts/test_integration.sh
```

## 📚 Key Learning Concepts

### 1. Stream Processing with Flink
- **Event Time vs Processing Time**
- **Watermarks and Late Data**
- **Window Operations**
- **State Management**

### 2. Data Transformation
- **Schema Evolution**
- **Data Validation**
- **Enrichment Strategies**
- **Error Handling**

### 3. Storage Optimization
- **ClickHouse Schema Design**
- **Partitioning Strategies**
- **Materialized Views**
- **Query Optimization**

### 4. Production Patterns
- **Monitoring and Observability**
- **Error Recovery**
- **Graceful Degradation**
- **Resource Management**

## 🚨 Monitoring

### Health Checks
Both services provide health endpoints:
- Producer: `http://localhost:8080/health`
- Consumer: `http://localhost:8081/health`

### Metrics
- **Throughput**: Events processed per second
- **Latency**: Processing time distribution
- **Error Rate**: Failed operations percentage
- **Resource Usage**: CPU, memory, disk

### Logging
Structured logging with:
- Request/Response tracing
- Error context
- Performance metrics
- Business events

## 🔧 Configuration

### Environment Variables

**Producer:**
- `PRODUCER_KAFKA_BOOTSTRAP_SERVERS`
- `PRODUCER_KAFKA_TOPIC`
- `PRODUCER_API_URL`
- `PRODUCER_LOG_LEVEL`

**Consumer:**
- `CONSUMER_KAFKA_BOOTSTRAP_SERVERS`
- `CONSUMER_FLINK_PARALLELISM`
- `CONSUMER_CLICKHOUSE_HOST`
- `CONSUMER_LOG_LEVEL`

## 📝 Learning Exercises

### 1. Modify Transformations
Change the `transform_event` method in `services/producer/src/transformer.py` to add new fields or business logic.

### 2. Add New Analytics
Update `services/consumer/src/flink_processor.py` to add new aggregation windows or metrics.

### 3. Experiment with Windowing
Try different window sizes and strategies in the Flink processor.

### 4. Add Anomaly Detection
Implement new anomaly detection algorithms in the ClickHouse manager.

### 5. Optimize Queries
Experiment with ClickHouse query optimizations and indexing strategies.

## 🛠️ Development Workflow

1. **Local Development**: Test changes locally
2. **Configuration**: Update environment variables
3. **Testing**: Run unit and integration tests
4. **Deployment**: Deploy to Railway
5. **Monitoring**: Check metrics and logs

## 📚 Additional Resources

- [Apache Flink Documentation](https://flink.apache.org/)
- [ClickHouse Documentation](https://clickhouse.com/)
- [Confluent Cloud Documentation](https://docs.confluent.io/)
- [Railway Documentation](https://docs.railway.app/)

## 🤝 Contributing

This project is designed for learning. Feel free to:
1. Fork the repository
2. Create a feature branch
3. Add new analytics or transformations
4. Submit pull requests

## 📄 License

MIT License - see LICENSE file for details.