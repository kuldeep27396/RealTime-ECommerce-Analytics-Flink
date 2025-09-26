# Deployment Guide

## 🚀 Railway Deployment

This guide covers deploying the real-time analytics pipeline to Railway.

## Prerequisites

1. **Railway Account**: Sign up at [railway.app](https://railway.app)
2. **GitHub Repository**: Your project code in GitHub
3. **Confluent Cloud**: Active Confluent Cloud account
4. **ClickHouse**: Running ClickHouse instance

## 🔧 Setup Railway Project

### 1. Create Railway Project
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login to Railway
railway login

# Initialize project
railway init
```

### 2. Create Services

#### Producer Service
```bash
# Create producer service
railway add --service producer

# Set build configuration
cat > railway.toml << EOF
[build]
command = "pip install -r requirements.txt"

[deploy]
startCommand = "python src/main.py"

[deploy.env]
PYTHONPATH = "/app"
PYTHONUNBUFFERED = "1"
EOF
```

#### Consumer Service
```bash
# Create consumer service in separate directory
mkdir consumer-service && cd consumer-service
railway add --service consumer

# Set build configuration
cat > railway.toml << EOF
[build]
command = "pip install -r requirements.txt"

[deploy]
startCommand = "python src/main.py"

[deploy.env]
PYTHONPATH = "/app"
PYTHONUNBUFFERED = "1"
EOF
```

## 🔐 Configure Secrets

### GitHub Secrets

Add these secrets to your GitHub repository settings:

**Kafka Configuration:**
```bash
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.confluent.cloud:9092
KAFKA_TOPIC=clickstream_ecommerce_kuldeep_pal
KAFKA_API_KEY=your-api-key
KAFKA_API_SECRET=your-api-secret
KAFKA_GROUP_ID=flink-consumer-group
```

**ClickHouse Configuration:**
```bash
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=ecommerce
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
```

**Railway Configuration:**
```bash
RAILWAY_TOKEN=your-railway-token
RAILWAY_PROJECT_ID=your-project-id
RAILWAY_PRODUCER_SERVICE_ID=producer-service-id
RAILWAY_CONSUMER_SERVICE_ID=consumer-service-id
```

### Railway Environment Variables

#### Producer Service Variables
```bash
railway service variables set \
  PRODUCER_KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  PRODUCER_KAFKA_TOPIC="${KAFKA_TOPIC}" \
  PRODUCER_KAFKA_API_KEY="${KAFKA_API_KEY}" \
  PRODUCER_KAFKA_API_SECRET="${KAFKA_API_SECRET}" \
  PRODUCER_API_URL="https://clickstream-datagenerator-production.up.railway.app/stream/interactions" \
  PRODUCER_API_RATE=10000 \
  PRODUCER_API_DURATION=60 \
  PRODUCER_LOG_LEVEL=INFO \
  PRODUCER_METRICS_PORT=8080
```

#### Consumer Service Variables
```bash
railway service variables set \
  CONSUMER_KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  CONSUMER_KAFKA_TOPIC="${KAFKA_TOPIC}" \
  CONSUMER_KAFKA_GROUP_ID="${KAFKA_GROUP_ID}" \
  CONSUMER_KAFKA_API_KEY="${KAFKA_API_KEY}" \
  CONSUMER_KAFKA_API_SECRET="${KAFKA_API_SECRET}" \
  CONSUMER_CLICKHOUSE_HOST="${CLICKHOUSE_HOST}" \
  CONSUMER_CLICKHOUSE_PORT="${CLICKHOUSE_PORT}" \
  CONSUMER_CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE}" \
  CONSUMER_CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
  CONSUMER_CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
  CONSUMER_FLINK_PARALLELISM=2 \
  CONSUMER_LOG_LEVEL=INFO \
  CONSUMER_METRICS_PORT=8081
```

## 🐳 Docker Configuration

### Producer Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "src/main.py"]
```

### Consumer Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including Java for Flink
RUN apt-get update && apt-get install -y \
    gcc g++ openjdk-17-jdk curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Download Flink JARs
RUN mkdir -p /opt/flink/jars && \
    cd /opt/flink/jars && \
    curl -o flink-sql-connector-kafka-1.17.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar && \
    curl -o kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV FLINK_HOME=/opt/flink

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

CMD ["python", "src/main.py"]
```

## 📦 Deployment Process

### 1. Deploy Producer Service
```bash
cd services/producer
railway up
```

### 2. Deploy Consumer Service
```bash
cd services/consumer
railway up
```

### 3. Verify Deployment
```bash
# Check service status
railway status

# View logs
railway logs

# Open service URLs
railway open
```

## 📊 Monitoring and Observability

### Health Endpoints

Both services provide health endpoints:

- **Producer**: `http://producer-service-url/health`
- **Consumer**: `http://consumer-service-url/health`

### Railway Dashboard

1. **Metrics**: CPU, memory, disk usage
2. **Logs**: Real-time log streaming
3. **Deployments**: Deployment history and status
4. **Environment**: Environment variables management

### Custom Metrics

Add custom metrics to your services:

```python
# In producer.py or consumer.py
import time
import psutil

def get_system_metrics():
    return {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_percent': psutil.disk_usage('/').percent,
        'timestamp': time.time()
    }
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Out of Memory
**Symptoms**: Service crashes with OOM error
**Solution**:
```bash
# Increase memory limit in Railway
railway service variables set RAILWAY_MEMORY="512"
```

#### 2. Connection Errors
**Symptoms**: Cannot connect to Kafka or ClickHouse
**Solution**: Verify environment variables and network connectivity

#### 3. High CPU Usage
**Symptoms**: Service becomes unresponsive
**Solution**: Scale service or optimize processing logic

### Debug Commands

```bash
# View service logs
railway logs --service producer

# Monitor resource usage
railway status

# Restart service
railway service restart --service producer

# Check environment variables
railway service variables list
```

## 🔄 Scaling

### Horizontal Scaling

```bash
# Scale producer to 2 instances
railway service scale --service producer --instances 2

# Scale consumer to 3 instances
railway service scale --service consumer --instances 3
```

### Vertical Scaling

```bash
# Increase memory allocation
railway service variables set RAILWAY_MEMORY="1024"

# Increase CPU allocation
railway service variables set RAILWAY_CPU="1"
```

## 🔄 CI/CD Pipeline

### GitHub Actions

The repository includes GitHub Actions workflows:

1. **`.github/workflows/deploy.yml`**: Automated deployment on push to main
2. **`.github/workflows/secrets-sync.yml`**: Sync secrets to Railway
3. **`.github/workflows/validate.yml`**: Validate configuration

### Manual Deployment

```bash
# Deploy specific branch
railway up --branch feature/new-analytics

# Deploy with specific commit
railway up --commit abc123
```

## 💰 Cost Optimization

### Railway Cost Tips

1. **Use Sleep Mode**: Stop services when not in use
2. **Right-size Resources**: Don't over-provision memory/CPU
3. **Monitor Usage**: Use Railway dashboard to track usage
4. **Optimize Code**: Reduce resource consumption

### Monitoring Costs

```bash
# View current usage and costs
railway billing

# Set spending alerts
railway billing alerts
```

## 🌐 Production Considerations

### Security

1. **Secrets Management**: Use Railway environment variables
2. **Network Security**: Configure firewall rules
3. **Data Encryption**: Enable TLS for all connections
4. **Access Control**: Use Railway RBAC features

### Performance

1. **Caching**: Add Redis for frequently accessed data
2. **Connection Pooling**: Reuse database connections
3. **Batch Processing**: Process data in batches
4. **Async Operations**: Use async I/O where possible

### Reliability

1. **Health Checks**: Implement comprehensive health checks
2. **Graceful Shutdown**: Handle SIGTERM properly
3. **Retry Logic**: Implement exponential backoff
4. **Circuit Breakers**: Prevent cascade failures

## 📚 Additional Resources

- [Railway Documentation](https://docs.railway.app/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Confluent Cloud Best Practices](https://docs.confluent.io/cloud/current/)
- [ClickHouse Performance Guide](https://clickhouse.com/docs/en/guides/developer/performance/)