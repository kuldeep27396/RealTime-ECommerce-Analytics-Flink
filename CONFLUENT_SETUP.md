# Confluent Cloud Integration Guide

This guide will help you set up an end-to-end pipeline to stream e-commerce clickstream data to Confluent Cloud and process it with Apache Flink.

## Prerequisites

- Python 3.8+
- Confluent Cloud account (free tier available)
- Apache PyFlink installed

## Setup Steps

### 1. Confluent Cloud Setup

1. **Create Account**: Go to [confluent.cloud](https://confluent.cloud) and sign up for a free account
2. **Create Cluster**:
   - Click "Create cluster"
   - Select "Basic" tier
   - Choose cloud provider and region
   - Click "Create cluster"
3. **Create Topic**:
   - Go to your cluster → Topics
   - Click "Create topic"
   - Name: `ecommerce_clickstream`
   - Partitions: 6
   - Click "Create"
4. **Get API Keys**:
   - Go to API keys section
   - Click "Create key"
   - Save the API key and secret (shown only once!)

### 2. Get Connection Details

1. **Bootstrap Server**:
   - Go to your cluster → Cluster settings → Connection details
   - Copy the Bootstrap server (e.g., `pkc-xxxxx.eastus.azure.confluent.cloud:9092`)

2. **API Key and Secret**:
   - From the API keys section you created earlier
   - Copy both the key and secret

### 3. Environment Setup

```bash
# Create virtual environment
python3 -m venv confluent-env
source confluent-env/bin/activate

# Install dependencies
pip install kafka-python requests pyflink
```

### 4. Download Required JARs

```bash
# Run the setup script
./setup_jars.sh
```

### 5. Configuration

Update the configuration files with your Confluent Cloud credentials:

**File**: `config/confluent_config.py`
```python
BOOTSTRAP_SERVERS = "your-bootstrap-server.cloud:9092"
API_KEY = "your-api-key"
API_SECRET = "your-api-secret"
```

**File**: `python/kafka/confluent_producer.py`
```python
CONFLUENT_BOOTSTRAP_SERVERS = 'your-bootstrap-server.cloud:9092'
CONFLUENT_API_KEY = 'your-api-key'
CONFLUENT_API_SECRET = 'your-api-secret'
```

**File**: `python/streaming-flink/flink-confluent.py`
```python
CONFLUENT_BOOTSTRAP_SERVERS = 'your-bootstrap-server.cloud:9092'
CONFLUENT_API_KEY = 'your-api-key'
CONFLUENT_API_SECRET = 'your-api-secret'
```

### 6. Run the Pipeline

#### Start the Producer (Pushes data to Confluent Cloud)

```bash
source confluent-env/bin/activate
python python/kafka/confluent_producer.py
```

#### Start the Flink Consumer (Reads from Confluent Cloud)

```bash
source confluent-env/bin/activate
python python/streaming-flink/flink-confluent.py
```

## Architecture

```
[API] → [Python Producer] → [Confluent Cloud Kafka] → [PyFlink Consumer] → [Console/Analytics]
```

## Data Flow

1. **Data Source**: Real-time clickstream API
2. **Producer**: Python script that fetches data from API and publishes to Confluent Cloud
3. **Kafka Topic**: `ecommerce_clickstream` in Confluent Cloud
4. **Consumer**: PyFlink application that processes the streaming data
5. **Output**: Real-time metrics printed to console

## Metrics Calculated

- Event counts by type and source (1-minute windows)
- Revenue by product category (1-minute windows)
- Unique visitors (1-minute windows)
- Real-time event statistics (5-second windows)

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify bootstrap server, API key, and secret
   - Check network connectivity
   - Ensure SASL_SSL configuration is correct

2. **Authentication Errors**:
   - Verify API key and secret are correct
   - Check if API key has proper permissions

3. **JAR Issues**:
   - Run `./setup_jars.sh` to download required JARs
   - Verify JARs are in the correct directory

4. **Flink Issues**:
   - Ensure all required JARs are in the classpath
   - Check PyFlink installation

### Testing Connection

Test your Confluent Cloud connection with this simple script:

```python
from kafka import KafkaProducer
import json

# Test connection
try:
    producer = KafkaProducer(
        bootstrap_servers='your-bootstrap-server.cloud:9092',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='your-api-key',
        sasl_plain_password='your-api-secret',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✓ Connection successful!")
    producer.close()
except Exception as e:
    print(f"✗ Connection failed: {e}")
```

## Monitoring

### Confluent Cloud Dashboard

1. Monitor your cluster health
2. Check topic throughput
3. View consumer lag
4. Monitor API usage

### Flink Job Monitoring

1. Metrics are printed to console in real-time
2. Check for processing delays
3. Monitor event throughput

## Cost Management

- Free tier includes $400 credits (30 days)
- Monitor your usage in the Confluent Cloud dashboard
- Basic tier has limits on throughput and storage
- Consider scaling down partitions if not needed

## Next Steps

1. **Add Data Sink**: Connect to databases, data lakes, or analytics platforms
2. **Add Processing Logic**: Implement more complex analytics and ML models
3. **Add Monitoring**: Set up alerts and dashboards
4. **Scale Up**: Increase partitions and consumer instances for higher throughput

## Support

- Confluent Cloud Documentation: https://docs.confluent.io/cloud/current/
- PyFlink Documentation: https://nightlies.apache.org/flink/flink-docs-release-1.15/
- Kafka Python Documentation: https://kafka-python.readthedocs.io/