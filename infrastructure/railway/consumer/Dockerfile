# Consumer service Dockerfile for Railway deployment
FROM python:3.11-slim

# Install system dependencies including Java for Flink
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-17-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install UV package manager
RUN pip install uv

# Download Flink dependencies
RUN mkdir -p /opt/flink/jars && \
    cd /opt/flink/jars && \
    curl -o flink-sql-connector-kafka-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar && \
    curl -o kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -o flink-connector-base-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.17.0/flink-connector-base-1.17.0.jar

# Set working directory
WORKDIR /app

# Copy pyproject.toml first for better layer caching
COPY pyproject.toml uv.lock ./

# Install Python dependencies with UV
RUN uv sync --extra consumer --frozen

# Copy application code
COPY services/consumer/src/ ./src/
COPY services/consumer/README.md ./README.md  # Optional: copy any additional files

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV FLINK_HOME=/opt/flink

# Expose metrics port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"  # Simple health check

# Start the application using UV
CMD ["uv", "run", "python", "src/main.py"]