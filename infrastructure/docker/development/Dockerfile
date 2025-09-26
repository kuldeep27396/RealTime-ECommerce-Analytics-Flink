# Development Dockerfile with UV and hot-reload
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-17-jdk \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install UV package manager
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy pyproject.toml and uv.lock
COPY pyproject.toml uv.lock ./

# Install all dependencies including dev tools
RUN uv sync --all-extras --dev

# Copy application code
COPY . .

# Set environment variables for development
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=DEBUG
ENV FLINK_HOME=/opt/flink

# Download Flink dependencies for development
RUN mkdir -p /opt/flink/jars && \
    cd /opt/flink/jars && \
    curl -o flink-sql-connector-kafka-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar && \
    curl -o kafka-clients-3.4.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -o flink-connector-base-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.17.0/flink-connector-base-1.17.0.jar

# Install pre-commit hooks
RUN uv run pre-commit install

# Create non-root user
RUN useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app
USER app

# Expose ports
EXPOSE 8080 8081

# Default command
CMD ["uv", "run", "python", "-m", "pytest"]