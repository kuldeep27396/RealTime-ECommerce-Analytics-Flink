# Producer service Dockerfile for Railway deployment
FROM python:3.11-slim

# Install system dependencies including UV
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install UV package manager
RUN pip install uv

# Set working directory
WORKDIR /app

# Copy pyproject.toml first for better layer caching
COPY pyproject.toml uv.lock ./

# Install Python dependencies with UV
RUN uv sync --extra producer --frozen

# Copy application code
COPY services/producer/src/ ./src/
COPY services/producer/README.md ./README.md  # Optional: copy any additional files

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO

# Expose metrics port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"  # Simple health check

# Start the application using UV
CMD ["uv", "run", "python", "src/main.py"]