# UV + Docker Setup Guide

This project has been modernized to use **UV** (the fast Python package manager) and **Docker** for efficient development and deployment.

## 🚀 What's New

### ✅ **UV Package Manager**
- **Fast dependency resolution** (10-100x faster than pip)
- **Lock files for reproducible builds**
- **Service-specific dependencies** (producer vs consumer)
- **Dev tools integration** (black, isort, flake8, mypy, pytest)
- **Virtual environment management**

### ✅ **Modern Docker Configuration**
- **Multi-stage builds** for optimized images
- **UV integration** for fast dependency installation
- **Health checks** and monitoring
- **Service isolation** with separate Dockerfiles
- **Development** and **production** configurations

### ✅ **Docker Compose for Local Development**
- **Complete local stack**: Producer + Consumer + Kafka + ClickHouse + Grafana
- **One-command setup**: `make docker-up`
- **Integrated monitoring** and dashboards
- **Development environment** that matches production

## 🛠️ Quick Start

### 1. **Install UV**
```bash
# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.sh | iex"
```

### 2. **Install Dependencies**
```bash
# Install all dependencies with dev tools
make install

# Or use UV directly
uv sync --all-extras --dev
```

### 3. **Set Up Environment**
```bash
# Copy environment template
cp .env.template .env

# Edit .env with your actual values
# (Confluent Cloud, ClickHouse, Railway secrets)
```

### 4. **Local Development**
```bash
# Start all services with Docker Compose
make docker-up

# Or run individual services
make run-producer  # Runs producer locally
make run-consumer  # Runs consumer locally
```

### 5. **Development Workflow**
```bash
# Format code
make format

# Run linting
make lint

# Run tests
make test

# Build for deployment
make build
```

## 📁 Project Structure

```
RealTime-ECommerce-Analytics-Flink/
├── pyproject.toml              # UV configuration
├── uv.lock                     # Dependency lock file
├── railway.toml               # Railway deployment config
├── Makefile                    # Development commands
├── docker-compose.yml         # Local development stack
├── .env.template              # Environment variables
├── services/                   # Service source code
│   ├── producer/              # Producer service
│   └── consumer/              # Consumer service
├── infrastructure/             # Infrastructure config
│   ├── docker/development/    # Development Dockerfiles
│   ├── railway/              # Railway Dockerfiles
│   ├── clickhouse/           # ClickHouse setup
│   └── grafana/              # Grafana dashboards
└── .github/workflows/         # GitHub Actions (UV-enabled)
```

## 🔧 Key Features

### **UV Benefits**
- **Lightning fast** dependency resolution
- **Deterministic builds** with lock files
- **Service-specific** dependency management
- **Dev tools** included by default
- **Virtual environment** automation

### **Docker Benefits**
- **Reproducible** builds across environments
- **Isolated** services with separate containers
- **Production-ready** deployment configuration
- **Development** parity with production
- **Monitoring** and health checks built-in

### **Development Tools**
- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing with coverage
- **pre-commit**: Git hooks

## 🚀 Deployment

### **Railway Deployment**
```bash
# Push to main branch - automatic deployment via GitHub Actions
git push origin main

# Or deploy manually
railway up
```

### **GitHub Actions**
- **Automated testing** with UV
- **Code quality** checks (linting, formatting, type checking)
- **Railway deployment** with proper service configuration
- **Status reporting** and notifications

## 🐳 Docker Services

### **Local Stack**
- **Producer**: Clickstream data ingestion
- **Consumer**: Flink processing + ClickHouse storage
- **Kafka**: Message broker (Confluent Cloud compatible)
- **ClickHouse**: Analytics database
- **Grafana**: Monitoring dashboards
- **Redis**: Caching and session management

### **Service Ports**
- **Producer**: `http://localhost:8080`
- **Consumer**: `http://localhost:8081`
- **ClickHouse**: `http://localhost:8123`
- **Grafana**: `http://localhost:3000` (admin/admin)
- **Kafka**: `localhost:9092`

## 📊 Monitoring

### **Health Checks**
```bash
# Check service health
make health

# View logs
make logs

# View specific service logs
make logs-producer
make logs-consumer
```

### **Grafana Dashboards**
- **Real-time metrics** from ClickHouse
- **Service performance** monitoring
- **Error rates** and alerts
- **Business analytics** dashboards

## 🔧 Configuration

### **Environment Variables**
- **Confluent Cloud**: Kafka connection and credentials
- **ClickHouse**: Database connection and credentials
- **Railway**: Deployment configuration
- **Service settings**: Logging, metrics, performance

### **Service-Specific Dependencies**
```toml
# Producer dependencies
uv sync --extra producer

# Consumer dependencies
uv sync --extra consumer

# Dev tools
uv sync --all-extras --dev
```

## 💡 Best Practices

### **Development**
1. **Use UV** for all dependency management
2. **Run `make lint`** before committing
3. **Use `make format`** for consistent code style
4. **Test locally** with `make test`
5. **Use Docker Compose** for full-stack testing

### **Deployment**
1. **Lock dependencies** with `uv sync --frozen`
2. **Test in Docker** before deploying
3. **Monitor service health** in Railway dashboard
4. **Check Grafana** for analytics
5. **Use GitHub Actions** for automated deployments

## 🎯 Next Steps

1. **Install UV** and set up the development environment
2. **Configure `.env`** with your actual secrets
3. **Run `make docker-up`** to start the local stack
4. **Test the services** and monitor in Grafana
5. **Push to GitHub** for automated Railway deployment

## 📚 Additional Resources

- [UV Documentation](https://docs.astral.sh/uv/)
- [Docker Documentation](https://docs.docker.com/)
- [Railway Documentation](https://docs.railway.app/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Grafana Documentation](https://grafana.com/docs/)

This modern setup provides a **fast**, **reproducible**, and **production-ready** development and deployment experience! 🚀