.PHONY: help install test lint format build run-producer run-consumer docker-up docker-down clean

# Default target
help:
	@echo "Available commands:"
	@echo "  install     Install dependencies with UV"
	@echo "  test        Run tests"
	@echo "  lint        Run linting (black, isort, flake8, mypy)"
	@echo "  format      Format code with black and isort"
	@echo "  build       Build the project"
	@echo "  run-producer Run the producer service"
	@echo "  run-consumer Run the consumer service"
	@echo "  docker-up   Start all services with Docker Compose"
	@echo "  docker-down Stop all Docker services"
	@echo "  clean       Clean up build artifacts"

# Install dependencies
install:
	uv sync --all-extras --dev

# Run tests
test:
	uv run pytest tests/ --cov=services --cov-report=term-missing --cov-report=html

# Run linting
lint:
	uv run black --check services/
	uv run isort --check-only services/
	uv run flake8 services/
	uv run mypy services/

# Format code
format:
	uv run black services/
	uv run isort services/

# Build the project
build:
	uv build

# Run producer service
run-producer:
	uv run python services/producer/src/main.py

# Run consumer service
run-consumer:
	uv run python services/consumer/src/main.py

# Start Docker Compose
docker-up:
	docker-compose up -d

# Stop Docker Compose
docker-down:
	docker-compose down

# Clean up
clean:
	rm -rf .venv build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# Initialize development environment
init: install
	@echo "Development environment initialized!"
	@echo "Don't forget to copy .env.template to .env and fill in your secrets"

# Generate database migrations (if needed)
migrate:
	uv run python scripts/migrate.py

# Run pre-commit hooks
pre-commit:
	uv run pre-commit run --all-files

# Start development environment
dev: docker-up
	@echo "Services started:"
	@echo "  Producer: http://localhost:8080"
	@echo "  Consumer: http://localhost:8081"
	@echo "  ClickHouse: http://localhost:8123"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"

# View logs
logs:
	docker-compose logs -f

# View logs for specific service
logs-%:
	docker-compose logs -f $*

# Run health checks
health:
	@curl -s http://localhost:8080/health | jq . || echo "Producer not healthy"
	@curl -s http://localhost:8081/health | jq . || echo "Consumer not healthy"
	@curl -s http://localhost:8123/ping || echo "ClickHouse not healthy"

# Generate documentation
docs:
	@echo "Generating documentation..."
	@echo "Documentation available in docs/ directory"

# Update dependencies
update-deps:
	uv sync --upgrade

# Lock dependencies
lock:
	uv sync --all-extras --frozen