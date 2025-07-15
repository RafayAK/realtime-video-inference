.PHONY: help setup up down logs restart clean build producer consumer health test

# Default target
help:
	@echo "Enhanced Pipeline - Available Commands:"
	@echo "  setup                 - Set up the project (install dependencies, create .env)"
	@echo "  up                    - Start all services with docker-compose"
	@echo "  down                  - Stop all services"
	@echo "  build                 - Build the enhanced pipeline Docker image"
	@echo "  logs                  - Show logs from all services"
	@echo "  logs-app              - Show logs from the enhanced pipeline FastAPI app only"
	@echo "  logs-image-service    - Show logs from the consumer image service only"
	@echo "  restart               - Restart all services"
	@echo "  producer              - Run producer (requires services to be up)"
	@echo "  consumer              - Run image service consumer (requires services to be up)"
	@echo "  health                - Check health of all services"
	@echo "  clean                 - Clean up containers, volumes, and generated files"
	@echo "  test                  - Run basic tests"

# Setup project
setup:
	@echo "Setting up enhanced pipeline..."
	@if [ ! -f .env ]; then \
		echo "Creating .env file from template..."; \
		cp .env.example .env; \
		echo "Please edit .env file if needed"; \
	fi
	@mkdir -p static/images
	@echo "Enhanced pipeline setup complete!"

	@echo "Setup ResNet18 model for Triton Inference Server..."
	python setup_model.py

# Start all services
up:
	@echo "Starting enhanced pipeline services..."
	docker compose up -d --build
	@echo "Services started! Web UI available at http://localhost:8000"
	@echo "Redpanda Console available at http://localhost:8080"

# Stop all services
down:
	@echo "Stopping enhanced pipeline services..."
	docker compose down

# Build the enhanced pipeline image
build:
	@echo "Building enhanced pipeline Docker image..."
	docker compose build enhanced-pipeline

# Show logs
logs:
	docker compose logs -f

# Show logs from enhanced pipeline app only
logs-app:
	docker compose logs -f enhanced-pipeline

# Show logs from image processing service only
logs-image-service:
	docker compose logs -f image-service

# Restart services
restart: down up

# Run image processing service (consumer)
consumer:
	@echo "Starting image processing service..."
	python streaming-simulator/image-service.py

# Check health of services
health:
	@echo "Checking service health..."
	@curl -s http://localhost:8000/health | python -m json.tool || echo "Enhanced pipeline not responding"
	@curl -s http://localhost:8080/api/cluster || echo "Redpanda Console not responding"

# Clean up everything
clean:
	@echo "Cleaning up enhanced pipeline..."
	docker compose down -v --remove-orphans
	rm -rf static/images/*.jpg static/images/*.png
	@echo "Cleanup complete!"

# Run basic tests
test:
	@echo "Running basic tests..."
	@python -c "import requests; r=requests.get('http://localhost:8000/health'); print('✓ Health check passed' if r.status_code==200 else '✗ Health check failed')" || echo "✗ Cannot connect to enhanced pipeline"
