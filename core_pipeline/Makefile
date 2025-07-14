# Core Pipeline Makefile
# Simple commands for managing the real-time video inference pipeline

.PHONY: help setup up down logs producer clean status test

# Default target
help:
	@echo "Core Pipeline - Available Commands:"
	@echo ""
	@echo "  make setup           - Install dependencies and generate gRPC code"
	@echo "  make up              - Start all services with Docker Compose"
	@echo "  make down            - Stop all services"
	@echo "  make logs            - View logs from all services"
	@echo "  make logs-consumer   - View logs from the consumer service"
	@echo "  make logs-producer   - View logs from the producer service"
	@echo "  make logs-inference  - View logs from the inference service"
	@echo "  make producer        - Run webcam producer for 30 seconds"
	@echo "  make status          - Check service status"
	@echo "  make test            - Run end-to-end test"
	@echo "  make clean           - Clean up containers and logs"
	@echo ""

# Setup dependencies and generate gRPC code
setup:
	@echo "📦 Setting up core pipeline..."
	@chmod +x generate_grpc.sh
	@pip install -r requirements.txt
	@bash generate_grpc.sh
	@echo "✅ Setup complete!"

# Start all services
up:
	@echo "🚀 Starting all services..."
	@docker compose up -d --build
	@echo "✅ Services started. Use 'make logs' to view output."

# Stop all services
down:
	@echo "🛑 Stopping all services..."
	@docker compose down

# View logs from all services
logs:
	@echo "📋 Viewing logs (Press Ctrl+C to exit)..."
	@docker compose logs -f

# View logs from specific service
logs-consumer:
	@docker compose logs -f consumer

logs-producer:
	@docker compose logs -f producer

logs-inference:
	@docker compose logs -f inference_service

# Run webcam producer
producer:
	@echo "📹 Starting webcam producer for 30 seconds with 10fps..."
	@python streaming_simulator/producer.py --mode webcam --duration 30 --kafka-servers localhost:19092

# Run video file producer (requires VIDEO_PATH variable)
producer-video:
	@echo "🎥 Starting video producer..."
	@if [ -z "$(VIDEO_PATH)" ]; then \
		echo "❌ Please specify VIDEO_PATH. Example: make producer-video VIDEO_PATH=/path/to/video.mp4"; \
	else \
		python streaming_simulator/producer.py --kafka-servers localhost:19092 --mode video --video-path $(VIDEO_PATH); \
	fi

# Check service status
status:
	@echo "📊 Service Status:"
	@docker compose ps

# Run end-to-end test
test:
	@echo "🧪 Running end-to-end test..."
	@echo "Starting services..."
	@make up
	@echo "Waiting 30 seconds for services to be ready..."
	@sleep 30
	@echo "Running producer test..."
	@python streaming_simulator/producer.py --mode webcam --duration 10 --fps 5
	@echo "✅ Test complete. Check logs with 'make logs'"

# Clean up everything
clean:
	@echo "🧹 Cleaning up..."
	@docker compose down -v
	@truncate -s 0 *.log # Clear all log files
	@echo "✅ Cleanup complete."

# Quick restart
restart: down up

# Development helpers
dev-setup:
	@echo "🔧 Setting up development environment..."
	@make setup
	@make up

# Show running containers
ps:
	@docker-compose ps
