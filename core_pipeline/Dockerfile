# Multi-stage Dockerfile for Core Pipeline

# Stage 1: Build stage with all build dependencies
FROM python:3.11-slim AS builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libhdf5-dev \
    libopencv-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Copy source code
COPY . .

# Stage 2: Runtime stage with minimal dependencies
FROM python:3.11-slim as runtime

# Install only runtime system dependencies
RUN apt-get update && apt-get install -y \
    libopencv-dev \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash app

# Set working directory
WORKDIR /app

# Copy Python packages from builder stage
COPY --from=builder /root/.local /home/app/.local

# Copy application code
COPY --from=builder /build /app

# Change ownership to app user
RUN chown -R app:app /app

# Switch to non-root user
USER app

# Add user's local bin to PATH
ENV PATH=/home/app/.local/bin:$PATH

# Health check for inference service
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import grpc; import inference_service.inference_pb2_grpc as pb2_grpc; import inference_service.inference_pb2 as pb2; \
    channel = grpc.insecure_channel('localhost:50051'); \
    stub = pb2_grpc.InferenceServiceStub(channel); \
    response = stub.HealthCheck(pb2.HealthRequest(), timeout=5); \
    exit(0 if response.healthy else 1)" || exit 1

# Default command (can be overridden)
CMD ["python", "inference_service/server.py"]
