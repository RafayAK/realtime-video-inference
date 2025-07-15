# Realtime Video Inference

A real-time video inference system with two implementations: a core pipeline following strict assignment requirements and an enhanced pipeline with web interface and Triton integration.

## (Brief) Project Structure

```
├── core_pipeline/          # Assignment-compliant implementation
│   ├── inference_service/  # gRPC server with PyTorch
│   ├── streaming_simulator/ # Kafka producer/consumer
│   ├── makefile            # Convenience commands
│   └── requirements.txt    #  dependencies created with uv
├── enhanced_pipeline/      # Full-featured implementation  
│   ├── api/               # FastAPI application
│   ├── static/            # Web interface
|   ├── makefile            # Convenience commands
│   ├── streaming-simulator/ # Triton integration
│   └── requirements.txt   # dependencies created with uv
└── uv.lock                # UV package manager lock file
└── pyproject.toml         # UV package manager configuration
```

## Quick Setup

### Using UV (Recommended)
```bash
# Install UV package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Activate environment
source .venv/bin/activate
```

### Using Pip (Alternative)
Each pipeline has its own `requirements.txt`:
```bash
# For core pipeline
cd core_pipeline
pip install -r requirements.txt

# For enhanced pipeline  
cd enhanced_pipeline
pip install -r requirements.txt
```

## Pipelines

### Core Pipeline
- **Purpose**: Assignment requirements compliance
- **Tech**: gRPC, PyTorch, Kafka, Docker
- See [core_pipeline/README.md](core_pipeline/README.md) for details.

### Enhanced Pipeline  
- **Purpose**: Live-streaming video inference with web interface and Triton integration
- **Tech**: FastAPI, Triton (gRPC client), WebSocket, Docker
- See [enhanced_pipeline/README.md](enhanced_pipeline/README.md) for details.

## Getting Started

Choose your pipeline and follow its README for detailed setup instructions:

- **Core Pipeline**: `cd core_pipeline && make up`
- **Enhanced Pipeline**: `cd enhanced_pipeline && make up`
