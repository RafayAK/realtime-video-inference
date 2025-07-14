#!/bin/bash

##################################################################
# Generates gRPC Python code from proto files
# and places it in the inference_service directory
##################################################################
echo "Generating gRPC Python code..."

cd inference_service
python -m grpc_tools.protoc \
    -I ../protos \
    --python_out=. \
    --grpc_python_out=. \
    --proto_path=. \
    inference.proto

echo "Generated inference_pb2.py and inference_pb2_grpc.py"
echo "gRPC code generation complete!"
cd ..
