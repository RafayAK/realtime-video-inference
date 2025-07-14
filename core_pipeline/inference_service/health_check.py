#!/usr/bin/env python3
import sys
print(sys.path)
import grpc
import inference_pb2_grpc as pb2_grpc
import inference_pb2 as pb2

def check_health():
    try:
        channel = grpc.insecure_channel('localhost:50051')
        stub = pb2_grpc.InferenceServiceStub(channel)
        response = stub.HealthCheck(pb2.HealthRequest(), timeout=5)
        if response.healthy:
            print("Health check passed")
            return 0
        else:
            print("Health check failed")
            return 1
    except Exception as e:
        print(f"Health check error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(check_health())