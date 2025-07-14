import grpc
from concurrent import futures
import time
from loguru import logger
import cv2
import numpy as np
import torch
import torchvision.transforms as transforms
from torchvision import models
from PIL import Image
import io

# Import generated gRPC classes
import inference_pb2
import inference_pb2_grpc

# Configure logging
logger.add("inference_service.log", rotation="1MB", level="DEBUG")

class InferenceServicer(inference_pb2_grpc.InferenceServiceServicer):
    def __init__(self):
        self.model = None
        self.transform = None
        self.class_names = None
        self._load_model()
        
    def _load_model(self):
        """Load and initialize the PyTorch model"""
        logger.info("Loading ResNet18 model...")
        
        # Load pretrained ResNet18
        model = models.resnet18(pretrained=True)
        model.eval()
        
        # Set device
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(self.device)
        logger.info(f"Model loaded on device: {self.device}")

        # Script the model for better performance
        self.model = torch.jit.script(model)
        logger.info("Model scripted successfully")


        # Define preprocessing transform
        self.transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], 
                               std=[0.229, 0.224, 0.225])
        ])
        
        # Load ImageNet class names
        self._load_class_names()
        
    def _load_class_names(self):
        """Load ImageNet class names"""
        # Simple mapping for ImageNet classes
        try:
            # Try to load from a file if available
            with open('inference_service/imagenet_classes.txt', 'r') as f:
                self.class_names = [line.strip() for line in f.readlines()]
                logger.info("ImageNet classes loaded from file")
        except FileNotFoundError:
            # Fallback to a few sample classes
            self.class_names = [f"class_{i}" for i in range(1000)]
            logger.warning("ImageNet classes file not found, using generic class names")
    
    def _preprocess_image(self, image_bytes: bytes) -> torch.Tensor:
        """Preprocess image for inference

        Args:
            image_bytes (bytes): Raw image bytes

        Returns:
            torch.Tensor: Preprocessed image tensor

        """

        # Kinda hacky way to convert bytes to PIL Image
        # when we can just user PIL directly Image.open(io.BytesIO(image_bytes)).convert('RGB')
        image = np.frombuffer(image_bytes, np.uint8)
        image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

        # Convert to PIL Image for torchvision transforms
        image = Image.fromarray(image)

        # Apply transforms
        image_tensor = self.transform(image).unsqueeze(0)
        return image_tensor.to(self.device)
    
    def ClassifyImage(self, request: inference_pb2.ImageRequest, context: grpc.ServicerContext) -> inference_pb2.ClassificationResponse:
        """Perform image classification

        Args:
            request (inference_pb2.ImageRequest): gRPC request containing image data
            context (grpc.ServicerContext): gRPC context for error handling

        Returns:
            inference_pb2.ClassificationResponse: gRPC response with classification results

        """
        start_time = time.time()
        
        try:
            # Preprocess image
            image_tensor = self._preprocess_image(request.image_data)
            
            # Run inference
            with torch.no_grad():
                outputs = self.model(image_tensor)
                probabilities = torch.nn.functional.softmax(outputs[0], dim=0)
                confidence, predicted_idx = torch.max(probabilities, 0)
                
            # Get class name
            predicted_class = self.class_names[predicted_idx.item()]
            confidence_score = confidence.item()
            
            inference_time = time.time() - start_time
            
            logger.info(f"Classified image {request.image_id}: {predicted_class} "
                       f"(confidence: {confidence_score:.3f}, time: {inference_time:.3f}s)")
            
            return inference_pb2.ClassificationResponse(
                predicted_class=predicted_class,
                confidence=confidence_score,
                inference_time=inference_time,
                image_id=request.image_id
            )
            
        except Exception as e:
            logger.error(f"Error during inference: {str(e)}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Inference failed: {str(e)}")
            return inference_pb2.ClassificationResponse()
    
    def HealthCheck(self, request: inference_pb2.HealthRequest, context: grpc.ServicerContext) -> inference_pb2.HealthResponse:
        """Health check endpoint

        Args:
            request (inference_pb2.HealthRequest): gRPC request for health check
            context (grpc.ServicerContext): gRPC context for error handling

        Returns:
            inference_pb2.HealthResponse: gRPC response indicating service health

        """
        try:
            # Simple health check - ensure model is loaded
            if self.model is not None:
                return inference_pb2.HealthResponse(
                    healthy=True,
                    status="Service is healthy and ready"
                )
            else:
                return inference_pb2.HealthResponse(
                    healthy=False,
                    status="Model not loaded"
                )
        except Exception as e:
            return inference_pb2.HealthResponse(
                healthy=False,
                status=f"Health check failed: {str(e)}"
            )

def serve():
    """Start the gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    inference_pb2_grpc.add_InferenceServiceServicer_to_server(
        InferenceServicer(), server
    )
    
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Starting gRPC server on {listen_addr}")
    server.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down gRPC server...")
        server.stop(0)
        server.wait_for_termination()

if __name__ == '__main__':
    serve()
