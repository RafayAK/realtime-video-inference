import torch
import os

# Disable forked repo validation
torch.hub._validate_not_a_forked_repo = lambda a, b, c: True

# Load and prepare the model
model = (
    torch.hub.load("pytorch/vision:v0.10.0", "resnet18", pretrained=True)
    .eval()
    .to("cuda")
)

# Script the model for TorchScript
scripted_model = torch.jit.script(model)

# Define the model repository structure
model_repository = "model_repository"
model_name = "resnet18"
version = "1"
model_dir = os.path.join(model_repository, model_name, version)


# Create the directory structure
os.makedirs(model_dir, exist_ok=True)


# Save the model
model_path = os.path.join(model_dir, "model.pt")
torch.jit.save(scripted_model, model_path)
