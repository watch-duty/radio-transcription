# ASR Evaluations

## Architecture

The ASR Evaluation framework uses a decoupled **Driver/Worker** architecture to benchmark and compare different Speech-to-Text models:

- **Driver**: Orchestrates the evaluation process. It reads a dataset manifest (JSONL file containing audio paths), sends transcription requests to the model workers, and aggregates the results into a final output file.
- **Model Workers**: Independent microservices wrapped in FastAPI and hosted in Docker containers. Each worker serves a specific model (e.g., Qwen, Canary) and exposes a `/transcribe` endpoint. This allows for easy scaling, environment isolation (CPU vs GPU), and simple addition of new models without changing the orchestration logic.

## "Local" Development
Each model is developed on its own colab, which is run on a Jupyter notebook. You can find them under `/model/colabs`.

When developing locally, we have a standard docker-compose.yml file which can be used to spin up a jupyter notebook with all the necessary dependencies. See `asr-eval-docker-compose.yml`.

If you want to run the Docker image with GPUs, you will need to create a GCE instance in your GCP project with a GPU attached. There is a Terraform definition under `/terraform/modules/asr_evaluation` which can create a dedicated instance for you. Running GPUs can be costly, so you will need to manually turn on your instance. When the instance starts, there is an auto shutoff script that runs after a specified number of hours, which you can configure through the `auto_shutdown_hours` Terraform variable.

Since each contributor will have their own dedicated instance, make sure to use a unique name for your instance to avoid conflicts. When you run the terraform plan, the state will be saved locally. You'll want to keep it so that you can easily make changes to your instance if the definition gets updated.

```bash
# Assuming you're starting from the root directory of the repo, navigate to the asr_evaluation module.
cd terraform/modules/asr_evaluation

# Create a tfvars file with the relevant variables filled in
cat <<EOF > local_variables.tfvars
name       = "<whatever you want to call your vm>"
project_id = "<dev_project_id>"
EOF

terraform init
terraform plan -var-file=local_variables.tfvars
terraform apply -var-file=local_variables.tfvars
```

### Setup docker on the VM (for GPU runs)
Once you have your instance provisioned and set up. You can setup docker on the instance:
```bash
gcloud compute ssh <your_instance_name> \
    --project <your_project_id> \
    --zone us-central1-a

sudo apt update
sudo apt install -y docker.io docker-compose-v2
# Verify installation completed
docker --version
docker compose version

# Verify that you have GPU configured
sudo docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi
```

Optional: Make docker sudoless
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### Run docker directly on the VM (with GPU)
Setup
```bash
git clone https://github.com/watch-duty/radio-transcription.git
cd radio-transcription
```

Run the desired services using Docker Compose:
```bash
# Add in sudo if you didn't make docker sudoless

# Run Jupyter Notebook environment (with NeMO on CPU)
docker compose -f asr-eval-docker-compose.yml up -d asr-eval-cpu

# Run Jupyter Notebook environment (with NeMO on GPU)
docker compose -f asr-eval-docker-compose.yml up -d asr-eval

# Run lightweight Jupyter Notebook (without NeMO)
docker compose -f asr-eval-docker-compose.yml up -d notebooks

# Run specific Model Worker APIs (FastAPI)
docker compose -f asr-eval-docker-compose.yml up -d qwen-asr-cpu
docker compose -f asr-eval-docker-compose.yml up -d canary-qwen

# To access NeMo CLI using the asr-eval container image
docker compose -f asr-eval-docker-compose.yml run --entrypoint /bin/zsh asr-eval
```

Accessing the Jupyter notebooks from your local machine
```bash
# Port forwarding for you to be able to access the notebook from your browser.
# The notebook should be accessible via localhost:8888
gcloud compute ssh <your_instance_name> \
    --project <your_project_id> \
    --zone us-central1-a \
    -- -L 8888:localhost:8888
```

Alternatively, if you want to use VSCode or your local IDE, you can also use Remote SSH. This way you won't have to keep syncing changes between your machine and your local code.

## Adding a New Model for Evaluation

To add a new Speech-to-Text model to the framework, follow these steps:

### 1. Create the Model Directory
Create a new folder under `model/models/` for your model (e.g., `model/models/my_new_model/`).

### 2. Implement the FastAPI Service (`api.py`)
Create an `api.py` file that exposes a `/transcribe` endpoint. Follow these best practices:
- **Use Lifespan Events**: Load your model during application startup using FastAPI's `lifespan` context manager to avoid loading delays on the first request.
- **Robust File Handling**: Use `tempfile` and ensure files are deleted in a `finally` block to prevent disk space leaks.

### 3. Create the `Dockerfile`
Create a `Dockerfile` in your model directory:
- Use a slim base image if the model runs on CPU.
- Install necessary system dependencies (like `ffmpeg`).
- Copy your code and expose port 8000.

### 4. Register in Docker Compose
Add your new service to `asr-eval-docker-compose.yml`. Map a unique host port to container port 8000.

### 5. Update the Driver/Colab
Update your evaluation notebook or driver script to target your new service's endpoint (e.g., `http://my_new_model:8000/transcribe`).