# ASR Evaluations

## "Local" Development
Each model is developed on its own colab, which is run on a Jupyter notebook. You can find them under `/model/colabs`.

When developing locally, we have a standard docker-compose.yml file which can be used to spin up a jupyter notebook with all the necessary dependencies. See `asr-eval-docker-compose.yml`.

> [!NOTE]
> Use the `asr-eval` container if you need NeMo/Canary support. Otherwise, use the lightweight `notebooks` container for pure Hugging Face evaluations like Whisper or Cohere to avoid heavy dependency overhead.

If you want to run the Docker image with GPUs, you will need to create a GCE instance in your GCP project with a GPU attached. There is a Terraform definition under `/terraform/modules/asr_evaluation` which can create a dedicated instance for you. Running GPUs can be costly, so you will need to manually turn on your instance. When the instance starts, there is an auto shutoff script that runs after a specified number of hours, which you can configure through the `auto_shutdown_hours` Terraform variable.

Since each contributor will have their own dedicated instance, make sure to use a unique name for your instance to avoid conflicts. When you run the terraform plan, the state will be saved locally. You'll want to keep it so that you can easily make changes to your instance if the definition gets updated.

```
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
```
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
```
sudo usermod -aG docker $USER
newgrp docker
```

### Run docker directly on the VM (with GPU)
Setup
```
git clone https://github.com/watch-duty/radio-transcription.git
cd radio-transcription
```

Run all 3 containers (NeMO + Jupyter, NeMO CLI, and Jupyter)
```
# Add in sudo if you didn't make docker sudoless

# Run NeMO + Jupyter | Jupyter
docker compose -f asr-eval-docker-compose.yml up -d [asr-eval-cpu|notebooks]

# To access NeMo CLI using the asr-eval container image
docker compose -f asr-eval-docker-compose.yml run --entrypoint /bin/zsh asr-eval

# Run NeMO + Jupyter with GPU
docker compose -f asr-eval-docker-compose.yml up -d asr-eval
```

Accessing the Jupyter notebooks from your local machine
```
# Port forwarding for you to be able to access the notebook from your browser.
# The notebook should be accessible via localhost:8888
gcloud compute ssh <your_instance_name> \
    --project <your_project_id> \
    --zone us-central1-a \
    -- -L 8888:localhost:8888
```

Alternatively, if you want to use VSCode or your local IDE, you can also use Remote SSH. This way you won't have to keep syncing changes between your machine and your local code.

## Adding a New Model Evaluation

To add a new model to the evaluation framework, follow these guidelines:

1.  **Create a Notebook**: Create a new notebook in `model/colabs/` named `evaluate_[model_name].ipynb`.
2.  **Use the Common Runner**: Import `run_inference_pipeline` from `colabs.common.inference_pipeline_runner` to handle the evaluation loop. This handles downloading, preprocessing, and cleanup automatically.
3.  **Define Required Callables**:
    *   `prompt_formatter(entry, local_path)`: Returns the prompt structure for the model.
    *   `inference_fn(model, prompts)`: Runs inference on a batch of prompts.
    *   `decode_fn(output, model)`: Extracts the text transcription from the output.
4.  **Dependencies**: If the model requires new packages, add them to `model/notebook_docker/requirements.txt`. If a cutting-edge version is needed (e.g. not in stable release yet), you can use a Git URL (e.g., `git+https://github.com/...`).

## Docker Commands for Maintenance

If you make changes to the `requirements.txt` or the Dockerfiles, use these commands to rebuild and test:

*   **Rebuild Image**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml build asr-eval
    ```
*   **Start Container**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml up asr-eval
    ```
*   **Run CPU Version**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml up asr-eval-cpu
    ```

## Running Baseline Evaluations

If you want to evaluate a model on a public dataset (like LibriSpeech) to get a baseline Word Error Rate (WER) score, you can use the `run_test_baseline_inference_evaluation` function in `inference_pipeline_runner.py`.

This function uses Hugging Face datasets in streaming mode, so it doesn't require downloading the full dataset.

### Example Usage

Add this to a cell in your model notebook:

```python
from common.inference_pipeline_runner import run_test_baseline_inference_evaluation

# Run the baseline evaluation on Librispeech
wer_score, predictions, references = run_test_baseline_inference_evaluation(
    model=model, 
    prompt_fn=prompt_formatter,
    inference_fn=inference_fn,
    decode_fn=decode_fn,
    num_examples=20,  # Number of examples to test, 0 means test on the whole dataset
    dataset_name="librispeech_asr",
    dataset_config="clean",
    split="test"
)

print(f"Final WER Score: {wer_score}")
```

> [!IMPORTANT]
> Ensure that your `prompt_fn` can handle `None` being passed as the `entry` argument, as public datasets do not have the custom metadata structure of your specific manifests.