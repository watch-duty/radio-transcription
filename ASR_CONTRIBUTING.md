# ASR Evaluations

## "Local" Development
Each model is developed on its own colab, which is run on a Jupyter notebook. You can find them under `/model/colabs`.

When developing locally, we have a standard docker-compose.yml file which can be used to spin up a jupyter notebook with all the necessary dependencies. See `asr-eval-docker-compose.yml`.

> Use the `nemo-cli-cpu` or `nemo-cli-gpu` container if you need NeMo/Canary support. Otherwise, default to the lightweight `notebooks-cpu` or `notebooks` ASR experiment runtime for notebooks and non-NeMo command-line workflows like Gemini SFT, Whisper, Granite, or Cohere to avoid heavy dependency overhead.

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

### GCS Bucket Permissions (Important!)
Ensure that you deploy your evaluation GPU VM instance inside the **same GCP project (environment)** where your targeted GCS manifest bucket (e.g., `gs://wd-transcription-data`) resides. For example, if your manifest bucket lives in the `production` environment, your Terraform `project_id` must also target that `production` project ID. This natively guarantees that your VM's service credentials will have seamless, zero-configuration read/write access to your datasets without needing any manual cross-project IAM policy changes.



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

> [!IMPORTANT]
> **Google Cloud Credentials Setup:**
> Before starting the containers, you **must** run the following command on your host/VM machine:
> ```bash
> gcloud auth application-default login
> ```
> This generates your Application Default Credentials (ADC) under `${HOME}/.config/gcloud/`.
> 
> Because this directory is volume-mapped into the Docker containers (to let them natively authenticate with Google Cloud Storage and GCP APIs), running this command *before* starting the containers ensures the credential directory exists on the host with correct user ownership.
> 
> If you start the containers before running this command, Docker will automatically create the `${HOME}/.config/gcloud` directory with `root` ownership, causing permission-denied errors when you try to authenticate.


### Run the containers (Jupyter notebooks, ASR CLI workflows, and NeMo CLI)
```
# Add in sudo if you didn't make docker sudoless

# 1. Run Jupyter notebooks in the lightweight ASR experiment runtime (CPU or GPU)
docker compose -f asr-eval-docker-compose.yml up -d [notebooks-cpu|notebooks]

# 2. Run non-NeMo command-line workflows, such as Gemini SFT, in the same lightweight runtime
# The container installs the mounted radio-transcription-model package as
# /workspace/model[scoring,vertex]
# before executing the command.
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'

# 3. Run NeMo CLI (CPU or GPU)
# These containers do not run Jupyter by default; they are designed for interactive shell use.
# Run the command below to launch an interactive ZSH shell session:
docker compose -f asr-eval-docker-compose.yml run [nemo-cli-cpu|nemo-cli-gpu]
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

## Running Gemini SFT

Gemini supervised fine-tuning is a packaged CLI workflow, not a notebook-only
experiment. Use the lightweight ASR runtime (`notebooks-cpu` or `notebooks`) so
the mounted `model/` package is installed with the `scoring` and `vertex`
extras:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft --help'
```

Run configs are operator inputs and should stay outside the repo. The example
shape lives at `model/scripts/sft/run_config.example.toml`; the detailed
workflow contract is documented in `model/scripts/sft/README.md`. Every SFT run
owns a GCS prefix under `gs://<bucket>/sft/runs/<round-id>/`, and that prefix is
the durable state for prepare, tune resume, and eval.

## Adding a New Model Evaluation

To add a new model to the evaluation framework, follow these guidelines:

1.  **Create a Notebook**: Create a new notebook in `model/colabs/` named `evaluate_[model_name].ipynb`.
2.  **Use the Common Runner**: Import `run_inference_pipeline` from `common.inference_pipeline_runner` to handle the evaluation loop. This handles downloading, preprocessing, and cleanup automatically.
3.  **Define Required Callables**:
    *   `prompt_formatter(entry, local_path)`: Returns the prompt structure for the model.
    *   `inference_fn(model, prompts)`: Runs inference on a batch of prompts.
    *   `decode_fn(output, model)`: Extracts the text transcription from the output.
4.  **Dependencies**: If the model requires new third-party packages, add them to `model/notebook_docker/requirements.txt`. The lightweight runtime already installs the mounted `radio-transcription-model` package in editable mode on startup, so changes under `model/src/common/` and `model/src/gemini_sft/` are immediately available to notebooks and CLI workflows. If a cutting-edge version is needed (e.g. not in stable release yet), you can use a Git URL (e.g., `git+https://github.com/...`).

## Formatting and Linting Notebooks

To ensure your notebooks don't fail validation in GitHub PRs, you should format and lint them before committing:

*   **Format all notebooks**:
    ```bash
    mise run format:notebooks
    ```
    This will automatically repair schema issues (like `execution_count` in markdown cells) and format the code.

*   **Lint all notebooks**:
    ```bash
    mise run lint:notebooks
    ```
    This only checks for schema issues without modifying files.

These tasks are automatically included in the main `mise run format` and `mise run lint` pipelines.

## Docker Commands for Maintenance

If you make changes to the `requirements.txt` or the Dockerfiles, use these commands to rebuild and test:

*   **Rebuild Image**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml build notebooks-cpu
    docker compose -f asr-eval-docker-compose.yml build nemo-cli-cpu
    ```
*   **Run GPU CLI**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml run nemo-cli-gpu
    ```
*   **Run CPU CLI**:
    ```bash
    docker compose -f asr-eval-docker-compose.yml run nemo-cli-cpu
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
