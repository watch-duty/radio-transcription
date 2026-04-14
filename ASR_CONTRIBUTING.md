# ASR Evaluations

## "Local" Development
Each model is developed on its own colab, which is run on a Jupyter notebook. You can find them under `/model/colabs`.

When developing locally, we have a standard docker-compose.yml file which can be used to spin up a jupyter notebook with all the necessary dependencies. See `asr-eval-docker-compose.yml`.

Running the docker image requires GPUs. You will need to create a GCE instance in your GCP project with a GPU attached. There is a Terraform definition under `/terraform/modules/asr_evaluation` which can create a dedicated instance for you. Running GPUs can be costly, so there are auto-start and -stop policies to turn on and off your instance.

Since each contributor will have their own dedicated instance, make sure to use a unique name for your instance to avoid conflicts. When you run the terraform plan, the state will be saved locally. Make sure to keep it so that you can easily make changes to your instance if the definition gets updated.

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

Once you have your instance provisioned and set up. You can setup docker on the instance:
```
gcloud compute ssh <your_instance_name> \
    --project <your_project_id> \
    --zone us-central1-a
git clone https://github.com/watch-duty/radio-transcription.git
cd radio-transcription

sudo apt update
sudo apt install -y docker.io docker-compose-v2
# Verify installation completed
docker --version
docker compose version

# Verify that you have GPU configured
sudo docker run --rm --gpus all nvidia/cuda:12.0.0-base-ubuntu22.04 nvidia-smi

sudo docker compose -f asr-eval-docker-compose.yml up

# Port forwarding for you to be able to access the notebook from your browser.
# The notebook should be accessible via localhost:8888
gcloud compute ssh <your_instance_name> \
    --project <your_project_id> \
    --zone us-central1-a \
    -- -L 8888:localhost:8888
```