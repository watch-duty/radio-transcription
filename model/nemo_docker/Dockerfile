# Start from the official, fully-loaded NeMo container
FROM nvcr.io/nvidia/nemo:26.02.00

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Set encoding to UTF-8 for handling special characters
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install ZSH, Git, and set up ZSH
RUN apt-get update && \
    apt-get install -y --no-install-recommends zsh git wget ffmpeg && \
    wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh | sh -s -- -t robbyrussell && \
    rm -rf /var/lib/apt/lists/*

# Highlight the prompt so we know we are in a docker container.
RUN echo "export PROMPT=\"%F{yellow}(docker)%f \$PROMPT\"" >> ~/.zshrc

# Copy standalone requirements
COPY model/notebook_docker/requirements.txt /tmp/notebook_requirements.txt

# Download requirements.txt first to cache pip install
RUN --mount=type=cache,target=/root/.cache/pip \
    wget https://raw.githubusercontent.com/NVIDIA/NeMo/v2.7.2/tools/speech_data_explorer/requirements.txt -O /tmp/requirements.txt && \
    pip install -r /tmp/requirements.txt && \
    pip install --upgrade -r /tmp/notebook_requirements.txt && \
    pip install peft && \
    pip install --upgrade torch torchvision torchaudio && \
    pip install "nemo_toolkit[asr,tts] @ git+https://github.com/NVIDIA/NeMo.git"

RUN /opt/venv/bin/pip install --no-cache-dir -U git+https://github.com/huggingface/transformers.git

# Download ONLY the directories needed from NeMO github repo.
WORKDIR /workspace/nemo
RUN git init && \
    git remote add origin https://github.com/NVIDIA/NeMo.git && \
    git config core.sparseCheckout true && \
    echo "examples/asr" >> .git/info/sparse-checkout && \
    echo "tools/speech_data_explorer" >> .git/info/sparse-checkout && \
    git fetch --depth 1 origin refs/tags/v2.7.2:refs/tags/v2.7.2 && \
    git checkout tags/v2.7.2

# Create a data directory for transcriptions (mount for local data at runtime)
RUN mkdir -p /workspace/nemo/data

# Launch into ZSH
ENTRYPOINT ["/bin/zsh"]
