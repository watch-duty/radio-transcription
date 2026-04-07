# Start from the official, fully-loaded NeMo container
FROM nvcr.io/nvidia/nemo:26.02.00

# 1. Install ZSH and Git
RUN apt-get update && apt-get install -y zsh git wget && rm -rf /var/lib/apt/lists/*

# 2. Download ONLY the example scripts from GitHub
# This puts them in /workspace/scripts
WORKDIR /workspace/scripts
RUN git init && \
    git remote add origin https://github.com/NVIDIA/NeMo.git && \
    git config core.sparseCheckout true && \
    echo "examples/asr" >> .git/info/sparse-checkout && \
    git fetch origin refs/tags/v2.7.2:refs/tags/v2.7.2 && \
    git checkout tags/v2.7.2

# 3. Set up ZSH for a better experience
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh)" -- \
    -t robbyrussell

# Set the working directory to the ASR scripts
WORKDIR /workspace/scripts/examples/asr

# Launch into ZSH
ENTRYPOINT ["/bin/zsh"]
