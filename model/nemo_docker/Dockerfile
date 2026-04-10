# Start from the official, fully-loaded NeMo container
FROM nvcr.io/nvidia/nemo:26.02.00

# Install ZSH and Git
RUN apt-get update && apt-get install -y zsh git wget && rm -rf /var/lib/apt/lists/*

# Set up ZSH for a better experience
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh)" -- \
    -t robbyrussell

# Highlight the prompt so we know we are in a docker container.
RUN echo "export PROMPT=\"%F{yellow}(docker)%f \$PROMPT\"" >> ~/.zshrc

# Download ONLY the directories needed from NeMO github repo.
WORKDIR /workspace/nemo
RUN git init && \
    git remote add origin https://github.com/NVIDIA/NeMo.git && \
    git config core.sparseCheckout true && \
    echo "examples/asr" >> .git/info/sparse-checkout && \
    echo "tools/speech_data_explorer" >> .git/info/sparse-checkout && \
    git fetch origin refs/tags/v2.7.2:refs/tags/v2.7.2 && \
    git checkout tags/v2.7.2

RUN pip install -r tools/speech_data_explorer/requirements.txt
# Install this for now to make all the other colabs compatible.
RUN pip install jiwer loguru num2words 


# Set the working directory to the ASR scripts
WORKDIR /workspace/nemo

# Launch into ZSH
ENTRYPOINT ["/bin/zsh"]
