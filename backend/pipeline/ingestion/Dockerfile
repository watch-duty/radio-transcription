# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.11-slim

# Install ffmpeg and system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install --no-cache-dir \
    functions-framework \
    google-cloud-pubsub

# Run the web service on container startup using gunicorn.
# The Functions Framework uses the 'run_logger' function as the entry point.
# Cloud Run sets the PORT environment variable automatically.
CMD exec functions-framework --target=run_logger --port=${PORT:-8080}