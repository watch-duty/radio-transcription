# Use the official Python 3.11 image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the ENTIRE backend folder to preserve the import paths
COPY backend /app/backend

# Install the dependencies required for the evaluation pipeline
RUN pip install --no-cache-dir -r backend/pipeline/evaluation/requirements.txt

# Run the Functions Framework on port 8080
CMD ["functions-framework", "--target=evaluate_transcribed_audio_segment", "--signature-type=cloudevent", "--port=8080"]