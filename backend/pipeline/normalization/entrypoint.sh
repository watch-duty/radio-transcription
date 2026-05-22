#!/bin/bash
set -e

# Dataflow workers are invoked with specific flags like --logging_endpoint
# If we see that flag, we know we are running as a worker.
if [[ "$*" == *"--logging_endpoint"* ]]; then
    echo "Running as Dataflow Worker..."
    exec /opt/apache/beam/boot "$@"
else
    echo "Running as Flex Template Launcher..."
    exec /opt/google/dataflow/python_template_launcher "$@"
fi
