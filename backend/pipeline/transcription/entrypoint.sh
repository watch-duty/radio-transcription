#!/bin/bash
export PYTHONPATH="/app:$PYTHONPATH"
exec /opt/google/dataflow/python_template_launcher "$@"
