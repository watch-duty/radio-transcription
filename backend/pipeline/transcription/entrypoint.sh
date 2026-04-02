#!/bin/bash
export PYTHONPATH="/app:$PYTHONPATH"
exec /opt/apache/beam/boot "$@"
