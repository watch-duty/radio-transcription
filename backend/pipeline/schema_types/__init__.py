# This directory contains generated schema types and should not be modified directly.
#
# Run the following command from the repository root to update the proto types:
# mise run generate:protos

import os
import sys

# Add this directory to sys.path so generated proto files can import each other
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
