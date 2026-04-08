import sys
import pytest


@pytest.fixture(autouse=True, scope="session")
def patch_sys_argv():
    """Globally injects required topic parameters into sys.argv so argparse doesn't crash the test suite when initializing PipelineOptions."""
    sys.argv = [
        "pytest",
        "--input_topic=projects/test/topics/in",
        "--output_topic=projects/test/topics/out",
    ]
