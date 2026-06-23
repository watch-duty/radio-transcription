#!/usr/bin/env python3
"""Script to validate that all Pub/Sub schema definitions are valid.

GCP Pub/Sub requires that a Protocol Buffer schema definition contains
exactly one top-level message type at the root level of the file.

This script uses the official 'grpc_tools.protoc' compiler to parse the
schemas into a FileDescriptorSet and inspect them using 'google.protobuf'.
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import grpc_tools
from google.protobuf import descriptor_pb2

# Non-Pub/Sub schemas or internal state definitions that are allowed
# to have multiple top-level messages.
EXCLUDED_PROTOS = {
    "streaming_state.proto",
}


def get_google_proto_path() -> Path:
    """Dynamically finds where the 'protobuf' library stores its .proto source."""
    for p in grpc_tools.__path__:
        proto_dir = Path(p) / "_proto"
        if proto_dir.is_dir():
            return proto_dir
    err_msg = "Google protobuf standard paths not found."
    raise RuntimeError(err_msg)


def validate_proto(filepath: Path) -> bool:
    """Validates a single proto file for a single top-level message type."""
    google_proto_path = get_google_proto_path()

    with tempfile.TemporaryDirectory() as tmpdir:
        descriptor_set_path = Path(tmpdir) / "descriptor.pb"

        # Run protoc to compile the proto file to a FileDescriptorSet
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{filepath.parent}",
                f"-I{google_proto_path}",
                f"--descriptor_set_out={descriptor_set_path}",
                str(filepath),
            ],
            capture_output=True,
            check=False,
        )

        if result.returncode != 0:
            sys.stderr.write(f"Error compiling '{filepath}':\n")
            sys.stderr.write(result.stderr.decode("utf-8") + "\n")
            return False

        with descriptor_set_path.open("rb") as f:
            fds = descriptor_pb2.FileDescriptorSet()
            fds.ParseFromString(f.read())

        # Extract the top-level messages from the target file descriptor
        target_filename = filepath.name

        for fd in fds.file:
            # Match the target filename to extract its messages
            if Path(fd.name).name == target_filename:
                num_messages = len(fd.message_type)
                if num_messages > 1:
                    message_names = [m.name for m in fd.message_type]
                    sys.stderr.write(
                        f"Error: Pub/Sub schema '{target_filename}' has {num_messages} "
                        f"top-level message types defined: {', '.join(message_names)}.\n"
                    )
                    sys.stderr.write(
                        "GCP Pub/Sub schemas must contain exactly one top-level message type. "
                        "Please nest helper messages inside the main message.\n"
                    )
                    return False
                if num_messages == 0:
                    sys.stderr.write(
                        f"Error: Pub/Sub schema '{target_filename}' has no top-level message type defined.\n"
                    )
                    return False
                break

    return True


def main() -> None:
    protos_dir = Path("protos")

    if not protos_dir.is_dir():
        sys.stdout.write(f"No .proto files found in '{protos_dir}' directory.\n")
        sys.exit(0)

    proto_files = list(protos_dir.glob("*.proto"))

    if not proto_files:
        sys.stdout.write(f"No .proto files found in '{protos_dir}' directory.\n")
        sys.exit(0)

    errors = 0
    for filepath in sorted(proto_files):
        filename = filepath.name
        if filename in EXCLUDED_PROTOS:
            continue
        if not validate_proto(filepath):
            errors += 1

    if errors > 0:
        sys.exit(1)
    sys.stdout.write("All Pub/Sub schemas validated successfully.\n")


if __name__ == "__main__":
    main()
