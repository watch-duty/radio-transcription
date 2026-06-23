#!/usr/bin/env python3
"""Script to validate that all Pub/Sub schema definitions are valid.

GCP Pub/Sub requires that a Protocol Buffer schema definition contains
exactly one top-level message type at the root level of the file.

This script uses the official 'grpc_tools.protoc' compiler to parse the
schemas into a FileDescriptorSet and inspect them using 'google.protobuf'.
"""

import glob
import os
import subprocess
import sys
import tempfile

import grpc_tools
from google.protobuf import descriptor_pb2

# Non-Pub/Sub schemas or internal state definitions that are allowed
# to have multiple top-level messages.
EXCLUDED_PROTOS = {
    "streaming_state.proto",
}


def get_google_proto_path() -> str:
    """Dynamically finds where the 'protobuf' library stores its .proto source."""
    for p in grpc_tools.__path__:
        proto_dir = os.path.join(p, "_proto")
        if os.path.isdir(proto_dir):
            return proto_dir
    raise RuntimeError("Could not determine google.protobuf standard paths.")


def validate_proto(filepath: str) -> bool:
    """Validates a single proto file for a single top-level message type."""
    google_proto_path = get_google_proto_path()

    with tempfile.TemporaryDirectory() as tmpdir:
        descriptor_set_path = os.path.join(tmpdir, "descriptor.pb")

        # Run protoc to compile the proto file to a FileDescriptorSet
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{os.path.dirname(filepath)}",
                f"-I{google_proto_path}",
                f"--descriptor_set_out={descriptor_set_path}",
                filepath,
            ],
            capture_output=True,
            check=False,
        )

        if result.returncode != 0:
            print(f"Error compiling '{filepath}':", file=sys.stderr)
            print(result.stderr.decode("utf-8"), file=sys.stderr)
            return False

        with open(descriptor_set_path, "rb") as f:
            fds = descriptor_pb2.FileDescriptorSet()
            fds.ParseFromString(f.read())

        # Extract the top-level messages from the target file descriptor
        target_filename = os.path.basename(filepath)

        for fd in fds.file:
            # Match the target filename to extract its messages
            if os.path.basename(fd.name) == target_filename:
                num_messages = len(fd.message_type)
                if num_messages > 1:
                    message_names = [m.name for m in fd.message_type]
                    print(
                        f"Error: Pub/Sub schema '{target_filename}' has {num_messages} "
                        f"top-level message types defined: {', '.join(message_names)}.",
                        file=sys.stderr,
                    )
                    print(
                        "GCP Pub/Sub schemas must contain exactly one top-level message type. "
                        "Please nest helper messages inside the main message.",
                        file=sys.stderr,
                    )
                    return False
                # Pub/Sub schemas must have at least one message defined
                if num_messages == 0:
                    print(
                        f"Error: Pub/Sub schema '{target_filename}' has no top-level message type defined.",
                        file=sys.stderr,
                    )
                    return False
                break

    return True


def main() -> None:
    protos_dir = "protos"
    proto_files = glob.glob(os.path.join(protos_dir, "*.proto"))

    if not proto_files:
        print(f"No .proto files found in '{protos_dir}' directory.")
        sys.exit(0)

    errors = 0
    for filepath in sorted(proto_files):
        filename = os.path.basename(filepath)
        if filename in EXCLUDED_PROTOS:
            continue
        if not validate_proto(filepath):
            errors += 1

    if errors > 0:
        sys.exit(1)
    print(
        "All Pub/Sub schemas validated successfully."
    )


if __name__ == "__main__":
    main()
