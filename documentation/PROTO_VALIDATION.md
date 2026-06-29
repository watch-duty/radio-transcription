# Protobuf Schema Validation Guide

This document explains the automated Protocol Buffer validation system in the `radio-transcription` repository, which ensures that all schemas deployed to Google Cloud Pub/Sub conform to platform-specific constraints.

---

## Why Do We Validate?

Google Cloud Pub/Sub has a strict platform requirement for Protocol Buffer schemas:
> [!IMPORTANT]
> **GCP Pub/Sub schemas must contain exactly one top-level (root-level) message type.**

If a `.proto` file containing multiple independent root-level messages is flattened and uploaded as a Pub/Sub schema, the deployment will result in a failure.

To prevent this from blocking deployments, we run automated validation on every schema file in the [protos/](../protos/) directory during local development and CI/CD.

---

## Validation Script

The validation is performed by the [validate_schemas.py](../scripts/validate_schemas.py) script. 

The script uses the Google Protobuf compiler (`protoc`)** to ensure AST-level correctness:
1. It dynamically locates the bundled Google standard library path (needed for imports like `google/protobuf/timestamp.proto`).
2. It compiles the target `.proto` file into a temporary binary `FileDescriptorSet` using `grpc_tools.protoc`.
3. It parses the descriptor set using `google.protobuf.descriptor_pb2`.
4. It counts the number of messages in the target file's `message_type` descriptor. If the count is not **exactly 1**, the check fails.

---

## Exceptions

We make exceptions for protos that are used exclusively for internal serialization and are **never** deployed as GCP Pub/Sub schema resources.

These exceptions are declared in the `EXCLUDED_PROTOS` set in `validate_schemas.py`.

> [!CAUTION]
> Do **not** add a file to `EXCLUDED_PROTOS` simply to bypass a validation failure. If the file is deployed as a Pub/Sub schema, excluding it here will only cause the failure to happen later during the Terraform deployment phase, which blocks the release pipeline.
