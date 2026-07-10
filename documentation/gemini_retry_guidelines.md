# Gemini Transcription Retry Guidelines

This document outlines how the radio transcription pipeline classifies and handles errors returned by the Google Gemini API (Vertex AI). It serves as a guide for developer reference and maintaining consistency across error-handling logic.

---

## 1. Google Cloud Error Classification & Mapping

Google Cloud services use a unified error model mapping gRPC status codes to HTTP status codes.

| gRPC Status Code | HTTP Status Code | Category | Retryable / Transient? | Common Causes & Notes |
| :--- | :--- | :--- | :--- | :--- |
| `DEADLINE_EXCEEDED` (4) | `504 Gateway Timeout` | Server Error | **Yes** | Request took longer than the deadline. |
| `RESOURCE_EXHAUSTED` (8) | `429 Too Many Requests` | Client Error | **Yes** | Rate limits exceeded or quota exhausted. |
| `ABORTED` (10) | `409 Conflict` | Client Error | **Yes** | Concurrency conflict (e.g. read-modify-write failure). |
| `INTERNAL` (13) | `500 Internal Server Error` | Server Error | **Sometimes** | Internal server bug or transient network drops. |
| `UNAVAILABLE` (14) | `503 Service Unavailable` | Server Error | **Yes** | Target service is down or connection dropped. |
| `CANCELLED` (1) | `499 Client Closed Request` | Client Error | **Sometimes** | Caller cancelled the request. |
| `UNKNOWN` (2) | `500 Internal Server Error` | Server Error | **No** | Generic error, not safe to retry. |
| `INVALID_ARGUMENT` (3) | `400 Bad Request` | Client Error | **No** | Syntax or schema validation error. |
| `NOT_FOUND` (5) | `404 Not Found` | Client Error | **No** | Resource does not exist. |
| `ALREADY_EXISTS` (6) | `409 Conflict` | Client Error | **No** | Attempting to create a duplicate resource. |
| `PERMISSION_DENIED` (7) | `403 Forbidden` | Client Error | **No** | IAM permissions mismatch or API key invalid. |
| `FAILED_PRECONDITION` (9)| `400 Bad Request` | Client Error | **No** | System state mismatch (e.g. directory not empty). |
| `OUT_OF_RANGE` (11) | `400 Bad Request` | Client Error | **No** | Query bounds or offset invalid. |
| `UNIMPLEMENTED` (12) | `501 Not Implemented` | Server Error | **No** | Feature or endpoint not supported. |
| `DATA_LOSS` (15) | `500 Internal Server Error` | Server Error | **No** | Unrecoverable corruption. |
| `UNAUTHENTICATED` (16) | `401 Unauthorized` | Client Error | **No** | Missing or expired credentials. |

---

## 2. Transient vs. Permanent Errors

### Transient Errors (Safe & Recommended to Retry)
* **Resource Exhausted / Rate Limits (`429` / `RESOURCE_EXHAUSTED`):** Retrying after backoff gives the rate limiter time to reset.
* **Service Unavailable (`503` / `UNAVAILABLE`):** Transient network drops, service rollout restarts, or server crashes.
* **Deadline Exceeded (`504` / `DEADLINE_EXCEEDED`):** Server-side latency spike or timeout.
* **Aborted (`409` / `ABORTED`):** Transaction was aborted. Safe to retry the operation.

### Permanent Errors (Do NOT Retry)
* **Bad Request / Invalid Arguments (`400` / `INVALID_ARGUMENT` / `FAILED_PRECONDITION`):** The payload is invalid. Retrying without modification will fail.
* **Not Found (`404`):** Resource doesn't exist.
* **Already Exists (`409` / `ALREADY_EXISTS`):** Attempting to create a resource that is already present. (Note that `409` in HTTP represents both `ALREADY_EXISTS` and `ABORTED`—so checking the error message or detail structure is necessary).
* **Auth Errors (`401` / `403` / `PERMISSION_DENIED` / `UNAUTHENTICATED`):** Credentials need to be rotated or permissions updated.

---

## 3. Gemini API Response & FinishReason Handling

When calling the Gemini API to generate content, the response contains `candidates` representing the generated outputs. Each candidate has a `finish_reason` indicating why generation stopped.

### FinishReason Classification

| Finish Reason | Description | Classification / Action | Retryable? |
| :--- | :--- | :--- | :--- |
| `STOP` | The model successfully reached a natural end point or met a stop sequence. | Success. | **No** |
| `MAX_TOKENS` | The model stopped because it hit the configured token limit budget. | Success (output truncated). | **No** (requires increasing token limit) |
| `SAFETY` | Generation was aborted due to safety filters. | Policy Block. | **No** (permanent block) |
| `RECITATION` | Generation stopped because the output matched copyrighted content. | Policy Block. | **No** (permanent block) |
| `BLOCKLIST` | Output matched a configured blocklist word/phrase. | Policy Block. | **No** (permanent block) |
| `OTHER` | Unspecified model execution error mid-generation. | Transient Glitch. | **Yes** |
| `UNSPECIFIED` / `None` | Generation was not completed (e.g. prompt block, API crashed). | Transient/Permanent depending on presence of prompt feedback. | **Yes** (if no block reason is given) |

### Transcriber Edge-Case Handling

#### 1. Empty Content with `STOP` Finish Reason
If the model returns a candidate containing **no content or parts**, but the finish reason is `STOP`, it indicates the model successfully finished execution but generated nothing (e.g., the audio was silence or had no speech). 
* **Action:** This is treated as a successful execution. We return `None` (which defaults to an unintelligible marker) and **do not retry**.

#### 2. Empty Content with `None` (or No) Finish Reason
If the model returns a candidate with no content and a missing/null `finish_reason` (or other non-policy reasons like `OTHER`):
* **Action:** Classified as transient. We raise `GeminiTransientTranscriptionError`, triggering a retry.

#### 3. Completely Empty `candidates` List
If the API returns zero candidates:
* **Check Prompt Feedback:** First, inspect `response.prompt_feedback.block_reason`. If it is blocked, it is a permanent safety violation (raise `GeminiTranscriptionError` -> **no retry**).
* **Glitch / Drop:** If there is no block reason, it indicates an unexpected API drop/glitch. We raise `GeminiTransientTranscriptionError` -> **trigger retry**.

---

## 4. Codebase Implementation Alignment

### Client-Side Auto-Retry Setup
We configure the client-level automatic retry policy in [GeminiTranscriber.setup](file:///Users/jakeclose/watch_duty/radio-transcription/backend/pipeline/transcription/transcribers/gemini.py#L104-L119):
* Configured using `HttpRetryOptions` passed under `http_options`.
* This handles transient network errors transparently within the SDK without raising them up to the caller unless they persist after all attempts.

### Egress Event Processor Exception Handling
If an error persists and escalates out of the transcriber, [TranscriptionEventProcessor._transcribe_and_publish](file:///Users/jakeclose/watch_duty/radio-transcription/backend/pipeline/transcription/processor.py#L90-L166) catches it and classifies it via `_is_transient_exception`:
* **Transient Errors:** Re-raised to propagate to Google Cloud Pub/Sub, triggering a delayed message retry/redelivery.
* **Permanent Errors:** Acknowledged immediately by logging a permanent failure and writing the failure status to the metadata store.

---

## References
* [Google Cloud API Design Guide - Errors (AIP-193)](https://cloud.google.com/apis/design/errors)
* [Vertex AI API Reference - GenerateContentResponse & FinishReason](https://cloud.google.com/vertex-ai/docs/reference/rest/v1/GenerateContentResponse#finishreason)

