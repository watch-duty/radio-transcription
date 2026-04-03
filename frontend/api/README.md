## Radio Transcription API

This is the API for the Radio Transcription service. It is a Cloud Run function that serves as a proxy to the Radio Transcription service.

### OpenAPI Specification Generation & Google Auth Injection

We use `tsoa` to generate the OpenAPI specification (`openapi.yaml`) from our TypeScript controllers.

However, `tsoa` does not support vendor extensions like `x-google-auth` in security definitions (in `tsoa.json` or decorators). To work around this, we have a post-processing script:

*   **File**: `scripts/post-process-spec.js`
*   **Purpose**: This script runs after `yarn run tsoa spec` and hardcodes the `x-google-auth` extension into the generated `openapi.yaml` file under the `google_id_token` security scheme.
*   **Execution**: It is part of the `yarn generate-spec` command.

### Terraform Placeholders

The generated `openapi.yaml` and the configuration files contain placeholders intended to be replaced by Terraform during deployment (e.g., using `templatefile`):

1.  **`${google_auth_client_id}`**
    *   **Locations**: `scripts/post-process-spec.js` (which injects it into `openapi.yaml`)
    *   **Purpose**: The Google OAuth 2.0 Client ID used for audience verification in API Gateway / Cloud Endpoints.
2.  **`${radio_transcription_api_url}`**
    *   **Locations**: `tsoa.json`, `openapi.yaml`
    *   **Purpose**: The backend address for the Cloud Run service where requests should be routed.

Make sure to provide these variables when rendering the spec file in your Terraform configuration.
