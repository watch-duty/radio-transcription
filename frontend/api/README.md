## Radio Transcription API

This is the API for the Radio Transcription service. It is a Cloud Run function that serves as a proxy to the Radio Transcription service.

### OpenAPI Specification Generation & Google Auth Injection

We use `tsoa` to generate the OpenAPI specification (`openapi.yaml`) from our TypeScript controllers.

However, `tsoa` does not support vendor extensions like `x-google-auth` in security definitions (in `tsoa.json` or decorators). To work around this, we have a post-processing script:

- **File**: `scripts/post-process-spec.js`
- **Purpose**: This script runs after `yarn run tsoa spec` and hardcodes the `x-google-auth` extension into the generated `openapi.yaml` file under the `google_id_token` security scheme.
- **Execution**: It is part of the `yarn generate-spec` command.

### Terraform Placeholders

The generated `openapi.yaml` and the configuration files contain placeholders intended to be replaced by Terraform during deployment (e.g., using `templatefile`):

1.  **`${google_auth_client_id}`**
    - **Locations**: `scripts/post-process-spec.js` (which injects it into `openapi.yaml`)
    - **Purpose**: The Google OAuth 2.0 Client ID used for audience verification in API Gateway / Cloud Endpoints.
2.  **`${radio_transcription_api_url}`**
    - **Locations**: `tsoa.json`, `openapi.yaml`
    - **Purpose**: The backend address for the Cloud Run service where requests should be routed.
3.  **`${rules_api_url}`**
    - **Locations**: `tsoa.json`, `openapi.yaml`
    - **Purpose**: The backend address for the Rules Management service where requests should be routed.

Make sure to provide these variables when rendering the spec file in your Terraform configuration.

---

### Admin Group Membership Configuration (Google Cloud Identity)

To determine which users are granted Administrator privileges in the application, the backend queries a Google Workspace / Cloud Identity group (e.g., `radio-transcription-admins@YOUR_DOMAIN.org`).

This uses the GCP-native **Cloud Identity API** to check memberships using the backend service account's credentials (no admin user impersonation or Domain-Wide Delegation is required).

#### 1. Google Workspace Settings

1. Create or identify the Google Group you want to use for administration (e.g. `radio-transcription-admins@YOUR_DOMAIN.org`).
2. Open the Group's settings (in Google Groups or Google Workspace Admin Console) and ensure **"Allow members outside the organization"** (or **"Allow external members"**) is enabled. This is necessary because the GCP Service Account domain differs from your workspace domain.

#### 2. Google Cloud Setup

1. **Enable the Cloud Identity API**: Enable `cloudidentity.googleapis.com` in your Google Cloud Project.
2. **Retrieve your Backend Service Account**: Identify the email of the Service Account running your API backend (e.g., `radio-transcription-api-prod@YOUR_PROJECT.iam.gserviceaccount.com`).
3. **Grant Access**: Add the Service Account email directly as a **member** of the Google Group you created in Step 1.
   - _This gives the Service Account permission to query the group's memberships directly without needing broad Organization-level IAM roles._

#### 3. Environment Variables

Configure the following environment variables for the API backend (`.env.local` or Cloud Run config):

- **`WORKSPACE_ADMIN_GROUP_EMAIL`**:
  - Set this to the Google Group email address (e.g., `radio-transcription-admins@YOUR_DOMAIN.org`).
  - _Local Development:_ If this variable is omitted or left empty, the backend will log a warning and grant admin access to **all authenticated users** (bypassing Google APIs entirely for easy local testing).
