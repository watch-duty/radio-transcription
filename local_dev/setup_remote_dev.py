#!/usr/bin/env python3
import json
import os
import subprocess
import sys


def get_gcloud_project():
    project = os.environ.get("PROJECT_ID") or os.environ.get(
        "GOOGLE_CLOUD_PROJECT"
    )
    if project:
        return project
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=True,
        )
        project = result.stdout.strip()
        if project:
            return project
    except Exception:
        pass
    return "<PROJECT_ID>"


def check_adc():
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    adc_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
    )

    project_id = get_gcloud_project()
    service_account = (
        f"radio-transcription-api-dev@{project_id}.iam.gserviceaccount.com"
    )

    if os.path.exists(adc_path):
        try:
            with open(adc_path) as f:
                data = json.load(f)
                if (
                    data.get("type") == "impersonated_service_account"
                    and "radio-transcription-api-dev"
                    in data.get("service_account_impersonation_url", "")
                ):
                    print("Verifying IAM impersonation permissions...")
                    try:
                        subprocess.run(
                            [
                                "gcloud",
                                "auth",
                                "print-access-token",
                                f"--impersonate-service-account={service_account}",
                            ],
                            capture_output=True,
                            check=True,
                        )
                    except subprocess.CalledProcessError:
                        print(
                            f"\n[ERROR] You do not have permission to impersonate the service account: {service_account}"
                        )
                        print(
                            "Please ask a GCP Admin to grant your Google account the 'Service Account Token Creator' role (roles/iam.serviceAccountTokenCreator).\n"
                        )
                        sys.exit(1)
                    else:
                        print("IAM impersonation verified successfully!")
                        return
        except Exception:
            pass

    print(
        "\n[ERROR] Application Default Credentials (ADC) are missing or not impersonating the dev service account."
    )
    print("To resolve this, please run:")
    print(
        f"gcloud auth application-default login --impersonate-service-account={service_account}\n"
    )
    sys.exit(1)


def get_gcp_env():
    print("Fetching remote environment configuration from GCP...")
    cmd = [
        "gcloud",
        "run",
        "services",
        "describe",
        "radio-transcription-api-dev",
        "--region=us-central1",
        "--format=json",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        service_data = json.loads(result.stdout)
        env_vars = service_data["spec"]["template"]["spec"]["containers"][0][
            "env"
        ]
        return {x["name"]: x["value"] for x in env_vars}
    except Exception as e:
        print(f"\n[ERROR] Failed to fetch service configuration from GCP: {e}")
        print(
            "Please ensure you are logged into gcloud (`gcloud auth login`) and have access to the project.\n"
        )
        sys.exit(1)


def merge_and_write_env(file_path, new_values, defaults_file_path=None):
    current_env = {}

    # Load defaults first if provided
    if defaults_file_path and os.path.exists(defaults_file_path):
        with open(defaults_file_path) as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    current_env[k.strip()] = v.strip()

    # Load existing file if it exists to preserve edits
    if os.path.exists(file_path):
        with open(file_path) as f:
            for line in f:
                if "=" in line and not line.strip().startswith("#"):
                    k, v = line.strip().split("=", 1)
                    current_env[k.strip()] = v.strip()

    # Update with new values
    for k, v in new_values.items():
        current_env[k.strip()] = v.strip()

    # Write back
    with open(file_path, "w") as f:
        for k, v in sorted(current_env.items()):
            f.write(f"{k}={v}\n")
    print(f"Successfully configured {file_path}!")


def main():
    check_adc()
    gcp_env = get_gcp_env()

    # 1. Configure frontend/api/.env.local
    api_updates = dict(gcp_env)
    api_updates["ALLOWED_ORIGIN"] = "http://localhost:5173"
    api_updates["API_PUBLIC_URL"] = "http://localhost:5173"
    api_updates["AUTH_BACKEND"] = "google"
    merge_and_write_env(
        "frontend/api/.env.local",
        api_updates,
        defaults_file_path="frontend/api/.env.example",
    )

    # 2. Configure frontend/transcription-ui/.env.dev.local
    ui_updates = {
        "VITE_GOOGLE_AUTH_CLIENT_ID": gcp_env["GOOGLE_AUTH_CLIENT_ID"],
        "VITE_API_BASE_URL": "",
        "VITE_PROXY_API_TARGET": gcp_env["API_PUBLIC_URL"],
        "VITE_PROXY_API_ORIGIN": gcp_env["ALLOWED_ORIGIN"],
        "VITE_ALERT_ICON_SYMBOL_NAME": "warning",
    }
    merge_and_write_env(
        "frontend/transcription-ui/.env.dev.local",
        ui_updates,
        defaults_file_path="frontend/transcription-ui/.env.example",
    )


if __name__ == "__main__":
    main()
