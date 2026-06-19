import os
from google.cloud import monitoring_v3

# Target GCP Project
project_id = "automatic-hawk-481415-m9"

print("=== LISTING ALL VERTEX AI METRIC DESCRIPTORS ===")
print(f"Project: {project_id}\n")

client = monitoring_v3.MetricServiceClient()
project_name = f"projects/{project_id}"

# Filter for all metrics under the aiplatform.googleapis.com domain
filter_str = 'metric.type = starts_with("aiplatform.googleapis.com/")'

try:
    results = client.list_metric_descriptors(
        request={"name": project_name, "filter": filter_str}
    )

    found = False
    for descriptor in results:
        found = True
        print(f"🔗 {descriptor.type}")
        print(f"   * Display Name: {descriptor.display_name}")
        print(f"   * Description: {descriptor.description}\n")

    if not found:
        print("ℹ️ No Vertex AI metric descriptors found in the project.")

except Exception as e:
    print(f"❌ Failed to list metric descriptors: {e}")
