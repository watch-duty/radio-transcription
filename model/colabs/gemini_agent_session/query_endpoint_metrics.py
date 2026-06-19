import os
import time
from google.cloud import monitoring_v3

# Target GCP Project
project_id = "automatic-hawk-481415-m9"

print("=== QUERYING HYPERVISOR TUNED MODEL METRICS ===")
print(f"Project: {project_id}\n")

client = monitoring_v3.MetricServiceClient()
project_name = f"projects/{project_id}"

# Query the last 7 days to cover their recent batch run
now = time.time()
seconds = int(now)
nanos = int((now - seconds) * 10**9)

interval = monitoring_v3.TimeInterval(
    {
        "end_time": {"seconds": seconds, "nanos": nanos},
        "start_time": {"seconds": seconds - 7 * 24 * 3600, "nanos": nanos},
    }
)

# SFT Tuned Model metric types
metric_types = [
    "aiplatform.googleapis.com/tuned_model/online_serving/model_invocation_latencies",
    "aiplatform.googleapis.com/tuned_model/online_serving/tokens",
    "aiplatform.googleapis.com/tuned_model/online_serving/consumed_token_throughput",
]

for metric_type in metric_types:
    print(f"\nExecuting query on '{metric_type}'...")
    filter_str = f'metric.type = "{metric_type}"'

    try:
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )

        found_data = False
        for time_series in results:
            found_data = True
            endpoint_id = time_series.resource.labels.get(
                "endpoint_id", "UNKNOWN"
            )
            model_id = time_series.metric.labels.get("model_id", "UNKNOWN")

            print(
                f"✅ Found Series -> Endpoint: {endpoint_id} | Model ID: {model_id}"
            )

            # Print the data points (timestamp and distribution/value)
            for point in list(time_series.points)[:5]:
                local_time = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(point.interval.end_time.seconds),
                )

                # Distribution values (like latencies or tokens) contain ranges, counts, and means
                if point.value.distribution_value:
                    dist = point.value.distribution_value
                    print(
                        f"  * {local_time} -> Count: {dist.count} | Mean: {dist.mean:.2f}"
                    )
                elif point.value.int64_value is not None:
                    print(
                        f"  * {local_time} -> Value: {point.value.int64_value}"
                    )
                else:
                    print(
                        f"  * {local_time} -> Value: {point.value.double_value}"
                    )

        if not found_data:
            print(f"ℹ️ No data found for {metric_type} in the last 7 days.")

    except Exception as e:
        print(f"❌ Failed to query {metric_type}: {e}")
