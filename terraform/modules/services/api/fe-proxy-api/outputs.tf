output "fe_proxy_api_name" {
  description = "The name of the deployed FE Proxy API Cloud Run service."
  value       = google_cloud_run_v2_service.fe_proxy_api.name
}

output "fe_proxy_api_uri" {
  description = "The URI of the deployed FE Proxy API Cloud Run service."
  value       = google_cloud_run_v2_service.fe_proxy_api.uri
}

output "fe_proxy_api_url" {
  description = "The URL of the API Gateway."
  value       = "https://${google_api_gateway_gateway.fe_proxy_api_gw_gateway.default_hostname}"
}
