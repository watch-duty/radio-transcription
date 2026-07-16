# =============================================================================
# NETWORK MODULE
# =============================================================================

# Primary VPC network for the radio transcription system
resource "google_compute_network" "main" {
  name                    = "radio-transcription-vpc-${var.environment}"
  project                 = var.project_id
  auto_create_subnetworks = false
}

# Subnetwork for Cloud Run and internal service communication
resource "google_compute_subnetwork" "main" {
  name                     = "radio-transcription-subnet-${var.environment}"
  project                  = var.project_id
  region                   = var.region
  network                  = google_compute_network.main.id
  ip_cidr_range            = var.subnet_ip_cidr_range
  private_ip_google_access = true
}

# Reserved IP range for internal service peering (required by AlloyDB)
resource "google_compute_global_address" "private_ip_alloc" {
  name          = "alloydb-psa-range-${var.environment}"
  project       = var.project_id
  address_type  = "INTERNAL"
  purpose       = "VPC_PEERING"
  prefix_length = 16
  network       = google_compute_network.main.id
}

# Private Service Access connection to enable VPC peering with Google managed services
resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_alloc.name]
}

# Cloud NAT removed 2026-04-13 for collectors, but re-enabled here to allow
# Dataflow private workers (`--disable-public-ips`) to pull container images from Artifact Registry.
resource "google_compute_router" "nat_router" {
  name    = "radio-transcription-router-${var.environment}"
  network = google_compute_network.main.id
  region  = var.region
  project = var.project_id
}

resource "google_compute_router_nat" "nat" {
  name                               = "radio-transcription-nat-${var.environment}"
  router                             = google_compute_router.nat_router.name
  region                             = var.region
  project                            = var.project_id
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}
