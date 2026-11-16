# ParkFlow platform infrastructure (Infrastructure as Code).
#
# Scope = the long-lived platform: storage, BigQuery, Pub/Sub, registry,
# service accounts + IAM, Cloud Scheduler triggers.
# Not here, on purpose:
#   - Cloud Run job/service *deployments* and the Workflows *definition*: they are
#     application code that changes on every release, deployed by scripts/CI
#     (gcloud run deploy / gcloud workflows deploy). The google provider also cannot
#     import an existing workflow. Terraform manages the schedule that triggers it.
#   - Bigtable + Dataflow: ephemeral, created/destroyed by the live demo job (cost).
# Resources first created with gcloud are adopted with `import` blocks (imports.tf).

data "google_project" "this" {}

locals {
  bucket        = "${var.project}-lake"
  orchestrator  = google_service_account.orchestrator.email
  map_sa        = google_service_account.live_map.email
  pubsub_agent  = "service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
  scheduler_uri = "https://run.googleapis.com/v2/projects/${var.project}/locations/${var.region}/jobs"
}

# ----------------------------------------------------------------- data lake
resource "google_storage_bucket" "lake" {
  name                        = local.bucket
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

resource "google_bigquery_dataset" "layer" {
  for_each   = toset(var.datasets)
  dataset_id = each.value
  location   = var.region
}

# ----------------------------------------------------------------- streaming
resource "google_pubsub_topic" "sensor_events" {
  name                       = "sensor-events"
  message_retention_duration = "86400s"
}

# Fan-out 1: raw archive straight to GCS (Bronze), no code, no workers
resource "google_pubsub_subscription" "archive" {
  name  = "sensor-events-archive"
  topic = google_pubsub_topic.sensor_events.id
  cloud_storage_config {
    bucket                   = google_storage_bucket.lake.name
    filename_prefix          = "bronze/stream/"
    filename_datetime_format = "YYYY-MM-DD/hh_mm_ssZ"
    max_duration             = "300s"
  }
  depends_on = [google_storage_bucket_iam_member.pubsub_writer, google_storage_bucket_iam_member.pubsub_reader]
}

# Fan-out 2: pull subscription read by Dataflow
resource "google_pubsub_subscription" "dataflow" {
  name                       = "sensor-events-dataflow"
  topic                      = google_pubsub_topic.sensor_events.id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
}

resource "google_storage_bucket_iam_member" "pubsub_writer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.pubsub_agent}"
}

resource "google_storage_bucket_iam_member" "pubsub_reader" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${local.pubsub_agent}"
}

# ----------------------------------------------------------------- registry
resource "google_artifact_registry_repository" "images" {
  repository_id = "parkflow"
  location      = var.region
  format        = "DOCKER"
}

# ----------------------------------------------------------------- identities (least privilege)
resource "google_service_account" "orchestrator" {
  account_id   = "parkflow-orchestrator"
  display_name = "ParkFlow orchestrator"
}

resource "google_service_account" "live_map" {
  account_id   = "parkflow-map"
  display_name = "ParkFlow live map (read-only)"
}

resource "google_project_iam_member" "orchestrator" {
  for_each = toset([
    "roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/run.developer",
    "roles/workflows.invoker", "roles/logging.logWriter", "roles/iam.serviceAccountUser",
    # live demo controller: creates/deletes Bigtable, starts/stops Dataflow, publishes
    "roles/dataflow.admin", "roles/bigtable.admin", "roles/pubsub.publisher", "roles/cloudscheduler.admin",
  ])
  project = var.project
  role    = each.value
  member  = "serviceAccount:${local.orchestrator}"
}

resource "google_storage_bucket_iam_member" "orchestrator_lake" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.orchestrator}"
}

resource "google_project_iam_member" "live_map" {
  for_each = toset(["roles/bigtable.reader", "roles/bigquery.dataViewer", "roles/bigquery.jobUser"])
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${local.map_sa}"
}

# ----------------------------------------------------------------- orchestration
resource "google_cloud_scheduler_job" "daily" {
  name      = "parkflow-daily"
  region    = var.region
  schedule  = "0 3 * * *"
  time_zone = "Asia/Riyadh"
  # Explicit = the API defaults: no automatic re-run of a missed trigger
  # (the daily workflow retries its own steps; the live demo has its own safety-stop job).
  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }
  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project}/locations/${var.region}/workflows/parkflow-daily/executions"
    body        = base64encode("{\"argument\":\"{}\"}")
    oauth_token {
      service_account_email = local.orchestrator
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}

# Live demo window for the presentation (18 Nov, Riyadh time)
resource "google_cloud_scheduler_job" "live" {
  for_each = {
    "parkflow-live-start" = { schedule = "50 17 18 11 *", job = "parkflow-live-demo" }
    "parkflow-live-stop"  = { schedule = "20 21 18 11 *", job = "parkflow-live-stop" }
  }
  name      = each.key
  region    = var.region
  schedule  = each.value.schedule
  time_zone = "Asia/Riyadh"
  # Explicit = the API defaults: no automatic re-run of a missed trigger
  # (the daily workflow retries its own steps; the live demo has its own safety-stop job).
  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }
  http_target {
    http_method = "POST"
    uri         = "${local.scheduler_uri}/${each.value.job}:run"
    oauth_token {
      service_account_email = local.orchestrator
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }
}
