# Adopt resources that were first created with gcloud (scripts/*.sh).
# After `terraform apply` these blocks are no-ops and can stay as documentation.
import {
  to = google_storage_bucket.lake
  id = "parkflow-514314-lake"
}
import {
  to = google_bigquery_dataset.layer["parkflow_bronze"]
  id = "projects/parkflow-514314/datasets/parkflow_bronze"
}
import {
  to = google_bigquery_dataset.layer["parkflow_ref"]
  id = "projects/parkflow-514314/datasets/parkflow_ref"
}
import {
  to = google_bigquery_dataset.layer["parkflow_staging"]
  id = "projects/parkflow-514314/datasets/parkflow_staging"
}
import {
  to = google_bigquery_dataset.layer["parkflow_silver"]
  id = "projects/parkflow-514314/datasets/parkflow_silver"
}
import {
  to = google_bigquery_dataset.layer["parkflow_ops"]
  id = "projects/parkflow-514314/datasets/parkflow_ops"
}
import {
  to = google_bigquery_dataset.layer["parkflow_gold"]
  id = "projects/parkflow-514314/datasets/parkflow_gold"
}
import {
  to = google_bigquery_dataset.layer["parkflow_assertions"]
  id = "projects/parkflow-514314/datasets/parkflow_assertions"
}
import {
  to = google_pubsub_topic.sensor_events
  id = "projects/parkflow-514314/topics/sensor-events"
}
import {
  to = google_pubsub_subscription.archive
  id = "projects/parkflow-514314/subscriptions/sensor-events-archive"
}
import {
  to = google_pubsub_subscription.dataflow
  id = "projects/parkflow-514314/subscriptions/sensor-events-dataflow"
}
import {
  to = google_artifact_registry_repository.images
  id = "projects/parkflow-514314/locations/us-central1/repositories/parkflow"
}
import {
  to = google_service_account.orchestrator
  id = "projects/parkflow-514314/serviceAccounts/parkflow-orchestrator@parkflow-514314.iam.gserviceaccount.com"
}
import {
  to = google_service_account.live_map
  id = "projects/parkflow-514314/serviceAccounts/parkflow-map@parkflow-514314.iam.gserviceaccount.com"
}
import {
  to = google_cloud_scheduler_job.daily
  id = "projects/parkflow-514314/locations/us-central1/jobs/parkflow-daily"
}
import {
  to = google_cloud_scheduler_job.live["parkflow-live-start"]
  id = "projects/parkflow-514314/locations/us-central1/jobs/parkflow-live-start"
}
import {
  to = google_cloud_scheduler_job.live["parkflow-live-stop"]
  id = "projects/parkflow-514314/locations/us-central1/jobs/parkflow-live-stop"
}
