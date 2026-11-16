output "lake_bucket" { value = google_storage_bucket.lake.url }
output "orchestrator_sa" { value = google_service_account.orchestrator.email }
output "pubsub_topic" { value = google_pubsub_topic.sensor_events.id }
