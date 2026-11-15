#!/usr/bin/env bash
# ParkFlow streaming layer: Pub/Sub topic + 2 subscriptions (fan-out).
#   1) sensor-events-archive : Pub/Sub -> GCS directly (Bronze archive, no code, no Dataflow)
#   2) sensor-events-dataflow: pull subscription read by the Dataflow pipeline
# Pub/Sub costs are per data volume (first 10 GiB/month free); nothing runs per hour here.
set -euo pipefail
PROJECT=${PROJECT:-parkflow-514314}
BUCKET=${BUCKET:-${PROJECT}-lake}
TOPIC=sensor-events
gcloud config set project "$PROJECT" >/dev/null

gcloud services enable pubsub.googleapis.com dataflow.googleapis.com

gcloud pubsub topics describe $TOPIC >/dev/null 2>&1 || gcloud pubsub topics create $TOPIC \
  --message-retention-duration=1d

# Pub/Sub's own service agent must be allowed to write into the bucket.
NUM=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
PUBSUB_SA="service-${NUM}@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$PUBSUB_SA" --role=roles/storage.objectCreator >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$PUBSUB_SA" --role=roles/storage.legacyBucketReader >/dev/null

gcloud pubsub subscriptions describe sensor-events-archive >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create sensor-events-archive --topic=$TOPIC \
    --cloud-storage-bucket="$BUCKET" \
    --cloud-storage-file-prefix="bronze/stream/" \
    --cloud-storage-file-datetime-format="YYYY-MM-DD/hh_mm_ssZ" \
    --cloud-storage-max-duration=5m \
    --cloud-storage-output-format=text

gcloud pubsub subscriptions describe sensor-events-dataflow >/dev/null 2>&1 || \
  gcloud pubsub subscriptions create sensor-events-dataflow --topic=$TOPIC \
    --ack-deadline=60 --message-retention-duration=1d

echo "DONE: Pub/Sub ready."
