#!/usr/bin/env bash
# Deploys the ParkFlow live map to Cloud Run (public URL, scales to zero = $0 when idle).
#   bash scripts/deploy_live_map.sh
set -euo pipefail
PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
SA_NAME=parkflow-map
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT" >/dev/null
gcloud services enable run.googleapis.com cloudbuild.googleapis.com bigtable.googleapis.com bigtableadmin.googleapis.com

echo "== Read-only service account =="
gcloud iam service-accounts describe "$SA" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_NAME" --display-name="ParkFlow live map (read-only)"
# A new service account takes a few seconds to become visible to IAM
for _ in $(seq 1 12); do gcloud iam service-accounts describe "$SA" >/dev/null 2>&1 && break; sleep 5; done
sleep 10
for role in roles/bigtable.reader roles/bigquery.dataViewer roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" \
    --role="$role" --condition=None --quiet >/dev/null
done

echo "== Build image (Cloud Build) into the project registry =="
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/parkflow/live-map:latest"
cp config/lots.csv live_map/lots.csv
gcloud builds submit live_map --tag "$IMAGE"

echo "== Deploy to Cloud Run =="
gcloud run deploy parkflow-live-map --image "$IMAGE" --region "$REGION" \
  --service-account "$SA" --allow-unauthenticated \
  --min-instances 0 --max-instances 2 --cpu 1 --memory 512Mi \
  --set-env-vars "PROJECT=$PROJECT,BT_INSTANCE=parkflow-bt,BT_TABLE=spot_status" --quiet

gcloud run services describe parkflow-live-map --region "$REGION" --format='value(status.url)'
