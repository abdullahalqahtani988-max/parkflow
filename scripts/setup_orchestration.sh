#!/usr/bin/env bash
# ParkFlow orchestration: Cloud Scheduler -> Workflows -> Cloud Run Job.
# Run from the repo root in Cloud Shell:  bash scripts/setup_orchestration.sh
# Safe to re-run. Cost: all four services stay inside their free tiers at
# one run per day (see docs/decisions.md).
set -euo pipefail

PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
BUCKET=${BUCKET:-${PROJECT}-lake}
SA_NAME=parkflow-orchestrator
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/parkflow/daily-job:latest"
SCHEDULE=${SCHEDULE:-"0 3 * * *"}   # 03:00 Riyadh time, after the day has closed

gcloud config set project "$PROJECT" >/dev/null

echo "== 1) Enable APIs =="
gcloud services enable run.googleapis.com workflows.googleapis.com \
  cloudscheduler.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com

echo "== 2) Service account (least privilege) =="
gcloud iam service-accounts describe "$SA" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_NAME" --display-name="ParkFlow orchestrator"
for role in roles/bigquery.dataEditor roles/bigquery.jobUser roles/run.developer \
            roles/workflows.invoker roles/logging.logWriter roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" \
    --role="$role" --condition=None --quiet >/dev/null
done
# Storage access only on the data-lake bucket, not the whole project
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:$SA" --role=roles/storage.objectAdmin >/dev/null

echo "== 3) Artifact Registry + image build (Cloud Build) =="
gcloud artifacts repositories describe parkflow --location="$REGION" >/dev/null 2>&1 || \
  gcloud artifacts repositories create parkflow --repository-format=docker --location="$REGION"
gcloud builds submit --config orchestration/cloudbuild.yaml \
  --substitutions=_DOCKERFILE=orchestration/daily_job/Dockerfile,_IMAGE="$IMAGE" .

echo "== 4) Cloud Run Job =="
gcloud run jobs deploy parkflow-daily --image="$IMAGE" --region="$REGION" \
  --service-account="$SA" --cpu=1 --memory=2Gi --task-timeout=1800 --max-retries=0 \
  --set-env-vars="PROJECT=$PROJECT,REGION=$REGION,BUCKET=$BUCKET"

echo "== 5) Workflow =="
gcloud workflows deploy parkflow-daily --location="$REGION" \
  --source=orchestration/workflow.yaml --service-account="$SA"

echo "== 6) Cloud Scheduler (daily) =="
URI="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT/locations/$REGION/workflows/parkflow-daily/executions"
if gcloud scheduler jobs describe parkflow-daily --location="$REGION" >/dev/null 2>&1; then
  gcloud scheduler jobs update http parkflow-daily --location="$REGION" --schedule="$SCHEDULE" \
    --time-zone="Asia/Riyadh" --uri="$URI" --http-method=POST --message-body='{"argument":"{}"}' \
    --oauth-service-account-email="$SA"
else
  gcloud scheduler jobs create http parkflow-daily --location="$REGION" --schedule="$SCHEDULE" \
    --time-zone="Asia/Riyadh" --uri="$URI" --http-method=POST --message-body='{"argument":"{}"}' \
    --oauth-service-account-email="$SA"
fi

echo "DONE. Test now with:  gcloud workflows run parkflow-daily --location=$REGION --data='{\"run_date\":\"2026-11-15\"}'"
