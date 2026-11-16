#!/usr/bin/env bash
# Schedules the live demo window: Dataflow + Bigtable + live sensor stream.
# Run AFTER scripts/setup_orchestration.sh (reuses its service account and registry):
#   bash scripts/setup_live_demo.sh
# Defaults = presentation day: start 17:50, stop publishing 21:00, safety stop 21:20 (Riyadh).
# Short end-to-end test (about 15 min, ~$0.3):
#   gcloud run jobs execute parkflow-live-demo --region us-central1 --update-env-vars DURATION_MIN=5
set -euo pipefail

PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
BUCKET=${BUCKET:-${PROJECT}-lake}
SA="parkflow-orchestrator@${PROJECT}.iam.gserviceaccount.com"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/parkflow/live-demo:latest"
START_CRON=${START_CRON:-"50 17 18 11 *"}   # Nov 18, 17:50
STOP_CRON=${STOP_CRON:-"20 21 18 11 *"}     # Nov 18, 21:20 (safety net)
END_TIME=${END_TIME:-21:00}

gcloud config set project "$PROJECT" >/dev/null

echo "== 1) Extra roles for the controller =="
for role in roles/dataflow.admin roles/bigtable.admin roles/pubsub.publisher roles/cloudscheduler.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$SA" \
    --role="$role" --condition=None --quiet >/dev/null
done
gcloud storage cp config/lots.csv "gs://$BUCKET/config/lots.csv"

echo "== 2) Build the controller image =="
gcloud builds submit --config orchestration/cloudbuild.yaml \
  --substitutions=_DOCKERFILE=orchestration/live_demo/Dockerfile,_IMAGE="$IMAGE" .

echo "== 3) Cloud Run Jobs (run + stop) =="
ENV="PROJECT=$PROJECT,REGION=$REGION,BUCKET=$BUCKET,END_TIME=$END_TIME"
gcloud run jobs deploy parkflow-live-demo --image="$IMAGE" --region="$REGION" \
  --service-account="$SA" --cpu=2 --memory=4Gi --task-timeout=4h --max-retries=0 \
  --set-env-vars="$ENV,MODE=run"
gcloud run jobs deploy parkflow-live-stop --image="$IMAGE" --region="$REGION" \
  --service-account="$SA" --cpu=1 --memory=2Gi --task-timeout=30m --max-retries=1 \
  --set-env-vars="$ENV,MODE=stop"

echo "== 4) Schedules (Asia/Riyadh) =="
for pair in "parkflow-live-start|$START_CRON|parkflow-live-demo" "parkflow-live-stop|$STOP_CRON|parkflow-live-stop"; do
  IFS='|' read -r NAME CRON JOB <<< "$pair"
  URI="https://run.googleapis.com/v2/projects/$PROJECT/locations/$REGION/jobs/$JOB:run"
  ACTION=create
  gcloud scheduler jobs describe "$NAME" --location="$REGION" >/dev/null 2>&1 && ACTION=update
  gcloud scheduler jobs $ACTION http "$NAME" --location="$REGION" --schedule="$CRON" \
    --time-zone="Asia/Riyadh" --uri="$URI" --http-method=POST \
    --oauth-service-account-email="$SA"
done
gcloud scheduler jobs list --location="$REGION"
