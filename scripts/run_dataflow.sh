#!/usr/bin/env bash
# Starts the streaming Dataflow job. COSTS MONEY WHILE RUNNING (~$0.3/hour with 1 small worker).
# Stop it right after the test:  bash scripts/stop_dataflow.sh
set -euo pipefail
PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
BUCKET=${BUCKET:-${PROJECT}-lake}
EXTRA=${EXTRA:-}   # e.g. EXTRA="--bigtable_instance parkflow-bt"

gcloud storage cp config/lots.csv "gs://$BUCKET/config/lots.csv"
. .venv/bin/activate
pip install -q setuptools wheel -r pipelines/requirements.txt   # setuptools: needed to package setup.py on Python 3.12

python -m pipelines.streaming_pipeline \
  --runner DataflowRunner --streaming \
  --project "$PROJECT" --region "$REGION" --job_name parkflow-streaming \
  --temp_location "gs://$BUCKET/tmp" --staging_location "gs://$BUCKET/staging" \
  --input_subscription "projects/$PROJECT/subscriptions/sensor-events-dataflow" \
  --lots_path "gs://$BUCKET/config/lots.csv" --bq_project "$PROJECT" \
  --num_workers 1 --max_num_workers 1 --worker_machine_type e2-standard-2 \
  --enable_streaming_engine --setup_file ./setup.py $EXTRA
