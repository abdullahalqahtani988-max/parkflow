#!/usr/bin/env bash
# ParkFlow batch layer setup. Run from the repo root in Cloud Shell:
#   bash scripts/setup_batch.sh
# Safe to re-run (idempotent): existing resources are kept.
set -euo pipefail

PROJECT=${PROJECT:-parkflow-514314}
REGION=${REGION:-us-central1}
BUCKET=${BUCKET:-${PROJECT}-lake}
START=${START:-2026-10-18}
DAYS=${DAYS:-28}

gcloud config set project "$PROJECT" >/dev/null

echo "== 1) Data lake bucket (regional, private) =="
gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 || \
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention

echo "== 2) Generate Bronze locally ($DAYS days from $START) =="
[ -d .venv ] || python3 -m venv .venv
. .venv/bin/activate
pip install -q -r requirements.txt
rm -rf data/bronze
python -m simulator.generate --start "$START" --days "$DAYS"

echo "== 3) Upload Bronze to GCS =="
gcloud storage rsync -r data/bronze/sensor_events "gs://$BUCKET/bronze/sensor_events"

echo "== 4) BigQuery datasets ($REGION) =="
for ds in parkflow_bronze parkflow_ref parkflow_staging parkflow_silver parkflow_ops parkflow_gold parkflow_assertions; do
  bq --location="$REGION" show "$PROJECT:$ds" >/dev/null 2>&1 || \
    bq --location="$REGION" mk --dataset "$PROJECT:$ds"
done

echo "== 5) Reference table: lots =="
bq load --replace --source_format=CSV --skip_leading_rows=1 "$PROJECT:parkflow_ref.lots" config/lots.csv \
  lot_id:STRING,lot_name:STRING,lot_type:STRING,district:STRING,capacity:INT64,latitude:FLOAT64,longitude:FLOAT64

echo "== 6) Bronze external table =="
bq query --use_legacy_sql=false < sql/bronze_external_table.sql

echo "== 7) Dataform: Silver + DLQ + Gold + assertions =="
cd dataform
echo "{\"projectId\": \"$PROJECT\", \"location\": \"$REGION\"}" > .df-credentials.json
npx -y @dataform/cli@3.1.5 run --tags silver --tags gold

echo "DONE: batch layer is ready."
