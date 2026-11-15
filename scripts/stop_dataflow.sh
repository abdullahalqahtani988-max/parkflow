#!/usr/bin/env bash
# Drain = stop reading new messages, finish in-flight work, then shut down (no data loss).
set -euo pipefail
REGION=${REGION:-us-central1}
for id in $(gcloud dataflow jobs list --region "$REGION" --status=active --format='value(id)'); do
  echo "Draining $id"; gcloud dataflow jobs drain "$id" --region "$REGION"
done
gcloud dataflow jobs list --region "$REGION" --limit 5
