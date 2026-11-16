#!/usr/bin/env bash
# ParkFlow daily batch job (runs inside the Cloud Run Job container).
#   1) simulate one day of IR sensor events  -> local Parquet (Bronze layout)
#   2) upload that day's files               -> gs://<bucket>/bronze/sensor_events/dt=.../
#   3) dataform run                          -> Silver (incremental MERGE) + DLQ + Gold + assertions (+ BQML)
# If any Dataform assertion fails, `dataform run` exits non-zero, so the job
# fails and the Workflow marks the whole run as failed (bad data never "passes" silently).
set -euo pipefail

PROJECT=${PROJECT:?PROJECT env var is required}
REGION=${REGION:-us-central1}
BUCKET=${BUCKET:-${PROJECT}-lake}
# Default = yesterday in Riyadh time (the day that has just finished).
RUN_DATE=${RUN_DATE:-$(TZ=Asia/Riyadh date -d yesterday +%F)}
RUN_ML=${RUN_ML:-true}
# Different seed per day (reproducible: re-running the same day gives the same data).
SEED=$(echo "$RUN_DATE" | tr -d '-')

echo "== ParkFlow daily run for $RUN_DATE (seed $SEED) =="

OUT=/tmp/bronze
rm -rf "$OUT"
python -m simulator.generate --start "$RUN_DATE" --days 1 --out "$OUT" \
  --seed "$SEED" --file-prefix "d${SEED}"

python - "$OUT" "$BUCKET" <<'PY'
import sys, pathlib
from google.cloud import storage
out, bucket_name = pathlib.Path(sys.argv[1]), sys.argv[2]
bucket = storage.Client().bucket(bucket_name)
n = 0
for f in sorted(out.rglob("*.parquet")):
    blob = bucket.blob(f"bronze/sensor_events/{f.relative_to(out).as_posix()}")
    blob.upload_from_filename(str(f))
    n += 1
print(f"Uploaded {n} Parquet files to gs://{bucket_name}/bronze/sensor_events/")
PY

cd dataform
echo "{\"projectId\": \"$PROJECT\", \"location\": \"$REGION\"}" > .df-credentials.json
TAGS=(--tags silver --tags gold)
[ "$RUN_ML" = "true" ] && TAGS+=(--tags ml)
dataform run "${TAGS[@]}"

echo "== DONE: $RUN_DATE loaded and transformed =="
