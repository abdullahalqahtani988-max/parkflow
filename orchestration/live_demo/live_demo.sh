#!/usr/bin/env bash
# ParkFlow live demo controller (Cloud Run Job, started by Cloud Scheduler).
#   MODE=run  : create Bigtable -> start Dataflow streaming -> publish live sensor
#               events until END_TIME (Riyadh) -> drain Dataflow -> delete Bigtable
#   MODE=stop : safety net, stops anything still running and pauses the schedules
# Paid resources (Dataflow, Bigtable) exist ONLY inside this window; cleanup runs on
# every exit path (trap), and the separate MODE=stop job is a second guarantee.
set -uo pipefail

PROJECT=${PROJECT:?}
REGION=${REGION:-us-central1}
ZONE=${ZONE:-us-central1-b}
BUCKET=${BUCKET:-${PROJECT}-lake}
BT_INSTANCE=${BT_INSTANCE:-parkflow-bt}
END_TIME=${END_TIME:-21:00}          # Riyadh local time to stop publishing
DURATION_MIN=${DURATION_MIN:-}       # optional override (used for short tests)
SPEED=${SPEED:-1}                    # 1 = simulated time equals real time
MODE=${MODE:-run}
TOPIC="projects/$PROJECT/topics/sensor-events"

log() { echo "[$(TZ=Asia/Riyadh date +%H:%M:%S)] $*"; }

cleanup() {
  log "Cleanup: draining Dataflow jobs"
  for id in $(gcloud dataflow jobs list --region "$REGION" --status=active --format='value(id)'); do
    gcloud dataflow jobs drain "$id" --region "$REGION" || gcloud dataflow jobs cancel "$id" --region "$REGION"
  done
  # Wait for drain so Bigtable is not deleted while workers still write to it (max 10 min)
  for _ in $(seq 1 20); do
    [ -z "$(gcloud dataflow jobs list --region "$REGION" --status=active --format='value(id)')" ] && break
    sleep 30
  done
  log "Cleanup: deleting Bigtable instance $BT_INSTANCE"
  gcloud bigtable instances describe "$BT_INSTANCE" >/dev/null 2>&1 && \
    gcloud bigtable instances delete "$BT_INSTANCE" --quiet
  [ -n "${LAUNCHER_PID:-}" ] && kill "$LAUNCHER_PID" 2>/dev/null
  log "Cleanup done"
}

if [ "$MODE" = "stop" ]; then
  cleanup
  for j in parkflow-live-start parkflow-live-stop; do
    gcloud scheduler jobs pause "$j" --location "$REGION" >/dev/null 2>&1 && log "Paused schedule $j"
  done
  exit 0
fi

trap cleanup EXIT

log "1) Bigtable instance + table"
gcloud bigtable instances describe "$BT_INSTANCE" >/dev/null 2>&1 || \
  gcloud bigtable instances create "$BT_INSTANCE" --display-name="ParkFlow live state" \
    --cluster-config="id=${BT_INSTANCE}-c1,zone=$ZONE,nodes=1" || exit 1
gcloud bigtable instances tables describe spot_status --instance="$BT_INSTANCE" >/dev/null 2>&1 || \
  gcloud bigtable instances tables create spot_status --instance="$BT_INSTANCE" --column-families=s || exit 1

log "2) Start Dataflow streaming job"
JOB="parkflow-live-$(date +%Y%m%d-%H%M)"
# The launcher process stays attached to the streaming job; run it in the background.
python3 -m pipelines.streaming_pipeline \
  --runner DataflowRunner --streaming \
  --project "$PROJECT" --region "$REGION" --job_name "$JOB" \
  --temp_location "gs://$BUCKET/tmp" --staging_location "gs://$BUCKET/staging" \
  --input_subscription "projects/$PROJECT/subscriptions/sensor-events-dataflow" \
  --lots_path "gs://$BUCKET/config/lots.csv" --bq_project "$PROJECT" \
  --bigtable_instance "$BT_INSTANCE" \
  --num_workers 1 --max_num_workers 1 --worker_machine_type e2-standard-2 \
  --enable_streaming_engine --setup_file ./setup.py > /tmp/launcher.log 2>&1 &
LAUNCHER_PID=$!

log "   waiting for job $JOB to be Running"
for _ in $(seq 1 40); do   # up to 20 min
  STATE=$(gcloud dataflow jobs list --region "$REGION" --filter="name=$JOB" --format='value(state)' | head -1)
  [ "$STATE" = "Running" ] && break
  if ! kill -0 "$LAUNCHER_PID" 2>/dev/null && [ -z "$STATE" ]; then
    log "Launcher exited before the job started:"; tail -50 /tmp/launcher.log; exit 1
  fi
  sleep 30
done
[ "$STATE" = "Running" ] || { log "Job not running (state=$STATE)"; tail -50 /tmp/launcher.log; exit 1; }
# Give workers a minute to start pulling before the first message
sleep 60

if [ -n "$DURATION_MIN" ]; then
  MINUTES=$DURATION_MIN
else
  NOW=$(date +%s); END=$(TZ=Asia/Riyadh date -d "today $END_TIME" +%s)
  MINUTES=$(( (END - NOW) / 60 ))
fi
[ "$MINUTES" -gt 0 ] || { log "END_TIME already passed"; exit 1; }

log "3) Publishing live sensor events for $MINUTES minutes at ${SPEED}x"
python3 -m simulator.publish --topic "$TOPIC" --minutes "$MINUTES" --speed "$SPEED"

log "4) Window closed; letting the last windows flush (2 min)"
sleep 120
log "Live demo finished; cleanup runs on exit"
