"""ParkFlow streaming pipeline (Apache Beam).

Pub/Sub -> parse -> validate -> dedup -> enrich (side input) ->
    1) BigQuery  parkflow_silver.sensor_events_stream   (valid events, append)
    2) BigQuery  parkflow_ops.dlq_stream                (malformed / invalid, with reason)
    3) BigQuery  parkflow_gold.occupancy_realtime       (1-minute windows per lot, CombinePerKey)
    4) Bigtable  spot_status (optional, --bigtable_instance): latest status per spot

Run locally for free (DirectRunner, JSON-lines file, results to local files):
    python -m pipelines.streaming_pipeline --input_file events.jsonl --local_out out/

Run on Dataflow (streaming):
    python -m pipelines.streaming_pipeline --runner DataflowRunner --streaming \
        --project parkflow-514314 --region us-central1 \
        --input_subscription projects/parkflow-514314/subscriptions/sensor-events-dataflow \
        --temp_location gs://parkflow-514314-lake/tmp ...
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.utils.timestamp import Duration

VALID_STATUS = {"OCCUPIED", "VACANT"}
DLQ = "dlq"

SILVER_STREAM_SCHEMA = (
    "event_id:STRING,event_ts:TIMESTAMP,sent_ts:TIMESTAMP,lot_id:STRING,spot_id:STRING,"
    "sensor_id:STRING,status:STRING,battery_pct:INTEGER,rssi_dbm:INTEGER,firmware:STRING,"
    "lot_type:STRING,delivery_delay_sec:FLOAT,processed_at:TIMESTAMP"
)
DLQ_SCHEMA = "raw_payload:STRING,error_reason:STRING,event_id:STRING,detected_at:TIMESTAMP"
REALTIME_SCHEMA = (
    "lot_id:STRING,window_start:TIMESTAMP,window_end:TIMESTAMP,arrivals:INTEGER,"
    "departures:INTEGER,net_change:INTEGER,events:INTEGER"
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- parse + validate
class ParseAndValidate(beam.DoFn):
    """Bytes -> dict. Anything that is not a valid event goes to the DLQ output with a reason.

    Same rules as the batch view parkflow_staging.bronze_validated, except the lot check,
    which happens after enrichment (it needs the lots side input)."""

    def process(self, payload: bytes):
        text = payload.decode("utf-8", errors="replace")
        try:
            e = json.loads(text)
        except json.JSONDecodeError:
            yield beam.pvalue.TaggedOutput(DLQ, self._dlq(text, "malformed_json"))
            return
        try:
            event_ts = datetime.fromisoformat(e["event_ts"])
            sent_ts = datetime.fromisoformat(e["sent_ts"])
        except (KeyError, TypeError, ValueError):
            yield beam.pvalue.TaggedOutput(DLQ, self._dlq(text, "bad_timestamp", e))
            return

        reason = None
        if not e.get("event_id"):
            reason = "missing_event_id"
        elif not e.get("spot_id"):
            reason = "missing_spot_id"
        elif e.get("status") not in VALID_STATUS:
            reason = "invalid_status"
        elif (event_ts - sent_ts).total_seconds() > 300:
            reason = "event_ts_in_future"
        elif not isinstance(e.get("battery_pct"), int) or not 0 <= e["battery_pct"] <= 100:
            reason = "invalid_battery"
        if reason:
            yield beam.pvalue.TaggedOutput(DLQ, self._dlq(text, reason, e))
            return

        e["delivery_delay_sec"] = (sent_ts - event_ts).total_seconds()
        # Event time (not arrival time) drives the windows -> correct results for late data.
        yield beam.window.TimestampedValue(e, event_ts.timestamp())

    @staticmethod
    def _dlq(raw: str, reason: str, e: dict | None = None) -> dict:
        return {"raw_payload": raw[:10000], "error_reason": reason,
                "event_id": (e or {}).get("event_id"), "detected_at": now_iso()}


class EnrichWithLot(beam.DoFn):
    """Join with the small lots table passed as a side input (broadcast, no shuffle)."""

    def process(self, e: dict, lots: dict):
        lot = lots.get(e["lot_id"])
        if lot is None:
            yield beam.pvalue.TaggedOutput(DLQ, {
                "raw_payload": json.dumps(e, default=str), "error_reason": "unknown_lot",
                "event_id": e["event_id"], "detected_at": now_iso()})
            return
        yield {**e, "lot_type": lot["lot_type"]}


def read_lots(path: str) -> dict:
    """Read config/lots.csv from local disk or GCS."""
    from apache_beam.io.filesystems import FileSystems
    with FileSystems.open(path) as f:
        rows = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
        return {r["lot_id"]: r for r in rows}


# --------------------------------------------------------------------------- aggregation
class LotWindowCounts(beam.CombineFn):
    """Combiner: partial sums on each worker first, so a hot key (Grand Mall) is not a bottleneck."""

    def create_accumulator(self):
        return (0, 0)  # arrivals, departures

    def add_input(self, acc, status):
        a, d = acc
        return (a + 1, d) if status == "OCCUPIED" else (a, d + 1)

    def merge_accumulators(self, accs):
        return tuple(map(sum, zip(*accs)))

    def extract_output(self, acc):
        return acc


class FormatWindow(beam.DoFn):
    def process(self, kv, win=beam.DoFn.WindowParam):
        lot_id, (arrivals, departures) = kv
        yield {
            "lot_id": lot_id,
            "window_start": win.start.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
            "window_end": win.end.to_utc_datetime().replace(tzinfo=timezone.utc).isoformat(),
            "arrivals": arrivals, "departures": departures,
            "net_change": arrivals - departures, "events": arrivals + departures,
        }


def to_silver_row(e: dict) -> dict:
    keys = ["event_id", "event_ts", "sent_ts", "lot_id", "spot_id", "sensor_id", "status",
            "battery_pct", "rssi_dbm", "firmware", "lot_type", "delivery_delay_sec"]
    return {**{k: e.get(k) for k in keys}, "processed_at": now_iso()}


def to_bigtable_row(e: dict):
    """Row key lot#spot keeps one lot's spots together (fast scan per lot); avoids timestamp-first keys."""
    from google.cloud.bigtable import row as bt_row
    r = bt_row.DirectRow(row_key=f"{e['lot_id']}#{e['spot_id']}".encode())
    ts = datetime.fromisoformat(e["event_ts"])
    for col, val in (("status", e["status"]), ("event_ts", e["event_ts"]),
                     ("battery_pct", str(e["battery_pct"]))):
        r.set_cell("s", col.encode(), val.encode(), timestamp=ts)
    return r


# --------------------------------------------------------------------------- pipeline
def run(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input_subscription")
    p.add_argument("--input_file", help="local JSON-lines file (testing)")
    p.add_argument("--lots_path", default="config/lots.csv")
    p.add_argument("--bq_project")
    p.add_argument("--local_out", help="write outputs to local files instead of BigQuery (testing)")
    p.add_argument("--bigtable_instance")
    p.add_argument("--bigtable_table", default="spot_status")
    p.add_argument("--window_sec", type=int, default=60)
    p.add_argument("--dedup_sec", type=int, default=120)
    p.add_argument("--bq_method", choices=["streaming_inserts", "storage_write"], default="streaming_inserts",
                   help="storage_write is cross-language (Java); not supported by the local streaming DirectRunner")
    args, beam_args = p.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    if args.input_subscription:
        options.view_as(StandardOptions).streaming = True
    lots = read_lots(args.lots_path)

    with beam.Pipeline(options=options) as pipe:
        if args.input_subscription:
            # Watermark from EVENT time (attribute set by the publisher), not Pub/Sub publish time.
            raw = pipe | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=args.input_subscription, timestamp_attribute="event_ts")
        else:
            raw = pipe | "ReadFile" >> beam.io.ReadFromText(args.input_file) | beam.Map(str.encode)

        parsed = raw | "ParseValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(DLQ, main="ok")

        # Pub/Sub is at-least-once and devices retry: drop repeated event_ids seen recently.
        # Trade-off: each key's dedup timer HOLDS the watermark for --dedup_sec, so a longer
        # dedup window catches more retries but delays the real-time windows by the same amount.
        # Retries arrive within ~30 s, so 120 s is enough; Silver's MERGE catches anything later.
        unique = (parsed.ok
                  | "KeyByEventId" >> beam.Map(lambda e: (e["event_id"], e))
                  | "Dedup" >> DeduplicatePerKey(processing_time_duration=Duration(seconds=args.dedup_sec))
                  | "DropKey" >> beam.Values())

        lots_si = pipe | "LotsTable" >> beam.Create([lots])
        enriched = unique | "Enrich" >> beam.ParDo(
            EnrichWithLot(), lots=beam.pvalue.AsSingleton(lots_si)).with_outputs(DLQ, main="ok")

        dlq = (parsed[DLQ], enriched[DLQ]) | "AllDLQ" >> beam.Flatten()
        valid = enriched.ok

        per_lot = (valid
                   | "Window1m" >> beam.WindowInto(
                       window.FixedWindows(args.window_sec),
                       allowed_lateness=Duration(seconds=3600),  # late sensors still counted
                       accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING)
                   | "LotStatus" >> beam.Map(lambda e: (e["lot_id"], e["status"]))
                   | "CountPerLot" >> beam.CombinePerKey(LotWindowCounts())
                   | "FormatWindow" >> beam.ParDo(FormatWindow()))

        silver_rows = valid | "ToSilverRow" >> beam.Map(to_silver_row)

        if args.local_out:
            out = args.local_out.rstrip("/")
            silver_rows | "SilverLocal" >> beam.Map(json.dumps) | "W1" >> beam.io.WriteToText(f"{out}/silver")
            dlq | "DlqLocal" >> beam.Map(json.dumps) | "W2" >> beam.io.WriteToText(f"{out}/dlq")
            per_lot | "AggLocal" >> beam.Map(json.dumps) | "W3" >> beam.io.WriteToText(f"{out}/realtime")
        else:
            proj = args.bq_project
            bq = dict(create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            if args.bq_method == "storage_write":
                bq.update(method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                          triggering_frequency=5 if args.input_subscription else None)
            else:
                bq.update(method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)
            silver_rows | "SilverBQ" >> beam.io.WriteToBigQuery(
                f"{proj}:parkflow_silver.sensor_events_stream", schema=SILVER_STREAM_SCHEMA,
                additional_bq_parameters={"timePartitioning": {"type": "DAY", "field": "event_ts"},
                                          "clustering": {"fields": ["lot_id", "spot_id"]}}, **bq)
            dlq | "DlqBQ" >> beam.io.WriteToBigQuery(
                f"{proj}:parkflow_ops.dlq_stream", schema=DLQ_SCHEMA, **bq)
            per_lot | "RealtimeBQ" >> beam.io.WriteToBigQuery(
                f"{proj}:parkflow_gold.occupancy_realtime", schema=REALTIME_SCHEMA, **bq)

        if args.bigtable_instance:
            from apache_beam.io.gcp.bigtableio import WriteToBigTable
            (valid | "ToBigtableRow" >> beam.Map(to_bigtable_row)
                   | "Bigtable" >> WriteToBigTable(project_id=args.bq_project,
                                                   instance_id=args.bigtable_instance,
                                                   table_id=args.bigtable_table))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
