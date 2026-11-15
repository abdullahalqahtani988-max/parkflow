"""Unit tests for the streaming pipeline transforms (run locally, no GCP)."""

import json

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipelines.streaming_pipeline import DLQ, LotWindowCounts, ParseAndValidate


def _event(**over):
    e = {"event_id": "e1", "event_ts": "2026-11-15T10:00:00+00:00", "sent_ts": "2026-11-15T10:00:01+00:00",
         "lot_id": "RUH-MALL-01", "spot_id": "RUH-MALL-01-S0001", "sensor_id": "IR-x",
         "status": "OCCUPIED", "battery_pct": 80, "rssi_dbm": -60, "firmware": "1.4.2"}
    e.update(over)
    return json.dumps(e).encode()


def test_parse_routes_bad_records_to_dlq():
    inputs = [_event(), b'{"event_id": "broken', _event(event_id="e2", status="UNKNOWN"),
              _event(event_id="e3", battery_pct=250), _event(event_id="e4", spot_id=None),
              _event(event_id="e5", event_ts="2028-01-01T00:00:00+00:00")]
    with TestPipeline() as p:
        out = p | beam.Create(inputs) | beam.ParDo(ParseAndValidate()).with_outputs(DLQ, main="ok")
        assert_that(out.ok | "ids" >> beam.Map(lambda e: e["event_id"]), equal_to(["e1"]), label="valid")
        assert_that(out[DLQ] | "reasons" >> beam.Map(lambda d: d["error_reason"]),
                    equal_to(["malformed_json", "invalid_status", "invalid_battery",
                              "missing_spot_id", "event_ts_in_future"]), label="dlq")


def test_lot_window_counts_combiner():
    fn = LotWindowCounts()
    a = fn.add_input(fn.add_input(fn.create_accumulator(), "OCCUPIED"), "VACANT")
    b = fn.add_input(fn.create_accumulator(), "OCCUPIED")
    assert fn.extract_output(fn.merge_accumulators([a, b])) == (2, 1)
