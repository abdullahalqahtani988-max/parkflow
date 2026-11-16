"""Live map API: source fallback and occupancy maths (cloud clients mocked)."""
from collections import Counter
import importlib.util
from pathlib import Path

import pytest

flask = pytest.importorskip("flask")
spec = importlib.util.spec_from_file_location("live_map_app", Path(__file__).parents[1] / "live_map" / "app.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def _client(monkeypatch, bigtable_result):
    m._cache.update(at=0.0, data=None)
    monkeypatch.setattr(m, "occupied_from_bigtable", lambda: bigtable_result)
    monkeypatch.setattr(m, "occupied_from_bigquery",
                        lambda: (Counter({"RUH-MALL-01": 600}), "2026-11-16T17:00:00+00:00"))
    return m.app.test_client()


def test_falls_back_to_bigquery_when_bigtable_is_off(monkeypatch):
    d = _client(monkeypatch, None).get("/api/lots").get_json()
    assert d["source"] == "batch"
    mall = next(l for l in d["lots"] if l["lot_id"] == "RUH-MALL-01")
    assert (mall["occupied"], mall["free"], mall["pct"]) == (600, 600, 50.0)


def test_uses_bigtable_when_streaming(monkeypatch):
    live = (Counter({"RUH-HOSP-01": 400, "RUH-MALL-01": 5000}), "2026-11-18T15:30:00+00:00")
    d = _client(monkeypatch, live).get("/api/lots").get_json()
    assert d["source"] == "live"
    by_id = {l["lot_id"]: l for l in d["lots"]}
    assert by_id["RUH-HOSP-01"]["pct"] == 100.0
    assert by_id["RUH-MALL-01"]["occupied"] == 1200   # capped at capacity
    assert d["lots"][0]["pct"] >= d["lots"][-1]["pct"]  # sorted by occupancy
