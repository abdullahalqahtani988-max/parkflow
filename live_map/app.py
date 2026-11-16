"""ParkFlow live map (Cloud Run).

Shows current occupancy of every parking lot on a map of Riyadh.

Data source, chosen automatically on every refresh:
  1) Bigtable `spot_status` (live)  - written by the Dataflow streaming job,
     one row per spot (row key "lot#spot"), latest cell = current status.
     Exists only while the live demo runs (cost control).
  2) BigQuery `parkflow_gold.spot_latest_status` (batch) - fallback when
     Bigtable is not running, so the page always shows something meaningful.
Results are cached for CACHE_SEC seconds so many viewers do not multiply reads.
"""
from __future__ import annotations

import csv
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, send_from_directory

PROJECT = os.environ.get("PROJECT", "parkflow-514314")
BT_INSTANCE = os.environ.get("BT_INSTANCE", "parkflow-bt")
BT_TABLE = os.environ.get("BT_TABLE", "spot_status")
CACHE_SEC = int(os.environ.get("CACHE_SEC", "10"))
LOTS_CSV = Path(__file__).parent / "lots.csv"

app = Flask(__name__, static_folder="static")
_cache: dict = {"at": 0.0, "data": None}


def load_lots() -> dict[str, dict]:
    with open(LOTS_CSV, newline="", encoding="utf-8") as f:
        return {r["lot_id"]: {**r, "capacity": int(r["capacity"]),
                              "latitude": float(r["latitude"]), "longitude": float(r["longitude"])}
                for r in csv.DictReader(f)}


def occupied_from_bigtable() -> tuple[Counter, str] | None:
    """Count OCCUPIED spots per lot from the latest cell of each Bigtable row."""
    from google.cloud import bigtable
    from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
    try:
        table = bigtable.Client(project=PROJECT).instance(BT_INSTANCE).table(BT_TABLE)
        counts, latest = Counter(), ""
        for row in table.read_rows(filter_=CellsColumnLimitFilter(1)):   # newest cell only
            cells = row.cells.get("s", {})
            lot_id = row.row_key.decode().split("#", 1)[0]
            status = cells.get(b"status", [None])[0]
            if status is not None and status.value == b"OCCUPIED":
                counts[lot_id] += 1
            ts = cells.get(b"event_ts", [None])[0]
            if ts is not None:
                latest = max(latest, ts.value.decode())
        return counts, latest
    except Exception as e:  # instance not running (outside the demo window) -> fallback
        app.logger.info("Bigtable not available: %s", type(e).__name__)
        return None


def occupied_from_bigquery() -> tuple[Counter, str]:
    from google.cloud import bigquery
    sql = f"""
      SELECT lot_id, COUNTIF(status = 'OCCUPIED') AS occupied, MAX(status_since) AS latest
      FROM `{PROJECT}.parkflow_gold.spot_latest_status` GROUP BY lot_id"""
    rows = list(bigquery.Client(project=PROJECT).query(sql).result())
    latest = max((r.latest for r in rows if r.latest), default=None)
    return Counter({r.lot_id: r.occupied for r in rows}), latest.isoformat() if latest else ""


def snapshot() -> dict:
    now = time.time()
    if _cache["data"] and now - _cache["at"] < CACHE_SEC:
        return _cache["data"]
    live = occupied_from_bigtable()
    if live is not None and sum(live[0].values()) > 0:
        counts, latest, source = live[0], live[1], "live"
    else:
        counts, latest = occupied_from_bigquery()
        source = "batch"
    lots = []
    for lot in load_lots().values():
        occ = min(counts.get(lot["lot_id"], 0), lot["capacity"])
        lots.append({**lot, "occupied": occ, "free": lot["capacity"] - occ,
                     "pct": round(100 * occ / lot["capacity"], 1)})
    data = {"source": source, "data_time": latest,
            "served_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "lots": sorted(lots, key=lambda l: -l["pct"])}
    _cache.update(at=now, data=data)
    return data


@app.get("/api/lots")
def api_lots():
    return jsonify(snapshot())


@app.get("/healthz")
def healthz():
    return "ok"


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
