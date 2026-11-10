"""ParkFlow sensor simulator.

Generates realistic IR parking-sensor events for fictional Riyadh parking lots
and writes them as Parquet files in a Hive-style Bronze layout:

    <out>/dt=YYYY-MM-DD/part-HHMM.parquet

Each spot is simulated as a two-state machine (vacant <-> occupied):
- Arrivals follow a non-homogeneous Poisson process (rate changes by hour,
  weekday/weekend and lot type), sampled with the "thinning" method.
- Parking durations follow a log-normal distribution per lot type.

On purpose, the output also contains dirty data for testing the pipeline:
- duplicates   : same event re-sent with the same event_id (at-least-once retry)
- corrupted    : semantically invalid records (for the Dead Letter Queue)
- late arrivals: event_ts much earlier than sent_ts (sensor was offline)

Usage:
    python -m simulator.generate --start 2026-11-01 --days 7
"""

from __future__ import annotations

import argparse
import csv
import math
import random
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RIYADH_TZ = timezone(timedelta(hours=3))  # Saudi Arabia has no DST
SAUDI_WEEKEND = {4, 5}  # Python weekday(): Friday=4, Saturday=5

# ---------------------------------------------------------------------------
# Behaviour profiles per lot type
# ---------------------------------------------------------------------------
# peak_rate: arrivals per VACANT spot per minute at multiplier 1.0.
#   Derived from target occupancy p at peak:  rate = p / (median_minutes * (1 - p))
# median_minutes / sigma: log-normal parking duration.
# hourly: 24 multipliers (local Riyadh hour 0..23) for weekdays and weekends.
PROFILES = {
    "mall": {
        "peak_rate": 0.10, "median_minutes": 90, "sigma": 0.6,
        # Riyadh malls: quiet mornings, busy evenings, closed ~01:00-09:00.
        "weekday": [0.08, 0.02, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.01, 0.03, 0.05,
                    0.07, 0.08, 0.08, 0.10, 0.15, 0.30, 0.50, 0.80, 1.00, 1.00, 0.60, 0.25],
        "weekend": [0.15, 0.04, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.01, 0.04, 0.08,
                    0.12, 0.15, 0.20, 0.30, 0.50, 0.80, 1.00, 1.00, 1.00, 1.00, 0.80, 0.40],
    },
    "hospital": {
        "peak_rate": 0.047, "median_minutes": 120, "sigma": 0.7,
        "weekday": [0.05, 0.03, 0.03, 0.03, 0.03, 0.05, 0.20, 0.60, 1.00, 1.00, 0.95, 0.90,
                    0.80, 0.70, 0.60, 0.50, 0.50, 0.45, 0.40, 0.35, 0.30, 0.20, 0.10, 0.08],
        "weekend": [0.05, 0.03, 0.03, 0.03, 0.03, 0.05, 0.10, 0.20, 0.35, 0.40, 0.40, 0.40,
                    0.35, 0.35, 0.35, 0.35, 0.35, 0.35, 0.30, 0.30, 0.25, 0.15, 0.10, 0.08],
    },
    "university": {
        "peak_rate": 0.0375, "median_minutes": 240, "sigma": 0.5,
        "weekday": [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.10, 0.80, 1.00, 0.70, 0.50, 0.40,
                    0.30, 0.20, 0.10, 0.05, 0.05, 0.03, 0.02, 0.01, 0.01, 0.00, 0.00, 0.00],
        "weekend": [0.00] * 8 + [0.02] * 8 + [0.00] * 8,
    },
    "office": {
        "peak_rate": 0.019, "median_minutes": 480, "sigma": 0.3,
        "weekday": [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.20, 1.00, 1.00, 0.50, 0.20, 0.10,
                    0.10, 0.10, 0.05, 0.03, 0.02, 0.01, 0.01, 0.00, 0.00, 0.00, 0.00, 0.00],
        "weekend": [0.00] * 24,
    },
}

# Pyarrow schema for Bronze. Every column is nullable on purpose:
# Bronze stores data "as received", validation happens later (Silver).
BRONZE_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_ts", pa.timestamp("us", tz="UTC")),   # when the sensor detected the change
    ("sent_ts", pa.timestamp("us", tz="UTC")),    # when the device sent it (ingestion side)
    ("lot_id", pa.string()),
    ("spot_id", pa.string()),
    ("sensor_id", pa.string()),
    ("status", pa.string()),                      # "OCCUPIED" | "VACANT"
    ("battery_pct", pa.int32()),
    ("rssi_dbm", pa.int32()),
    ("firmware", pa.string()),
])


@dataclass
class Spot:
    lot_id: str
    lot_type: str
    spot_id: str
    sensor_id: str
    battery_pct: int
    firmware: str


# ---------------------------------------------------------------------------
# Loading static data
# ---------------------------------------------------------------------------
def load_lots(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_spots(lots: list[dict], rng: random.Random) -> list[Spot]:
    spots = []
    for lot in lots:
        for n in range(1, int(lot["capacity"]) + 1):
            spot_id = f"{lot['lot_id']}-S{n:04d}"
            spots.append(Spot(
                lot_id=lot["lot_id"],
                lot_type=lot["lot_type"],
                spot_id=spot_id,
                sensor_id=f"IR-{spot_id}",
                battery_pct=rng.randint(15, 100),
                firmware=rng.choice(["1.4.2", "1.4.2", "1.5.0"]),
            ))
    return spots


# ---------------------------------------------------------------------------
# Simulation of one spot
# ---------------------------------------------------------------------------
def arrival_rate(profile: dict, t_utc: datetime) -> float:
    """Arrivals per vacant spot per minute at time t (depends on local hour/day)."""
    local = t_utc.astimezone(RIYADH_TZ)
    day_kind = "weekend" if local.weekday() in SAUDI_WEEKEND else "weekday"
    return profile["peak_rate"] * profile[day_kind][local.hour]


def next_arrival(profile: dict, t: datetime, end: datetime, rng: random.Random) -> datetime | None:
    """Thinning: sample candidates at the max rate, accept with rate(t)/max_rate."""
    max_rate = profile["peak_rate"]  # multipliers are <= 1.0
    while t < end:
        t = t + timedelta(minutes=rng.expovariate(max_rate))
        if t < end and rng.random() < arrival_rate(profile, t) / max_rate:
            return t
    return None


def simulate_spot(spot: Spot, warmup_start: datetime, start: datetime, end: datetime,
                  rng: random.Random) -> list[dict]:
    profile = PROFILES[spot.lot_type]
    mu = math.log(profile["median_minutes"])
    events = []
    t = warmup_start  # warm-up so the lot is not artificially empty at `start`
    while True:
        arrive = next_arrival(profile, t, end, rng)
        if arrive is None:
            break
        stay = timedelta(minutes=rng.lognormvariate(mu, profile["sigma"]))
        leave = arrive + stay
        for ts, status in ((arrive, "OCCUPIED"), (leave, "VACANT")):
            if start <= ts < end:
                events.append(make_event(spot, ts, status, rng))
        t = leave
    return events


def make_event(spot: Spot, ts: datetime, status: str, rng: random.Random) -> dict:
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "event_ts": ts,
        "sent_ts": ts + timedelta(seconds=rng.uniform(0.2, 3.0)),  # normal network delay
        "lot_id": spot.lot_id,
        "spot_id": spot.spot_id,
        "sensor_id": spot.sensor_id,
        "status": status,
        "battery_pct": spot.battery_pct,
        "rssi_dbm": rng.randint(-90, -40),
        "firmware": spot.firmware,
    }


# ---------------------------------------------------------------------------
# Dirty data injection (for testing Dedup, DLQ and late data)
# ---------------------------------------------------------------------------
CORRUPTIONS = ["null_spot_id", "bad_status", "future_ts", "unknown_lot", "bad_battery"]


def corrupt(event: dict, rng: random.Random) -> str:
    kind = rng.choice(CORRUPTIONS)
    if kind == "null_spot_id":
        event["spot_id"] = None
    elif kind == "bad_status":
        event["status"] = rng.choice(["UNKNOWN", "occupied?", ""])
    elif kind == "future_ts":
        event["event_ts"] = event["event_ts"] + timedelta(days=rng.randint(365, 900))
    elif kind == "unknown_lot":
        event["lot_id"] = "RUH-XXXX-99"
    elif kind == "bad_battery":
        event["battery_pct"] = rng.choice([-5, 250])
    return kind


def inject_dirty_data(events: list[dict], rng: random.Random, dup_rate: float,
                      bad_rate: float, late_rate: float) -> tuple[list[dict], Counter]:
    stats = Counter()
    out = []
    for e in events:
        r = rng.random()
        if r < bad_rate:
            stats["corrupted:" + corrupt(e, rng)] += 1
        elif r < bad_rate + late_rate:
            # Sensor lost connectivity and sent the buffered event later.
            e["sent_ts"] = e["event_ts"] + timedelta(minutes=rng.uniform(5, 120))
            stats["late"] += 1
        out.append(e)
        if rng.random() < dup_rate:
            # Retry after a lost ACK: identical payload & event_id, later sent_ts.
            dup = dict(e)
            dup["sent_ts"] = e["sent_ts"] + timedelta(seconds=rng.uniform(1, 30))
            out.append(dup)
            stats["duplicates"] += 1
    return out, stats


# ---------------------------------------------------------------------------
# Writing Bronze Parquet files
# ---------------------------------------------------------------------------
def write_bronze(events: list[dict], out_dir: Path, file_minutes: int) -> int:
    """Group by arrival time (sent_ts, UTC) into one file per `file_minutes` window."""
    buckets = defaultdict(list)
    for e in events:
        s = e["sent_ts"]
        window = s.replace(minute=(s.minute // file_minutes) * file_minutes, second=0, microsecond=0)
        buckets[window].append(e)

    for window, rows in sorted(buckets.items()):
        rows.sort(key=lambda r: r["sent_ts"])
        part_dir = out_dir / f"dt={window:%Y-%m-%d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
        pq.write_table(table, part_dir / f"part-{window:%H%M}.parquet", compression="snappy")
    return len(buckets)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ParkFlow IR sensor simulator")
    p.add_argument("--start", default="2026-11-01", help="first day (Riyadh local date)")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--lots", default="config/lots.csv")
    p.add_argument("--out", default="data/bronze/sensor_events")
    p.add_argument("--file-minutes", type=int, default=15)
    p.add_argument("--dup-rate", type=float, default=0.01)
    p.add_argument("--bad-rate", type=float, default=0.005)
    p.add_argument("--late-rate", type=float, default=0.02)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)  # same seed -> same data (reproducible tests)

    start = datetime.fromisoformat(args.start).replace(tzinfo=RIYADH_TZ).astimezone(timezone.utc)
    end = start + timedelta(days=args.days)
    warmup_start = start - timedelta(days=1)

    lots = load_lots(Path(args.lots))
    spots = build_spots(lots, rng)

    events = []
    for spot in spots:
        events.extend(simulate_spot(spot, warmup_start, start, end, rng))
    clean_count = len(events)

    events, stats = inject_dirty_data(events, rng, args.dup_rate, args.bad_rate, args.late_rate)
    n_files = write_bronze(events, Path(args.out), args.file_minutes)

    per_lot = Counter(e["lot_id"] for e in events)
    print(f"Spots: {len(spots)} | clean events: {clean_count} | written rows: {len(events)} | files: {n_files}")
    print("Rows per lot:", dict(per_lot.most_common()))
    print("Dirty data:", dict(sorted(stats.items())))


if __name__ == "__main__":
    main()
