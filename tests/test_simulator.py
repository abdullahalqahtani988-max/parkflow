"""Basic tests for the simulator. Run with:  python -m pytest -q"""

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.dataset as ds

from simulator import generate as g

LOTS = g.load_lots(Path("config/lots.csv"))


def _spot(lot_type="mall"):
    return g.Spot("RUH-TEST-01", lot_type, "RUH-TEST-01-S0001", "IR-x", 80, "1.4.2")


def test_spots_match_capacity():
    spots = g.build_spots(LOTS, random.Random(1))
    assert len(spots) == sum(int(l["capacity"]) for l in LOTS)
    assert len({s.spot_id for s in spots}) == len(spots)  # spot_id is unique


def test_spot_events_alternate_and_are_ordered():
    start = datetime(2026, 11, 5, tzinfo=timezone.utc)
    events = g.simulate_spot(_spot(), start - timedelta(days=1), start, start + timedelta(days=2),
                             random.Random(7))
    assert events, "a mall spot should have events over 2 days"
    ts = [e["event_ts"] for e in events]
    assert ts == sorted(ts)
    statuses = [e["status"] for e in events]
    assert all(a != b for a, b in zip(statuses, statuses[1:]))  # OCCUPIED/VACANT alternate


def test_office_is_empty_on_weekend():
    # 2026-11-06 is a Friday (Saudi weekend)
    friday = datetime(2026, 11, 6, tzinfo=g.RIYADH_TZ).astimezone(timezone.utc)
    events = g.simulate_spot(_spot("office"), friday, friday, friday + timedelta(days=1),
                             random.Random(3))
    assert not [e for e in events if e["status"] == "OCCUPIED"]


def test_dirty_data_injection():
    rng = random.Random(11)
    base = [g.make_event(_spot(), datetime(2026, 11, 5, tzinfo=timezone.utc), "OCCUPIED", rng)
            for _ in range(5000)]
    out, stats = g.inject_dirty_data(base, rng, dup_rate=0.02, bad_rate=0.01, late_rate=0.03)
    assert len(out) == len(base) + stats["duplicates"]
    assert len({e["event_id"] for e in out}) == len(base)  # duplicates reuse event_id
    assert stats["late"] > 0 and sum(v for k, v in stats.items() if k.startswith("corrupted")) > 0


def test_write_bronze_layout(tmp_path):
    rng = random.Random(5)
    t0 = datetime(2026, 11, 5, 10, 7, tzinfo=timezone.utc)
    events = [g.make_event(_spot(), t0 + timedelta(minutes=m), "OCCUPIED", rng) for m in range(38)]
    n_files = g.write_bronze(events, tmp_path, file_minutes=15)
    assert n_files == 3  # windows 10:00, 10:15, 10:30 (last event 10:44)
    assert (tmp_path / "dt=2026-11-05" / "part-1000.parquet").exists()
    table = ds.dataset(tmp_path, format="parquet", partitioning="hive").to_table()
    assert table.num_rows == 38
