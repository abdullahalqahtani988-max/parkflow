"""ParkFlow live publisher: replays simulated sensor events to Pub/Sub in (accelerated) real time.

The simulation clock starts at the current Riyadh time, so the lots have a realistic
occupancy for "now". With --speed 60, one simulated hour plays in one real minute.
All timestamps sent are mapped to wall-clock time, so Dataflow sees a live stream.

Dirty data for the streaming path (on top of the simulator's duplicates/corrupt/late):
- malformed JSON payloads (cannot be parsed -> Dataflow DLQ)

Usage:
    python -m simulator.publish --topic projects/<PROJECT>/topics/sensor-events --minutes 10 --speed 60
    python -m simulator.publish --out events.jsonl --minutes 5 --speed 600   # local file, no GCP
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from simulator import generate as g


def build_events(sim_minutes: float, speed: float, seed: int, lots_path: str,
                 malformed_rate: float) -> list[tuple[float, bytes, dict]]:
    """Return (seconds_from_start, payload, attributes) sorted by send time."""
    rng = random.Random(seed)
    wall_start = datetime.now(timezone.utc)
    sim_start = wall_start  # simulate "today" so occupancy matches the real hour
    sim_end = sim_start + timedelta(minutes=sim_minutes * speed)

    spots = g.build_spots(g.load_lots(Path(lots_path)), rng)
    events = []
    for spot in spots:
        events.extend(g.simulate_spot(spot, sim_start - timedelta(days=1), sim_start, sim_end, rng))
    events, _ = g.inject_dirty_data(events, rng, dup_rate=0.01, bad_rate=0.005, late_rate=0.02)

    def to_wall(ts: datetime) -> datetime:
        return wall_start + (ts - sim_start) / speed

    out = []
    for e in events:
        sent_wall = to_wall(e["sent_ts"])
        record = {**e, "event_ts": to_wall(e["event_ts"]).isoformat(), "sent_ts": sent_wall.isoformat()}
        payload = json.dumps(record).encode()
        if rng.random() < malformed_rate:
            payload = payload[: len(payload) // 2]  # truncated JSON, like a broken network packet
        # event_ts also travels as a Pub/Sub attribute (RFC 3339) so Dataflow can build its
        # watermark from EVENT time (ReadFromPubSub(timestamp_attribute="event_ts")).
        attrs = {"event_ts": to_wall(e["event_ts"]).strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
        out.append(((sent_wall - wall_start).total_seconds(), payload, attrs))
    out.sort(key=lambda x: x[0])
    # Late events can be *sent* up to 2 h after they happened. Without this cut the
    # publisher would keep running long after --minutes (found in the first live test:
    # a 5-minute run was still publishing 30 minutes later). Events whose send time
    # falls after the window are simply never sent, like a sensor that stayed offline.
    horizon = sim_minutes * 60
    return [x for x in out if x[0] <= horizon]


def main() -> None:
    p = argparse.ArgumentParser(description="Publish live ParkFlow sensor events")
    p.add_argument("--topic", help="projects/<PROJECT>/topics/<TOPIC>")
    p.add_argument("--out", help="write JSON lines to this file instead of Pub/Sub")
    p.add_argument("--minutes", type=float, default=10, help="real minutes to run")
    p.add_argument("--speed", type=float, default=60, help="simulated seconds per real second")
    p.add_argument("--malformed-rate", type=float, default=0.003)
    p.add_argument("--lots", default="config/lots.csv")
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args()
    if not args.topic and not args.out:
        p.error("give --topic or --out")

    events = build_events(args.minutes, args.speed, args.seed, args.lots, args.malformed_rate)
    print(f"Prepared {len(events)} messages for {args.minutes} real minutes at {args.speed}x")

    if args.out:
        with open(args.out, "wb") as f:
            for _, payload, _attrs in events:
                f.write(payload + b"\n")
        print(f"Wrote {args.out}")
        return

    from google.cloud import pubsub_v1  # imported here so --out works without GCP libs

    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(max_messages=500, max_latency=0.2))
    t0 = time.monotonic()
    futures = []
    for i, (offset, payload, attrs) in enumerate(events, 1):
        delay = offset - (time.monotonic() - t0)
        if delay > 0:
            time.sleep(delay)
        futures.append(publisher.publish(args.topic, payload, **attrs))
        if i % 1000 == 0:
            print(f"  published {i}/{len(events)}", flush=True)
    for f in futures:
        f.result()
    print(f"Done: {len(events)} messages published")


if __name__ == "__main__":
    main()
