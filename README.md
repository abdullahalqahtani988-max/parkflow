# ParkFlow

**Smart parking data platform on Google Cloud.** ParkFlow extends an IoT graduation project
(Smart Gate Parking: ESP32 + IR sensors + Firebase) into an end-to-end data engineering
platform: real-time occupancy, a Medallion lakehouse, data quality checks and occupancy forecasting.

> Capstone project, Tuwaiq Academy, Data Engineering on Google Cloud bootcamp (Nov 2026).

## Architecture (target)

```
Sensor simulator (Python)
        │
        ▼
     Pub/Sub ──► Dataflow (Apache Beam) ──► BigQuery ──► Looker Studio
                     │        │
                     │        └──► Dead Letter Queue (invalid events)
                     └──► Real-time spot status (Bigtable / Firestore)

Lakehouse (Medallion):
  Bronze  GCS Parquet, dt=YYYY-MM-DD/  ──►  Silver (dedup + validation, MERGE)
          ──►  Gold (Dataform models + assertions)  ──►  BigQuery ML (ARIMA_PLUS forecast)
```

## Repository layout

| Folder | Purpose |
|---|---|
| `simulator/` | Generates realistic IR sensor events (peaks, weekends, hot key, dirty data) |
| `config/` | Static parking-lot reference data (later served from Google Sheets) |
| `pipelines/` | Apache Beam pipelines (batch + streaming) |
| `dataform/` | Silver/Gold SQL models and data quality assertions |
| `sql/` | BigQuery DDL run outside Dataform (Bronze external table) |
| `scripts/` | One-command setup: `setup_batch.sh`, `setup_streaming.sh`, `run_dataflow.sh`, `stop_dataflow.sh` |
| `orchestration/` | Cloud Scheduler + Workflows definitions |
| `infra/` | Terraform (planned) |
| `docs/` | Architecture decisions and design notes |
| `tests/` | Unit tests |

## Quick start (local, free)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m simulator.generate --start 2026-11-01 --days 7
python -m pytest -q
```

Output: `data/bronze/sensor_events/dt=YYYY-MM-DD/part-HHMM.parquet` (git-ignored).

## Event schema (Bronze)

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | UUID, idempotency key (duplicates reuse it) |
| `event_ts` | TIMESTAMP (UTC) | When the sensor detected the change (event time) |
| `sent_ts` | TIMESTAMP (UTC) | When the device sent it (processing side) |
| `lot_id`, `spot_id`, `sensor_id` | STRING | Location identifiers |
| `status` | STRING | `OCCUPIED` or `VACANT` |
| `battery_pct`, `rssi_dbm` | INT | Sensor health |
| `firmware` | STRING | Sensor firmware version |

## Status

- [x] Repository and project structure
- [x] Sensor simulator → local Bronze Parquet
- [x] Bronze on GCS + Silver/DLQ/Gold (Dataform) + data quality assertions
- [x] Streaming pipeline (Beam) tested locally with DirectRunner
- [x] Pub/Sub + Dataflow streaming on GCP (+ Bigtable live state)
- [x] BQML ARIMA_PLUS occupancy forecast (48h)
- [x] Data Studio dashboard (KPIs, hourly profile, daily arrivals, forecast, data quality)
- [x] CI (GitHub Actions: pytest + Dataform compile)
- [ ] Orchestration (Cloud Scheduler + Workflows)
- [ ] Live map on Cloud Run
- [ ] Terraform, daily cost slide

See [docs/decisions.md](docs/decisions.md) for the reasoning behind each design choice.
