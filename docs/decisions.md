# Architecture Decision Log

Each decision lists the choice and **why** it was made.

| # | Decision | Why |
|---|---|---|
| 1 | Dataflow (Apache Beam), not Dataproc/Spark | New project, serverless, and the same Beam code serves both batch and streaming. Developed locally with DirectRunner first (free). |
| 2 | Cloud Scheduler + Workflows, not Cloud Composer | Composer is not serverless and has an always-on cost; our orchestration needs are simple. |
| 3 | No AlloyDB; static lot data in Google Sheets as a BigQuery external table | Lot metadata is small and rarely changes; a database would add cost without benefit. |
| 4 | Every event has `event_id`; Silver uses `MERGE` | Pub/Sub delivers at-least-once, so duplicates are expected. MERGE on `event_id` makes loads idempotent. |
| 5 | Dead Letter Queue for invalid events | Bad records must not stop the pipeline or silently disappear; they are kept for inspection. |
| 6 | Enrich events with lot data via Beam side input | The lot table is small, so broadcasting it avoids a shuffle. |
| 7 | Silver: `PARTITION BY DATE(event_ts)`, `CLUSTER BY lot_id, spot_id`, `require_partition_filter` | Queries filter by date and lot; this reduces bytes scanned (cost) and blocks accidental full scans. |
| 8 | Region `us-central1` | Widest service availability and free-tier coverage. |
| 9 | Bronze format: Parquet, files rolled every N minutes, `dt=YYYY-MM-DD/` | Columnar + compressed + splittable; avoids the small-files problem of one file per event; Hive partitions allow pruning. |
| 10 | Bronze `dt` uses UTC arrival time (`sent_ts`) | Bronze reflects *when data landed*. Event-time logic (Riyadh local hours) happens in Silver/Gold. |
| 11 | Hot key awareness (Grand Mall ≈ 60% of events); use `CombinePerKey` | Combiners pre-aggregate on each worker, so one big key does not overload a single worker. |
| 12 | All timestamps stored in UTC | One unambiguous time zone in storage; convert to Asia/Riyadh only for reporting. |
| 13 | Simulator uses a fixed random seed | Same input → same output, so tests and demos are reproducible. |
