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
| 14 | Validation rules defined once (`bronze_validated` view); Silver = valid rows, DLQ = invalid rows | Both outputs come from the same rules, so they can never disagree; every Bronze row lands in exactly one place. |
| 15 | Silver as a Dataform **incremental** table with `uniqueKey: event_id` | Dataform generates the `MERGE`; re-running the same files is idempotent. `updatePartitionFilter` limits the MERGE scan to recent partitions (cost). |
| 16 | Hand-written assertions instead of built-in ones on Silver | Silver has `require_partition_filter`; built-in assertions query without a partition filter and would fail. |
| 17 | Gold occupancy from **sessions** (OCCUPIED → next event) split into hours | Gives true average occupancy per hour (not just event counts), on a complete hourly grid with zeros — required by ARIMA_PLUS. |
| 18 | Pub/Sub fan-out: a Cloud Storage subscription archives raw messages to Bronze; a second subscription feeds Dataflow | The raw archive needs no code and no running workers; Dataflow can be stopped without losing the Bronze record. |
| 19 | Streaming dedup with Beam `DeduplicatePerKey` (10 min) + MERGE downstream | Pub/Sub is at-least-once and devices retry; dedup close to the source keeps real-time counts correct, MERGE guarantees it in Silver. |
| 20 | Windows use **event time** with 1 h allowed lateness | Late sensor messages are counted in the minute they happened, not when they arrived. |
| 21 | Dataflow: 1 × e2-standard-2 worker, Streaming Engine, run only during tests, stopped with **drain** | Keeps cost near $0.3/hour; drain finishes in-flight data before shutdown (no loss). |
