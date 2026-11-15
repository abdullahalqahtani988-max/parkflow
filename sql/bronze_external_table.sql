-- Bronze: BigQuery reads the Parquet files directly from GCS (no copy, no load cost).
-- Hive partitioning turns the folder name dt=YYYY-MM-DD into a DATE column `dt`,
-- so a filter on dt skips whole folders (partition pruning).
CREATE OR REPLACE EXTERNAL TABLE `parkflow-514314.parkflow_bronze.sensor_events`
WITH PARTITION COLUMNS (dt DATE)
OPTIONS (
  format = "PARQUET",
  uris = ["gs://parkflow-514314-lake/bronze/sensor_events/*"],
  hive_partition_uri_prefix = "gs://parkflow-514314-lake/bronze/sensor_events"
);
