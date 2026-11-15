// External / reference tables created outside Dataform (see sql/ and scripts/).
declare({ schema: "parkflow_bronze", name: "sensor_events",
  description: "Bronze: raw IR sensor events, Parquet files in GCS (Hive partitions dt=YYYY-MM-DD)." });
declare({ schema: "parkflow_ref", name: "lots",
  description: "Reference: static parking-lot metadata (capacity, location)." });
