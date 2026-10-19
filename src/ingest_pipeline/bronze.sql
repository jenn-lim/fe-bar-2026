-- =============================================================================
-- BRONZE · raw ingest via Auto Loader
-- =============================================================================
-- Notebook 01 lands each synthetic feed as newline-delimited JSON in the UC Volume
-- {catalog}.{schema}.raw/<feed>. Here Auto Loader (STREAM read_files) incrementally
-- ingests those files into bronze streaming tables. Schema is inferred and every raw
-- column is preserved as-is; we only add ingest lineage (_source_file, _ingested_at).
--
-- ${raw_volume_path} comes from the pipeline `configuration` block in databricks.yml
-- and resolves to /Volumes/<catalog>/<schema>/raw.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_members
  COMMENT "Raw members feed (JSON), ingested with Auto Loader."
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
   FROM STREAM read_files('${raw_volume_path}/members', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_products
  COMMENT "Raw products catalog feed (JSON), ingested with Auto Loader."
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
   FROM STREAM read_files('${raw_volume_path}/products', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_orders
  COMMENT "Raw orders feed (JSON), ingested with Auto Loader."
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
   FROM STREAM read_files('${raw_volume_path}/orders', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_order_items
  COMMENT "Raw order line-items feed (JSON), ingested with Auto Loader."
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
   FROM STREAM read_files('${raw_volume_path}/order_items', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_web_sessions
  COMMENT "Raw web-sessions feed (JSON), ingested with Auto Loader."
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
   FROM STREAM read_files('${raw_volume_path}/web_sessions', format => 'json');
