-- FPL Unity Catalog Setup
-- Run once as a workspace admin before deploying the DAB bundle.
-- Replace <storage_account> with your ADLS Gen2 storage account name.

CREATE CATALOG IF NOT EXISTS fpl
  COMMENT 'Fantasy Premier League analytics platform';

CREATE SCHEMA IF NOT EXISTS fpl.bronze
  COMMENT 'Raw ingested data from FPL API and historical sources';

CREATE SCHEMA IF NOT EXISTS fpl.silver
  COMMENT 'Cleansed, conformed, and deduplicated FPL data';

CREATE SCHEMA IF NOT EXISTS fpl.gold
  COMMENT 'Business-level aggregations and FPL analytics metrics';

-- Volume for Auto Loader schema inference metadata and checkpoints
CREATE VOLUME IF NOT EXISTS fpl.bronze.autoloader_meta
  COMMENT 'Auto Loader schema tracking for bronze ingestion streams';

-- External location for the API + historical landing zone
-- Uncomment and configure once storage credential is set up:
-- CREATE EXTERNAL LOCATION IF NOT EXISTS fpl_landing
--   URL 'abfss://fpl-landing@<storage_account>.dfs.core.windows.net/'
--   WITH (STORAGE CREDENTIAL <credential_name>)
--   COMMENT 'Landing zone for FPL API JSON drops and vaastav historical CSVs';
