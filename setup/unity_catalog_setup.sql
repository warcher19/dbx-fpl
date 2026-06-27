-- FPL Unity Catalog Setup
-- Run once as a workspace admin before deploying the DAB bundle.

CREATE CATALOG IF NOT EXISTS fpl
  COMMENT 'Fantasy Premier League analytics platform';

CREATE SCHEMA IF NOT EXISTS fpl.bronze
  COMMENT 'Raw ingested data from FPL API and historical sources';

CREATE SCHEMA IF NOT EXISTS fpl.silver
  COMMENT 'Cleansed, conformed, and deduplicated FPL data';

CREATE SCHEMA IF NOT EXISTS fpl.gold
  COMMENT 'Business-level aggregations and FPL analytics metrics';

-- Landing zone: FPL API JSON drops and vaastav historical CSVs
-- Path: /Volumes/fpl/bronze/landing/
CREATE VOLUME IF NOT EXISTS fpl.bronze.landing
  COMMENT 'Landing zone for FPL API JSON drops and vaastav historical CSVs';

-- Auto Loader schema inference metadata and checkpoints
-- Path: /Volumes/fpl/bronze/autoloader_meta/
CREATE VOLUME IF NOT EXISTS fpl.bronze.autoloader_meta
  COMMENT 'Auto Loader schema tracking for bronze ingestion streams';
