-- Run in the BigQuery editor AFTER creating the datasets meters_raw, meters_curated and meters_marts
-- through the UI in asia-south1. Replace ${PROJECT_ID} with your project id.

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.meters_raw.meter_readings` (
  reading_id      STRING,
  meter_id        STRING,
  reading_date    DATE,
  kwh_consumed    FLOAT64,
  voltage         FLOAT64,
  read_source     STRING,
  reading_quality STRING,
  source_file     STRING,
  ingested_at     TIMESTAMP
)
PARTITION BY reading_date
CLUSTER BY meter_id
OPTIONS (description = 'Validated readings written by the Dataflow job (re-delivered duplicates possible)');

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.meters_raw.dead_letter_readings` (
  raw_line    STRING,
  error       STRING,
  source_file STRING,
  failed_at   TIMESTAMP,
  pipeline    STRING
)
OPTIONS (description = 'Lines rejected by validation, with the reason');

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.meters_raw.meters` (
  meter_id           STRING,
  customer_type      STRING,
  city               STRING,
  state              STRING,
  tariff_plan        STRING,
  feeder_id          STRING,
  sanctioned_load_kw INT64,
  install_date       DATE
);
