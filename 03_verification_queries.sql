-- After Dataflow: 394 rows in raw (388 unique), 6 dead letters
SELECT COUNT(*) AS raw_rows, COUNT(DISTINCT reading_id) AS unique_readings FROM `meters_raw.meter_readings`;
SELECT error, COUNT(*) n FROM `meters_raw.dead_letter_readings` GROUP BY 1 ORDER BY n DESC;

-- After Spark: 384 curated readings, 4 rejected (unknown meter)
SELECT COUNT(*) AS fact_rows, COUNT(DISTINCT reading_id) AS unique_readings FROM `meters_curated.fact_meter_readings`;
SELECT reject_reason, COUNT(*) n FROM `meters_curated.rejected_readings` GROUP BY 1;

-- Anomalies and data completeness
SELECT * FROM `meters_marts.anomaly_report` ORDER BY z_score DESC;
SELECT SUM(missing_days) AS total_missing_days, MIN(completeness_pct) AS worst_completeness_pct
FROM `meters_marts.meter_completeness`;
SELECT * FROM `meters_marts.daily_city_consumption` ORDER BY reading_date, city LIMIT 20;
