"""Curate raw meter readings and build analytics marts (runs on Dataproc Serverless).

meters_raw.meter_readings + meters_raw.meters
   ├─ unknown meter ───────────────► meters_curated.rejected_readings
   └─ deduplicated, enriched ──────► meters_curated.fact_meter_readings
        ├─► meters_marts.meter_daily_features   (rolling 7-day stats, z-score, anomaly flag)
        ├─► meters_marts.anomaly_report         (flagged readings with severity)
        ├─► meters_marts.meter_completeness     (missing days per meter)
        └─► meters_marts.daily_city_consumption
"""
import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("meter-transform")

ROLLING_DAYS = 7
MIN_HISTORY = 5
Z_THRESHOLD = 3.0
RATIO_THRESHOLD = 3.0
READING_COLUMNS = ["reading_id", "meter_id", "reading_date", "kwh_consumed", "voltage",
                   "read_source", "reading_quality", "source_file", "ingested_at"]
METER_COLUMNS = ["meter_id", "customer_type", "city", "tariff_plan", "feeder_id", "sanctioned_load_kw"]


def dedupe(readings: DataFrame) -> DataFrame:
    """Keep the most recently ingested copy of every reading_id."""
    w = Window.partitionBy("reading_id").orderBy(F.col("ingested_at").desc())
    return readings.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def split_by_known_meter(readings: DataFrame, meters: DataFrame):
    """Return (enriched readings for known meters, rejected readings for unknown meters)."""
    joined = readings.join(meters.select(*METER_COLUMNS), "meter_id", "left")
    known = joined.filter(F.col("customer_type").isNotNull())
    unknown = (joined.filter(F.col("customer_type").isNull())
               .select(*READING_COLUMNS)
               .withColumn("reject_reason", F.lit("unknown_meter")))
    return known, unknown


def build_fact(known: DataFrame, run_date: str) -> DataFrame:
    return known.select(*READING_COLUMNS, *METER_COLUMNS[1:]).withColumn("processed_run_date", F.lit(run_date))


def add_rolling_features(fact: DataFrame) -> DataFrame:
    """Rolling stats over the previous 7 calendar days (excluding the current day) per meter."""
    day_num = F.datediff(F.col("reading_date"), F.lit("1970-01-01").cast("date"))
    w = Window.partitionBy("meter_id").orderBy("day_num").rangeBetween(-ROLLING_DAYS, -1)
    kwh = F.col("kwh_consumed")
    df = (fact.withColumn("day_num", day_num)
          .withColumn("history_days", F.count("kwh_consumed").over(w))
          .withColumn("rolling_avg_kwh", F.avg("kwh_consumed").over(w))
          .withColumn("rolling_std_kwh", F.stddev_samp("kwh_consumed").over(w)))
    enough = F.col("history_days") >= MIN_HISTORY
    z = F.when(enough & (F.col("rolling_std_kwh") > 0), (kwh - F.col("rolling_avg_kwh")) / F.col("rolling_std_kwh"))
    df = df.withColumn("z_score", F.round(z, 2))
    flag = enough & ((F.col("z_score") >= Z_THRESHOLD) | (kwh >= RATIO_THRESHOLD * F.col("rolling_avg_kwh")))
    return (df.withColumn("is_anomaly", F.coalesce(flag, F.lit(False)))
            .withColumn("rolling_avg_kwh", F.round("rolling_avg_kwh", 2))
            .withColumn("rolling_std_kwh", F.round("rolling_std_kwh", 2))
            .drop("day_num"))


def build_anomaly_report(features: DataFrame) -> DataFrame:
    severity = F.when((F.col("z_score") >= 6) | (F.col("kwh_consumed") >= 5 * F.col("rolling_avg_kwh")), "HIGH") \
        .otherwise("MEDIUM")
    return (features.filter("is_anomaly")
            .select("reading_id", "meter_id", "customer_type", "city", "feeder_id", "reading_date",
                    "kwh_consumed", "rolling_avg_kwh", "z_score")
            .withColumn("severity", severity))


def build_completeness(fact: DataFrame) -> DataFrame:
    bounds = fact.agg(F.min("reading_date").alias("lo"), F.max("reading_date").alias("hi")).first()
    expected = (bounds["hi"] - bounds["lo"]).days + 1
    return (fact.groupBy("meter_id")
            .agg(F.countDistinct("reading_date").alias("actual_days"))
            .withColumn("expected_days", F.lit(expected))
            .withColumn("missing_days", F.col("expected_days") - F.col("actual_days"))
            .withColumn("completeness_pct", F.round(100 * F.col("actual_days") / F.col("expected_days"), 1)))


def build_daily_city(fact: DataFrame) -> DataFrame:
    return (fact.groupBy("reading_date", "city", "customer_type")
            .agg(F.countDistinct("meter_id").alias("meters_reporting"),
                 F.round(F.sum("kwh_consumed"), 2).alias("total_kwh"),
                 F.round(F.avg("kwh_consumed"), 2).alias("avg_kwh_per_meter")))


def write_bq(df: DataFrame, table: str, bucket: str) -> None:
    (df.write.format("bigquery")
       .option("table", table)
       .option("temporaryGcsBucket", bucket)
       .mode("overwrite")
       .save())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--project", required=True)
    p.add_argument("--bucket", required=True, help="GCS bucket for BigQuery connector temp files")
    p.add_argument("--run-date", required=True)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO)

    spark = SparkSession.builder.appName("meter-transform").getOrCreate()
    proj = args.project
    readings = (spark.read.format("bigquery").option("table", f"{proj}.meters_raw.meter_readings")
                .load().select(*READING_COLUMNS))
    meters = spark.read.format("bigquery").option("table", f"{proj}.meters_raw.meters").load()

    deduped = dedupe(readings).cache()
    known, unknown = split_by_known_meter(deduped, meters)
    fact = build_fact(known, args.run_date).cache()
    features = add_rolling_features(fact).cache()

    log.info("raw=%d deduped=%d fact=%d rejected=%d anomalies=%d",
             readings.count(), deduped.count(), fact.count(), unknown.count(),
             features.filter("is_anomaly").count())

    b = args.bucket
    write_bq(unknown.withColumn("processed_run_date", F.lit(args.run_date)),
             f"{proj}.meters_curated.rejected_readings", b)
    write_bq(fact, f"{proj}.meters_curated.fact_meter_readings", b)
    write_bq(features, f"{proj}.meters_marts.meter_daily_features", b)
    write_bq(build_anomaly_report(features), f"{proj}.meters_marts.anomaly_report", b)
    write_bq(build_completeness(fact), f"{proj}.meters_marts.meter_completeness", b)
    write_bq(build_daily_city(fact), f"{proj}.meters_marts.daily_city_consumption", b)
    spark.stop()


if __name__ == "__main__":
    main()
