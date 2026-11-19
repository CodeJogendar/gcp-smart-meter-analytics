import csv
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
import transform_meter_readings as t  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
               .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield session
    session.stop()


def _load(spark):
    from ingest_meter_readings import ValidationError, parse_line

    ts = datetime(2026, 11, 20, 8, 0, 0)
    rows = []
    for path in sorted((ROOT / "data" / "landing").glob("*.csv")):
        for line in path.read_text(encoding="utf-8").splitlines()[1:]:
            try:
                r = parse_line(line)
            except ValidationError:
                continue
            rows.append((r["reading_id"], r["meter_id"], date.fromisoformat(r["reading_date"]),
                         r["kwh_consumed"], r["voltage"], r["read_source"], r["reading_quality"],
                         path.name, ts))
    readings = spark.createDataFrame(rows, ", ".join(
        ["reading_id string", "meter_id string", "reading_date date", "kwh_consumed double", "voltage double",
         "read_source string", "reading_quality string", "source_file string", "ingested_at timestamp"]))
    with open(ROOT / "data" / "meters.csv", encoding="utf-8") as f:
        meters = spark.createDataFrame([
            (m["meter_id"], m["customer_type"], m["city"], m["tariff_plan"], m["feeder_id"],
             int(m["sanctioned_load_kw"])) for m in csv.DictReader(f)],
            "meter_id string, customer_type string, city string, tariff_plan string, feeder_id string, "
            "sanctioned_load_kw int")
    return readings, meters


def test_pipeline_logic_end_to_end(spark):
    readings, meters = _load(spark)
    deduped = t.dedupe(readings)
    assert readings.count() == 394 and deduped.count() == 388

    known, unknown = t.split_by_known_meter(deduped, meters)
    assert unknown.count() == 4
    fact = t.build_fact(known, "2026-11-20")
    assert fact.count() == 384

    features = t.add_rolling_features(fact)
    assert features.count() == 384
    anomalies = t.build_anomaly_report(features)
    n = anomalies.count()
    assert 6 <= n <= 12, n
    assert anomalies.filter("severity = 'HIGH'").count() >= 1

    completeness = t.build_completeness(fact)
    assert completeness.count() == 20
    assert completeness.agg({"missing_days": "sum"}).first()[0] == 16

    daily = t.build_daily_city(fact)
    assert daily.count() > 20
