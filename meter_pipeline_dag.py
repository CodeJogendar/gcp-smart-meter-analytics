"""Daily orchestration: GCS landing files -> Dataflow ingest -> BigQuery raw -> Dataproc Spark -> marts.

wait_for_files -> ingest_dataflow -> [check_raw_loaded, check_reject_ratio] -> transform_spark
               -> check_marts -> archive_landing_files -> end

Airflow Variables: gcp_project, gcp_region, lake_bucket (optional: dataflow_zone).
Note: BigQuery operators do not template `location`, so these values are read once at parse time.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = Variable.get("gcp_project", default_var="CHANGE_ME")
REGION = Variable.get("gcp_region", default_var="asia-south1")
BUCKET = Variable.get("lake_bucket", default_var="CHANGE_ME")
DATAFLOW_ZONE = Variable.get("dataflow_zone", default_var="")
BEAM_VERSION = "2.77.0"

LANDING_PREFIX = "meters/landing/"
# Match real files only: a folder created in the Console is a zero-byte object under LANDING_PREFIX.
LANDING_FILE_PREFIX = f"{LANDING_PREFIX}meter_readings_"
ARCHIVE_PREFIX = "meters/archive/"
CODE_PREFIX = "meters/code/"


def sql_check(task_id: str, query: str) -> BigQueryInsertJobOperator:
    """Run a query that calls ERROR() on violation, which fails the task."""
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location=REGION,
        configuration={"query": {"query": query, "useLegacySql": False}},
    )


dataflow_options = {
    "project": PROJECT_ID,
    "temp_location": f"gs://{BUCKET}/meters/temp",
    "staging_location": f"gs://{BUCKET}/meters/staging",
    "input_pattern": f"gs://{BUCKET}/{LANDING_PREFIX}*.csv",
    "num_workers": "1",
    "max_num_workers": "1",
    "machine_type": "e2-standard-2",
}
if DATAFLOW_ZONE:
    dataflow_options["worker_zone"] = DATAFLOW_ZONE

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="meter_pipeline_daily",
    description="Smart-meter pipeline: GCS -> Dataflow -> BigQuery -> Dataproc -> marts",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gcp", "meters", "portfolio"],
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_files = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_files",
        bucket=BUCKET,
        prefix=LANDING_FILE_PREFIX,
        mode="reschedule",
        poke_interval=30,
        timeout=600,
    )

    ingest_dataflow = BeamRunPythonPipelineOperator(
        task_id="ingest_dataflow",
        runner=BeamRunnerType.DataflowRunner,
        py_file=f"gs://{BUCKET}/{CODE_PREFIX}ingest_meter_readings.py",
        py_interpreter="python3",
        py_requirements=[f"apache-beam[gcp]=={BEAM_VERSION}"],
        py_system_site_packages=False,
        pipeline_options=dataflow_options,
        dataflow_config=DataflowConfiguration(
            job_name="meter-ingest",
            project_id=PROJECT_ID,
            location=REGION,
            wait_until_finished=True,
        ),
    )

    check_raw_loaded = sql_check(
        "check_raw_loaded",
        f"SELECT IF(COUNT(*) = 0, ERROR('meters_raw.meter_readings is empty'), COUNT(*)) "
        f"FROM `{PROJECT_ID}.meters_raw.meter_readings`",
    )

    check_reject_ratio = sql_check(
        "check_reject_ratio",
        f"""
        WITH good AS (SELECT COUNT(*) AS n FROM `{PROJECT_ID}.meters_raw.meter_readings`),
             bad  AS (SELECT COUNT(*) AS n FROM `{PROJECT_ID}.meters_raw.dead_letter_readings`)
        SELECT IF(SAFE_DIVIDE(bad.n, good.n + bad.n) > 0.10,
                  ERROR('more than 10% of ingested rows were rejected'), bad.n)
        FROM good, bad
        """,
    )

    transform_spark = DataprocCreateBatchOperator(
        task_id="transform_spark",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="meter-transform-{{ ts_nodash | lower }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{BUCKET}/{CODE_PREFIX}transform_meter_readings.py",
                "args": ["--project", PROJECT_ID, "--bucket", BUCKET, "--run-date", "{{ ds }}"],
            },
            "runtime_config": {"version": "2.2"},
        },
    )

    check_marts = sql_check(
        "check_marts",
        f"""
        SELECT IF(
          (SELECT COUNT(*) FROM `{PROJECT_ID}.meters_curated.fact_meter_readings`)
            != (SELECT COUNT(DISTINCT reading_id) FROM `{PROJECT_ID}.meters_curated.fact_meter_readings`)
          OR (SELECT COUNT(*) FROM `{PROJECT_ID}.meters_curated.fact_meter_readings`)
            != (SELECT COUNT(*) FROM `{PROJECT_ID}.meters_marts.meter_daily_features`)
          OR (SELECT COUNTIF(kwh_consumed < 0 OR meter_id IS NULL)
              FROM `{PROJECT_ID}.meters_curated.fact_meter_readings`) > 0,
          ERROR('curated/mart integrity check failed'), 1)
        """,
    )

    archive_landing_files = GCSToGCSOperator(
        task_id="archive_landing_files",
        source_bucket=BUCKET,
        source_object=f"{LANDING_PREFIX}*.csv",
        destination_bucket=BUCKET,
        destination_object=ARCHIVE_PREFIX,
        move_object=True,
    )

    end = EmptyOperator(task_id="end")

    (start >> wait_for_files >> ingest_dataflow >> [check_raw_loaded, check_reject_ratio]
     >> transform_spark >> check_marts >> archive_landing_files >> end)
