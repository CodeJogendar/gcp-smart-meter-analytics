"""Batch Dataflow pipeline: CSV files in GCS -> validated rows in BigQuery.

Every line is parsed and validated. Valid rows go to the raw table; anything else goes to a
dead-letter table together with the original line, the source file and the reason.

This is a single file on purpose, so Cloud Composer can submit it straight from GCS.
"""
import argparse
import csv
import io
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, SetupOptions

COLUMNS = ["reading_id", "meter_id", "reading_date", "kwh_consumed", "voltage", "read_source", "reading_quality"]
READ_SOURCES = {"AMI", "MANUAL"}
QUALITIES = {"OK", "ESTIMATED"}
VOLTAGE_RANGE = (150.0, 280.0)
KWH_RANGE = (0.0, 5000.0)
DEAD = "dead"
PIPELINE_NAME = "meter-ingest"


class ValidationError(ValueError):
    """Raised when a CSV line cannot be turned into a valid reading."""


def _number(value, name, low, high):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValidationError(f"{name} must be numeric, got {value!r}") from None
    if not low <= number <= high:
        raise ValidationError(f"{name} out of range [{low}, {high}]: {number}")
    return number


def parse_line(line):
    """Return a clean record for one CSV line or raise ValidationError."""
    try:
        fields = next(csv.reader([line]))
    except (csv.Error, StopIteration) as exc:
        raise ValidationError(f"unparseable CSV line: {exc}") from exc
    if len(fields) != len(COLUMNS):
        raise ValidationError(f"expected {len(COLUMNS)} columns, got {len(fields)}")

    rec = dict(zip(COLUMNS, (f.strip() for f in fields)))
    for key in ("reading_id", "meter_id"):
        if not rec[key]:
            raise ValidationError(f"{key} is required")
    try:
        reading_date = datetime.strptime(rec["reading_date"], "%Y-%m-%d").date()
    except ValueError:
        raise ValidationError(f"reading_date is not a valid date: {rec['reading_date']!r}") from None

    read_source = rec["read_source"].upper()
    quality = rec["reading_quality"].upper()
    if read_source not in READ_SOURCES:
        raise ValidationError(f"unknown read_source {rec['read_source']!r}")
    if quality not in QUALITIES:
        raise ValidationError(f"unknown reading_quality {rec['reading_quality']!r}")

    return {
        "reading_id": rec["reading_id"],
        "meter_id": rec["meter_id"],
        "reading_date": reading_date.isoformat(),
        "kwh_consumed": _number(rec["kwh_consumed"], "kwh_consumed", *KWH_RANGE),
        "voltage": _number(rec["voltage"], "voltage", *VOLTAGE_RANGE),
        "read_source": read_source,
        "reading_quality": quality,
    }


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_lines(readable_file):
    """Yield (path, line) for every data line of a file, skipping the header and blank lines."""
    path = readable_file.metadata.path
    with readable_file.open() as handle:
        for index, raw in enumerate(io.TextIOWrapper(handle, encoding="utf-8")):
            line = raw.rstrip("\r\n")
            if index == 0 or not line.strip():
                continue
            yield path, line


class ParseAndRoute(beam.DoFn):
    """Valid rows -> main output. Everything else -> tagged `dead` output."""

    def __init__(self):
        super().__init__()
        self.valid_counter = Metrics.counter("meters", "valid_rows")
        self.dead_counter = Metrics.counter("meters", "dead_letter_rows")

    def process(self, element):
        source_file, line = element
        try:
            record = parse_line(line)
        except ValidationError as exc:
            self.dead_counter.inc()
            yield beam.pvalue.TaggedOutput(DEAD, {
                "raw_line": line[:5000], "error": str(exc)[:1000], "source_file": source_file,
                "failed_at": _now(), "pipeline": PIPELINE_NAME})
            return
        record["source_file"] = source_file
        record["ingested_at"] = _now()
        self.valid_counter.inc()
        yield record


def _bq_writer(table):
    return WriteToBigQuery(
        table=table,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
    )


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--input_pattern", required=True, help="e.g. gs://bucket/meters/landing/*.csv")
    parser.add_argument("--raw_table", default="meters_raw.meter_readings")
    parser.add_argument("--dead_letter_table", default="meters_raw.dead_letter_readings")
    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(GoogleCloudOptions).project = args.project
    options.view_as(SetupOptions).save_main_session = True  # single-file pipeline: ship module-level helpers

    pipeline = beam.Pipeline(options=options)
    lines = (
        pipeline
        | "Pattern" >> beam.Create([args.input_pattern])
        | "MatchFiles" >> fileio.MatchAll()
        | "ReadMatches" >> fileio.ReadMatches()
        | "ReadLines" >> beam.FlatMap(read_lines)
    )
    routed = lines | "ParseValidate" >> beam.ParDo(ParseAndRoute()).with_outputs(DEAD, main="valid")
    routed.valid | "WriteRaw" >> _bq_writer(f"{args.project}:{args.raw_table}")
    routed[DEAD] | "WriteDeadLetter" >> _bq_writer(f"{args.project}:{args.dead_letter_table}")

    result = pipeline.run()
    result.wait_until_finish()
    return result


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
