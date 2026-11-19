import pytest

pytest.importorskip("apache_beam")
from ingest_meter_readings import ValidationError, parse_line  # noqa: E402

GOOD = "RD-1,MTR-001,2026-11-02,12.5,230.1,AMI,OK"


def test_valid_line_is_parsed_and_typed():
    rec = parse_line(GOOD)
    assert rec["reading_id"] == "RD-1"
    assert rec["reading_date"] == "2026-11-02"
    assert rec["kwh_consumed"] == 12.5 and rec["voltage"] == 230.1


def test_case_and_whitespace_are_normalised():
    assert parse_line("RD-1, MTR-001 ,2026-11-02,12.5,230.1,ami,ok")["read_source"] == "AMI"


@pytest.mark.parametrize("line", [
    "RD-1,,2026-11-02,12.5,230.1,AMI,OK",
    ",MTR-001,2026-11-02,12.5,230.1,AMI,OK",
    "RD-1,MTR-001,2026-11-02,-4.2,230.1,AMI,OK",
    "RD-1,MTR-001,2026-11-02,abc,230.1,AMI,OK",
    "RD-1,MTR-001,2026-02-30,12.5,230.1,AMI,OK",
    "RD-1,MTR-001,not-a-date,12.5,230.1,AMI,OK",
    "RD-1,MTR-001,2026-11-02,9.8",
    "RD-1,MTR-001,2026-11-02,12.5,999.0,AMI,OK",
    "RD-1,MTR-001,2026-11-02,nan,230.1,AMI,OK",
    "RD-1,MTR-001,2026-11-02,12.5,230.1,SATELLITE,OK",
    "RD-1,MTR-001,2026-11-02,12.5,230.1,AMI,BROKEN",
    "",
])
def test_invalid_lines_are_rejected(line):
    with pytest.raises(ValidationError):
        parse_line(line)


def test_beam_routing_splits_valid_and_dead_letter():
    from apache_beam.testing.test_pipeline import TestPipeline
    from apache_beam.testing.util import assert_that, equal_to
    import apache_beam as beam
    from ingest_meter_readings import DEAD, ParseAndRoute

    elements = [("f.csv", GOOD), ("f.csv", "RD-2,,2026-11-02,1,230,AMI,OK"), ("f.csv", "garbage")]
    with TestPipeline() as p:
        routed = p | beam.Create(elements) | beam.ParDo(ParseAndRoute()).with_outputs(DEAD, main="valid")
        assert_that(routed.valid | "v" >> beam.Map(lambda r: r["reading_id"]), equal_to(["RD-1"]), label="valid")
        assert_that(routed[DEAD] | "d" >> beam.Map(lambda r: r["source_file"]),
                    equal_to(["f.csv", "f.csv"]), label="dead")
