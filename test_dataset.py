from pathlib import Path

import pytest

pytest.importorskip("apache_beam")
from ingest_meter_readings import ValidationError, parse_line  # noqa: E402

LANDING = Path(__file__).resolve().parents[1] / "data" / "landing"


def data_lines():
    out = []
    for path in sorted(LANDING.glob("*.csv")):
        out += [ln for ln in path.read_text(encoding="utf-8").splitlines()[1:] if ln]
    return out


def test_row_count_in_range():
    assert 300 <= len(data_lines()) <= 500


def test_valid_invalid_and_duplicate_counts():
    valid, invalid = [], 0
    for line in data_lines():
        try:
            valid.append(parse_line(line))
        except ValidationError:
            invalid += 1
    assert invalid == 6
    assert len(valid) == 394
    assert len({r["reading_id"] for r in valid}) == 388
    assert sum(r["meter_id"] == "MTR-099" for r in valid) >= 4
