"""Unit tests for source/database record reconciliation."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from reconciliation import compare_records


def record(key, status="Open"):
    return {"unique_key": key, "status": status}


def test_identical_data_matches():
    source = [record("101"), record("102")]
    stored = {row["unique_key"]: row.copy() for row in source}

    result = compare_records(source, stored)

    assert result["is_match"] is True
    assert result["matching"] == 2
    assert result["missing"] == []
    assert result["changed"] == []
    assert result["extra"] == []


def test_missing_and_extra_records():
    result = compare_records(
        [record("101"), record("102")],
        {"101": record("101"), "103": record("103")},
    )

    assert result["is_match"] is False
    assert result["matching"] == 1
    assert result["missing"] == ["102"]
    assert result["extra"] == ["103"]


def test_changed_payload_detected():
    result = compare_records(
        [record("101", "Closed")],
        {"101": record("101", "Open")},
    )

    assert result["changed"] == ["101"]
    assert result["matching"] == 0
    assert result["is_match"] is False


def test_duplicate_source_ids_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        compare_records(
            [record("101"), record("101")],
            {},
        )