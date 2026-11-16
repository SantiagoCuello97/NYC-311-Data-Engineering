"""Unit tests for full-payload reconciliation policy."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from id_reconciliation import (
    classify_record,
    compare_known_records,
)

OLD = "2026-11-15T02:58:20.910Z"
NEW = "2026-11-16T02:32:33.989Z"


def record(key, updated=OLD, status="Open"):
    return {
        "unique_key": key,
        ":updated_at": updated,
        "status": status,
        "agency": "DOT",
    }


def test_timestamp_only_change():
    result = classify_record(
        record("101"),
        record("101", updated=NEW),
    )

    assert result["status"] == "TIMESTAMP_ONLY"
    assert result["changed_fields"] == []


def test_business_change_detected():
    result = classify_record(
        record("101"),
        record("101", updated=NEW, status="Closed"),
    )

    assert result["status"] == "BUSINESS_CHANGED"
    assert result["changed_fields"] == ["status"]


def test_missing_source_is_not_deleted():
    result = compare_known_records(
        {"101": record("101")},
        [],
    )

    assert result["101"]["status"] == "NOT_FOUND"


def test_reject_duplicate_source_keys():
    with pytest.raises(ValueError, match="Duplicate"):
        compare_known_records(
            {"101": record("101")},
            [record("101"), record("101")],
        )


def test_stale_source_is_detected():
    result = classify_record(
        record("101", updated=NEW),
        record("101", updated=OLD),
    )

    assert result["status"] == "STALE_SOURCE"