"""Unit tests for selected-field payload comparison."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from id_reconciliation import changed_fields


def test_same_business_fields():
    old = {
        "status": "Closed",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }
    current = old.copy()
    current[":updated_at"] = "2026-11-16T02:32:33.989Z"

    assert changed_fields(old, current) == []


def test_detect_single_changed_field():
    old = {
        "status": "Open",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }
    current = {**old, "status": "Closed"}

    assert changed_fields(old, current) == ["status"]


def test_detect_multiple_changed_fields():
    old = {
        "status": "Open",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }
    current = {
        "status": "Closed",
        "agency": "DEP",
        "complaint_type": "Water Leak",
    }

    assert changed_fields(old, current) == [
        "status", "agency", "complaint_type"
    ]