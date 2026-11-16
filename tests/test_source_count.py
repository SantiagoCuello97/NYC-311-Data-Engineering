"""Offline tests for the NYC 311 count adapter."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from source_count import count_source_records


LOW = datetime(2026, 11, 15, tzinfo=timezone.utc)
HIGH = datetime(2026, 11, 16, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, rows, status=200):
        self.rows = rows
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("HTTP error")

    def json(self):
        return self.rows


def test_count_success_and_query_bounds():
    captured = {}

    def fake_get(url, params, timeout):
        captured.update(params)
        return FakeResponse([{"total": "1743"}])

    total = count_source_records(
        LOW, HIGH, get=fake_get
    )

    assert total == 1743
    assert "count(*)" in captured["$select"]
    assert ":updated_at >=" in captured["$where"]
    assert ":updated_at <" in captured["$where"]


def test_transient_failure_retries():
    attempts = []

    def fake_get(url, params, timeout):
        attempts.append(1)
        if len(attempts) == 1:
            return FakeResponse([], status=503)
        return FakeResponse([{"total": "0"}])

    result = count_source_records(
        LOW,
        HIGH,
        get=fake_get,
        sleep=lambda seconds: None,
    )

    assert result == 0
    assert len(attempts) == 2


def test_invalid_count_rejected():
    def fake_get(url, params, timeout):
        return FakeResponse([{"total": "-1"}])

    with pytest.raises(ValueError, match="Invalid source count"):
        count_source_records(
            LOW, HIGH, get=fake_get
        )