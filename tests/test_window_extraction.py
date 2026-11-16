import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from window_extraction import extract_window

LOW = datetime(2026, 11, 14, tzinfo=timezone.utc)
HIGH = datetime(2026, 11, 16, tzinfo=timezone.utc)
TS = "2026-11-15T10:00:00Z"


def record(key, updated=TS):
    return {
        "unique_key": key,
        ":updated_at": updated,
        "status": "Open",
    }


def pages_fixture(pages):
    calls = iter(pages)

    def fetch(lower, upper, after, limit):
        return next(calls)

    return fetch


def test_complete_two_page_extraction():
    result = extract_window(
        LOW,
        HIGH,
        fetch_page=pages_fixture([
            [record("103"), record("102")],
            [record("101")],
        ]),
        page_size=2,
    )

    assert result["count"] == 3
    assert result["pages"] == 2
    assert result["complete"] is True
    assert [
        r["unique_key"] for r in result["records"]
    ] == ["103", "102", "101"]


def test_duplicate_record_is_rejected():
    with pytest.raises(ValueError):
        extract_window(
            LOW,
            HIGH,
            fetch_page=pages_fixture([
                [record("103"), record("102")],
                [record("102")],
            ]),
            page_size=2,
        )


def test_out_of_order_page_is_rejected():
    with pytest.raises(ValueError):
        extract_window(
            LOW,
            HIGH,
            fetch_page=pages_fixture([
                [record("101"), record("102")],
            ]),
            page_size=2,
        )


def test_maximum_record_limit_is_enforced():
    with pytest.raises(
        ValueError, match="Extraction limit exceeded"
    ):
        extract_window(
            LOW,
            HIGH,
            fetch_page=pages_fixture([
                [record("103"), record("102")],
                [record("101")],
            ]),
            page_size=2,
            max_records=2,
        )


def test_incomplete_api_request_fails():
    def broken_page(lower, upper, after, limit):
        if after is None:
            return [record("103"), record("102")]

        raise TimeoutError("Simulated API timeout")

    with pytest.raises(TimeoutError):
        extract_window(
            LOW,
            HIGH,
            fetch_page=broken_page,
            page_size=2,
        )


def test_outside_window_is_rejected():
    with pytest.raises(ValueError):
        extract_window(
            LOW,
            HIGH,
            fetch_page=pages_fixture([
                [record("101", "2026-11-17T10:00:00Z")]
            ]),
        )