import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from window_pipeline import run_window
from sync_state import get_watermark

load_dotenv(ROOT / ".env")

LOW = datetime(2026, 11, 14, tzinfo=timezone.utc)
HIGH = datetime(2026, 11, 16, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    connection = psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
    )

    try:
        with connection.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE raw_service_requests
                (LIKE public.raw_service_requests INCLUDING ALL)
                ON COMMIT DROP
            """)
            cur.execute("""
                CREATE TEMP TABLE sync_state
                (LIKE public.sync_state INCLUDING ALL)
                ON COMMIT DROP
            """)
            cur.execute(
                "SET LOCAL search_path TO pg_temp, public"
            )
            cur.execute("""
                INSERT INTO sync_state (pipeline_name)
                VALUES ('nyc311')
            """)

        yield connection

    finally:
        connection.rollback()
        connection.close()


def record(key):
    return {
        "unique_key": key,
        ":updated_at": "2026-11-15T10:00:00Z",
        "created_date": "2026-11-14T09:00:00.000",
        "closed_date": None,
        "status": "Open",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }


def fake_pages(pages):
    iterator = iter(pages)

    def fetch(lower, upper, after, limit):
        return next(iterator)

    return fetch


def count_rows(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        return cur.fetchone()[0]


def test_complete_window_stores_records(conn):
    result = run_window(
        conn,
        LOW,
        HIGH,
        fake_pages([
            [record("103"), record("102")],
            [record("101")],
        ]),
        page_size=2,
    )

    assert result["pages"] == 2
    assert result["rows_read"] == 3
    assert result["inserted"] == 3
    assert result["watermark_advanced"] is True
    assert count_rows(conn) == 3
    assert get_watermark(conn)[1] == "103"


def test_incomplete_extraction_changes_nothing(conn):
    def broken_fetch(lower, upper, after, limit):
        if after is None:
            return [record("103"), record("102")]

        raise TimeoutError("Simulated second-page failure")

    with pytest.raises(TimeoutError):
        run_window(
            conn,
            LOW,
            HIGH,
            broken_fetch,
            page_size=2,
        )

    assert count_rows(conn) == 0
    assert get_watermark(conn) == (None, None)


def test_repeated_complete_window_is_idempotent(conn):
    def source():
        return fake_pages([
            [record("103"), record("102")],
            [record("101")],
        ])

    first = run_window(
        conn, LOW, HIGH, source(), page_size=2
    )

    second = run_window(
        conn, LOW, HIGH, source(), page_size=2
    )

    assert first["inserted"] == 3
    assert second["inserted"] == 0
    assert second["updated"] == 0
    assert second["unchanged"] == 3
    assert second["watermark_advanced"] is False
    assert count_rows(conn) == 3