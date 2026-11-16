import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import completed_window
from completed_window import run_completed_window

load_dotenv(ROOT / ".env")

T0 = datetime(2026, 11, 14, tzinfo=timezone.utc)
T1 = datetime(2026, 11, 15, tzinfo=timezone.utc)
T2 = datetime(2026, 11, 16, tzinfo=timezone.utc)


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
                CREATE TEMP TABLE window_sync_state
                (LIKE public.window_sync_state INCLUDING ALL)
                ON COMMIT DROP
            """)
            cur.execute(
                "SET LOCAL search_path TO pg_temp, public"
            )
            cur.execute("""
                INSERT INTO window_sync_state (pipeline_name)
                VALUES ('nyc311')
            """)

        yield connection
    finally:
        connection.rollback()
        connection.close()


def record(key, updated="2026-11-14T10:00:00Z"):
    return {
        "unique_key": key,
        ":updated_at": updated,
        "created_date": "2026-11-14T09:00:00.000",
        "closed_date": None,
        "status": "Open",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }


def source(pages):
    iterator = iter(pages)

    def fetch(lower, upper, after, limit):
        return next(iterator)

    return fetch


def checkpoint(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        return cur.fetchone()[0]


def count(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        return cur.fetchone()[0]


def test_full_window_saves_data_and_checkpoint(conn):
    result = run_completed_window(
        conn, T0, T1,
        source([
            [record("103"), record("102")],
            [record("101")],
        ]),
        page_size=2,
    )

    assert result["status"] == "SUCCESS"
    assert result["inserted"] == 3
    assert result["checkpoint_advanced"] is True
    assert count(conn) == 3
    assert checkpoint(conn) == T1


def test_empty_window_advances_checkpoint(conn):
    result = run_completed_window(
        conn, T0, T1,
        source([[]]),
        page_size=2,
    )

    assert result["status"] == "SUCCESS"
    assert result["rows_read"] == 0
    assert count(conn) == 0
    assert checkpoint(conn) == T1


def test_replay_does_not_duplicate_or_rewind(conn):
    def pages():
        return source([[record("101")]])

    first = run_completed_window(
        conn, T0, T1, pages()
    )
    second = run_completed_window(
        conn, T0, T1, pages()
    )

    assert first["inserted"] == 1
    assert second["status"] == "SKIPPED"
    assert second["checkpoint_advanced"] is False
    assert count(conn) == 1
    assert checkpoint(conn) == T1


def test_checkpoint_failure_rolls_back_records(
    conn, monkeypatch
):
    def fail(*args, **kwargs):
        raise RuntimeError("Simulated checkpoint failure")

    monkeypatch.setattr(
        completed_window, "write_checkpoint", fail
    )

    with pytest.raises(RuntimeError):
        run_completed_window(
            conn, T0, T1,
            source([[record("101")]])
        )

    assert count(conn) == 0
    assert checkpoint(conn) is None


def test_gap_between_windows_is_rejected(conn):
    run_completed_window(
        conn, T0, T1,
        source([[]])
    )

    later = datetime(
        2026, 11, 15, 12,
        tzinfo=timezone.utc,
    )

    with pytest.raises(
        ValueError,
        match="previous checkpoint",
    ):
        run_completed_window(
            conn, later, T2,
            source([[record(
                "102", "2026-11-15T13:00:00Z"
            )]])
        )

    assert checkpoint(conn) == T1
    assert count(conn) == 0