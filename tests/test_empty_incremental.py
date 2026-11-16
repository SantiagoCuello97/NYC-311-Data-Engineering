"""Tests for safe processing of empty incremental windows."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from incremental_runner import run_incremental_window

load_dotenv(ROOT / ".env")

LOW = datetime(2026, 11, 15, tzinfo=timezone.utc)
HIGH = datetime(2026, 11, 15, 1, tzinfo=timezone.utc)


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
            for table in (
                "raw_service_requests",
                "window_sync_state",
                "ingestion_runs",
            ):
                cur.execute(
                    f"CREATE TEMP TABLE {table} "
                    f"(LIKE public.{table} INCLUDING ALL) "
                    f"ON COMMIT DROP"
                )

            cur.execute(
                "SET LOCAL search_path TO pg_temp, public"
            )

            cur.execute("""
                INSERT INTO window_sync_state (
                    pipeline_name,
                    completed_through,
                    last_success_at
                )
                VALUES ('nyc311', %s, NOW())
            """, (LOW,))

        yield connection
    finally:
        connection.rollback()
        connection.close()


def empty_page(lower, upper, after, limit):
    return []


def test_empty_window_commits_checkpoint_and_audit(conn):
    result = run_incremental_window(
        conn,
        LOW,
        HIGH,
        lambda lower, upper: 0,
        empty_page,
        apply=True,
    )

    assert result["status"] == "SUCCESS"
    assert result["rows_read"] == 0
    assert result["inserted"] == 0
    assert result["checkpoint_advanced"] is True

    with conn.cursor() as cur:
        cur.execute("""
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        assert cur.fetchone()[0] == HIGH

        cur.execute("""
            SELECT rows_read, rows_inserted, status
            FROM ingestion_runs
        """)
        assert cur.fetchone() == (0, 0, "SUCCESS")

        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        assert cur.fetchone()[0] == 0


def test_changed_empty_source_writes_nothing(conn):
    counts = iter([0, 1])

    with pytest.raises(
        ValueError, match="Source count changed"
    ):
        run_incremental_window(
            conn,
            LOW,
            HIGH,
            lambda lower, upper: next(counts),
            empty_page,
            apply=True,
        )

    with conn.cursor() as cur:
        cur.execute("""
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        assert cur.fetchone()[0] == LOW

        cur.execute(
            "SELECT COUNT(*) FROM ingestion_runs"
        )
        assert cur.fetchone()[0] == 0