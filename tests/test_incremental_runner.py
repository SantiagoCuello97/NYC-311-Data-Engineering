"""Offline and isolated-PostgreSQL incremental runner tests."""

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
HIGH = datetime(2026, 11, 16, tzinfo=timezone.utc)


def record(key):
    return {
        "unique_key": key,
        ":updated_at": "2026-11-15T10:00:00Z",
        "created_date": "2026-11-15T09:00:00.000",
        "closed_date": None,
        "status": "Open",
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }


def source(rows):
    def fetch(lower, upper, after, limit):
        if after is None:
            return rows
        return []
    return fetch


def counter(value):
    def count(lower, upper):
        return value
    return count


def test_dry_run_reconciles_without_database():
    result = run_incremental_window(
        None, LOW, HIGH,
        counter(2),
        source([record("102"), record("101")]),
    )

    assert result["status"] == "DRY_RUN"
    assert result["rows_read"] == 2
    assert result["checkpoint_advanced"] is False


def test_safety_limit_rejects_before_extraction():
    def forbidden_fetch(*args):
        raise AssertionError("Should not fetch")

    with pytest.raises(ValueError, match="safety limit"):
        run_incremental_window(
            None, LOW, HIGH,
            counter(2501),
            forbidden_fetch,
        )


def test_incorrect_source_count_is_rejected():
    with pytest.raises(ValueError, match="count mismatch"):
        run_incremental_window(
            None, LOW, HIGH,
            counter(3),
            source([record("102"), record("101")]),
        )


def test_source_count_change_is_rejected():
    values = iter([2, 3])

    def changing_count(lower, upper):
        return next(values)

    with pytest.raises(ValueError, match="count changed"):
        run_incremental_window(
            None, LOW, HIGH,
            changing_count,
            source([record("102"), record("101")]),
        )


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
            cur.execute(
                """
                INSERT INTO window_sync_state (
                    pipeline_name, completed_through,
                    last_success_at
                )
                VALUES ('nyc311', %s, NOW())
                """,
                (LOW,),
            )

        yield connection
    finally:
        connection.rollback()
        connection.close()


def test_apply_commits_rows_checkpoint_and_audit(conn):
    result = run_incremental_window(
        conn, LOW, HIGH,
        counter(2),
        source([record("102"), record("101")]),
        apply=True,
    )

    assert result["status"] == "SUCCESS"
    assert result["inserted"] == 2
    assert result["checkpoint_advanced"] is True

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        assert cur.fetchone()[0] == 2

        cur.execute(
            """
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
            """
        )
        assert cur.fetchone()[0] == HIGH

        cur.execute(
            "SELECT rows_read, rows_inserted "
            "FROM ingestion_runs"
        )
        assert cur.fetchone() == (2, 2)


def test_non_contiguous_apply_writes_nothing(conn):
    later = datetime(
        2026, 11, 15, 12,
        tzinfo=timezone.utc,
    )

    with pytest.raises(ValueError, match="Non-contiguous"):
        run_incremental_window(
            conn, later, HIGH,
            counter(1),
            source([{
                **record("101"),
                ":updated_at": "2026-11-15T13:00:00Z",
            }]),
            apply=True,
        )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        assert cur.fetchone()[0] == 0

        cur.execute(
            """
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
            """
        )
        assert cur.fetchone()[0] == LOW


def test_preflight_rejects_gap_without_api_calls(conn):
    later = datetime(
        2026, 11, 15, 12,
        tzinfo=timezone.utc,
    )

    def forbidden(*args):
        raise AssertionError("API must not be queried")

    with pytest.raises(ValueError, match="Non-contiguous"):
        run_incremental_window(
            conn,
            later,
            HIGH,
            forbidden,
            forbidden,
            apply=True,
        )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        assert cur.fetchone()[0] == 0


def test_preflight_skips_completed_window_without_api(conn):
    first = run_incremental_window(
        conn,
        LOW,
        HIGH,
        counter(1),
        source([record("101")]),
        apply=True,
    )

    assert first["status"] == "SUCCESS"

    def forbidden(*args):
        raise AssertionError("API must not be queried")

    second = run_incremental_window(
        conn,
        LOW,
        HIGH,
        forbidden,
        forbidden,
        apply=True,
    )

    assert second["status"] == "SKIPPED"
    assert second["checkpoint_advanced"] is False

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        assert cur.fetchone()[0] == 1

        cur.execute(
            "SELECT COUNT(*) FROM ingestion_runs"
        )
        assert cur.fetchone()[0] == 1