"""Isolated PostgreSQL tests for known-ID reconciliation."""

import os
from datetime import datetime
import sys
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingest import ingest_records
from reconciliation_apply import reconcile_known_records

load_dotenv(ROOT / ".env")

OLD = "2026-11-15T02:58:20.910Z"
NEW = "2026-11-16T02:32:33.989Z"


def record(key, updated=OLD, status="Open"):
    return {
        "unique_key": key,
        ":updated_at": updated,
        "created_date": "2026-11-15T01:00:00.000",
        "closed_date": None,
        "status": status,
        "agency": "DOT",
        "complaint_type": "Street Condition",
    }


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
            cur.execute(
                "SET LOCAL search_path TO pg_temp, public"
            )

        yield connection

    finally:
        connection.rollback()
        connection.close()


def seed(conn, rows):
    ingest_records(conn, rows)
    return {row["unique_key"]: row for row in rows}


def database_state(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT unique_key, status, source_updated_at
            FROM raw_service_requests
            ORDER BY unique_key
        """)
        return cur.fetchall()


def test_timestamp_change_updates_existing_record(conn):
    original = record("101")
    stored = seed(conn, [original])

    result = reconcile_known_records(
        conn,
        stored,
        [record("101", updated=NEW)],
    )

    assert result["timestamp_only"] == 1
    assert result["updated"] == 1
    assert result["deleted"] == 0

    rows = database_state(conn)
    assert len(rows) == 1
    assert rows[0][0] == "101"
    assert rows[0][2] == datetime.fromisoformat(
        "2026-11-16T02:32:33.989+00:00"
    )


def test_business_change_updates_existing_record(conn):
    stored = seed(conn, [record("101")])

    result = reconcile_known_records(
        conn,
        stored,
        [record("101", updated=NEW, status="Closed")],
    )

    assert result["business_changed"] == 1
    assert result["updated"] == 1
    assert database_state(conn)[0][1] == "Closed"


def test_missing_source_does_not_delete(conn):
    stored = seed(conn, [record("101")])

    result = reconcile_known_records(
        conn,
        stored,
        [],
    )

    assert result["not_found"] == 1
    assert result["updated"] == 0
    assert result["deleted"] == 0
    assert len(database_state(conn)) == 1


def test_stale_source_does_not_overwrite(conn):
    stored = seed(conn, [record("101", updated=NEW)])

    result = reconcile_known_records(
        conn,
        stored,
        [record("101", updated=OLD, status="Closed")],
    )

    assert result["stale_source"] == 1
    assert result["updated"] == 0
    assert database_state(conn)[0][1] == "Open"


def test_stale_snapshot_does_not_overwrite_newer_db_row(conn):
    # Initial snapshot is old.
    snapshot = seed(conn, [record("101", updated=OLD)])

    # Another operation has already stored a newer version.
    ingest_records(
        conn,
        [record("101", updated=NEW, status="Closed")],
    )

    # Reconciliation brings an intermediate version.
    intermediate = "2026-11-16T01:00:00Z"

    result = reconcile_known_records(
        conn,
        snapshot,
        [record("101", updated=intermediate, status="Pending")],
    )

    assert result["stale_source"] == 1
    assert result["updated"] == 0

    rows = database_state(conn)
    assert len(rows) == 1
    assert rows[0][1] == "Closed"
    assert rows[0][2] == datetime.fromisoformat(
        "2026-11-16T02:32:33.989+00:00"
    )


def test_missing_database_row_is_never_reinserted(conn):
    snapshot = seed(conn, [record("101")])

    # Simulate a record removed after the initial read.
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM raw_service_requests "
            "WHERE unique_key = %s",
            ("101",),
        )

    with pytest.raises(
        ValueError, match="disappeared"
    ):
        reconcile_known_records(
            conn,
            snapshot,
            [record("101", updated=NEW)],
        )

    assert database_state(conn) == []