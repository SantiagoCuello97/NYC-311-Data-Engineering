import os
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "src"))

from ingest import ingest_records

load_dotenv(PROJECT / ".env")


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


def make_record(
    key="SP-TEST-001",
    status="Open",
    updated="2026-11-01T10:00:00Z",
):
    return {
        "unique_key": key,
        "created_date": "2026-11-01T09:00:00.000",
        "closed_date": None,
        "status": status,
        "agency": "DOT",
        "complaint_type": "Street Condition",
        ":updated_at": updated,
    }


def get_record(conn, key="SP-TEST-001"):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, source_updated_at, raw_payload
            FROM raw_service_requests
            WHERE unique_key = %s
            """,
            (key,),
        )
        return cur.fetchone()


def get_count(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        return cur.fetchone()[0]


def test_insert_new_record(conn):
    result = ingest_records(conn, [make_record()])

    assert result == (1, 0, 0)
    assert get_count(conn) == 1
    assert get_record(conn)[0] == "Open"


def test_repeated_ingestion_is_idempotent(conn):
    record = make_record()

    assert ingest_records(conn, [record]) == (1, 0, 0)
    assert ingest_records(conn, [record]) == (0, 0, 1)
    assert get_count(conn) == 1


def test_newer_update_changes_status(conn):
    original = make_record()
    changed = make_record(
        status="Closed",
        updated="2026-11-02T12:00:00Z",
    )
    changed["closed_date"] = "2026-11-02T11:00:00.000"

    ingest_records(conn, [original])
    result = ingest_records(conn, [changed])

    assert result == (0, 1, 0)
    assert get_count(conn) == 1
    assert get_record(conn)[0] == "Closed"
    assert get_record(conn)[2]["closed_date"] == (
        "2026-11-02T11:00:00.000"
    )


def test_older_update_cannot_overwrite_newer(conn):
    newer = make_record(
        status="Closed",
        updated="2026-11-03T10:00:00Z",
    )
    older = make_record(
        status="Open",
        updated="2026-11-01T10:00:00Z",
    )

    ingest_records(conn, [newer])
    result = ingest_records(conn, [older])

    assert result == (0, 0, 1)
    assert get_count(conn) == 1
    assert get_record(conn)[0] == "Closed"


def test_same_timestamp_content_correction(conn):
    original = make_record()
    corrected = deepcopy(original)
    corrected["agency"] = "NYPD"

    ingest_records(conn, [original])
    result = ingest_records(conn, [corrected])

    assert result == (0, 1, 0)
    assert get_count(conn) == 1
    assert get_record(conn)[2]["agency"] == "NYPD"