import os
from datetime import datetime
import sys
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import incremental_batch
from incremental_batch import process_batch
from sync_state import get_watermark

load_dotenv(ROOT / ".env")


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


def record(key="SP-BATCH-001", status="Open"):
    return {
        "unique_key": key,
        "created_date": "2026-11-14T08:00:00.000",
        "closed_date": None,
        "status": status,
        "agency": "DOT",
        "complaint_type": "Street Condition",
        ":updated_at": "2026-11-14T10:00:00Z",
    }


def row_count(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw_service_requests"
        )
        return cur.fetchone()[0]


def test_batch_updates_records_and_watermark(conn):
    result = process_batch(
        conn,
        [record("SP-BATCH-001"), record("SP-BATCH-002")],
    )

    assert result["inserted"] == 2
    assert result["watermark_advanced"] is True
    assert row_count(conn) == 2

    timestamp, key = get_watermark(conn)

    assert timestamp == datetime.fromisoformat(
        "2026-11-14T10:00:00+00:00"
    )
    assert key == "SP-BATCH-002"


def test_batch_replay_is_idempotent(conn):
    records = [
        record("SP-BATCH-001"),
        record("SP-BATCH-002"),
    ]

    process_batch(conn, records)
    result = process_batch(conn, records)

    assert result["inserted"] == 0
    assert result["updated"] == 0
    assert result["unchanged"] == 2
    assert result["watermark_advanced"] is False
    assert row_count(conn) == 2


def test_failure_rolls_back_records_and_watermark(
    conn, monkeypatch
):
    def simulated_failure(*args, **kwargs):
        raise RuntimeError("Simulated checkpoint failure")

    monkeypatch.setattr(
        incremental_batch,
        "advance_watermark",
        simulated_failure,
    )

    with pytest.raises(RuntimeError):
        process_batch(conn, [record()])

    assert row_count(conn) == 0
    assert get_watermark(conn) == (None, None)