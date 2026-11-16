import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from sync_state import advance_watermark, get_watermark

load_dotenv(ROOT / ".env")


def utc(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
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


def test_initial_watermark_is_empty(conn):
    assert get_watermark(conn) == (None, None)


def test_watermark_advances(conn):
    first = utc("2026-11-14T10:00:00Z")

    assert advance_watermark(
        conn, first, "71100001"
    ) is True

    assert get_watermark(conn) == (
        first, "71100001"
    )


def test_same_timestamp_uses_unique_key(conn):
    timestamp = utc("2026-11-14T10:00:00Z")

    assert advance_watermark(
        conn, timestamp, "71100001"
    ) is True

    assert advance_watermark(
        conn, timestamp, "71100002"
    ) is True

    assert advance_watermark(
        conn, timestamp, "71100001"
    ) is False

    assert get_watermark(conn) == (
        timestamp, "71100002"
    )


def test_failed_transaction_rolls_back_watermark(conn):
    original = utc("2026-11-14T10:00:00Z")
    newer = utc("2026-11-15T10:00:00Z")

    advance_watermark(conn, original, "71100001")

    with pytest.raises(RuntimeError):
        with conn.transaction():
            advance_watermark(
                conn, newer, "71100002"
            )
            raise RuntimeError("Simulated batch failure")

    assert get_watermark(conn) == (
        original, "71100001"
    )