"""Apply exactly one bounded incremental window and verify its checkpoint."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from incremental_runner import run_incremental_window
from source_count import count_source_records
from window_extraction import socrata_page, utc_text
from window_planner import plan_next_window

ROOT = Path(__file__).resolve().parents[1]


def connect(*, readonly=False):
    options = (
        "-c default_transaction_read_only=on"
        if readonly else None
    )

    return psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
        options=options,
    )


def read_state(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        row = cur.fetchone()
        if row is None or row[0] is None:
            raise RuntimeError("Checkpoint unavailable")

        cur.execute("""
            SELECT COUNT(*)
            FROM raw_service_requests
        """)
        total = cur.fetchone()[0]

    return row[0].astimezone(timezone.utc), total


def main():
    load_dotenv(ROOT / ".env")

    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("Database credentials unavailable")

    print("=== SERVICEPULSE - APPLY NEXT WINDOW ===")
    print("Mode: APPLY")
    print("Maximum window: 60 minutes")
    print("Maximum records: 2500")
    print("Safety lag: 10 minutes")

    with connect(readonly=True) as conn:
        checkpoint, before_total = read_state(conn)

    now = datetime.now(timezone.utc)
    now = now.replace(
        microsecond=(now.microsecond // 1000) * 1000
    )

    window = plan_next_window(
        checkpoint,
        now,
        window_minutes=60,
        safety_lag_minutes=10,
    )

    print(f"Checkpoint before: {utc_text(checkpoint)}")
    print(f"Raw records before: {before_total}")

    if window is None:
        print("Eligible window: NONE")
        print("Database writes: 0")
        print("APPLY NEXT WINDOW: PASS")
        return 0

    lower, upper = window

    print(f"Window lower: {utc_text(lower)}")
    print(f"Window upper: {utc_text(upper)}")
    print("Checking source and applying window...", flush=True)

    with connect() as conn:
        result = run_incremental_window(
            conn,
            lower,
            upper,
            count_source=count_source_records,
            fetch_page=socrata_page,
            page_size=500,
            max_records=2500,
            apply=True,
        )

    print(f"Result status: {result['status']}")
    print(f"Records processed: {result['rows_read']}")

    if result["status"] == "SUCCESS":
        print(f"Inserted: {result['inserted']}")
        print(f"Updated: {result['updated']}")
        print(f"Unchanged: {result['unchanged']}")
        print(f"Run ID: {result['run_id']}")

    with connect(readonly=True) as conn:
        after_checkpoint, after_total = read_state(conn)

        if result["status"] == "SUCCESS":
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT status, rows_read,
                           rows_inserted, rows_updated,
                           rows_unchanged, rows_rejected
                    FROM ingestion_runs
                    WHERE run_id = %s
                """, (result["run_id"],))
                audit = cur.fetchone()

            expected_audit = (
                "SUCCESS",
                result["rows_read"],
                result["inserted"],
                result["updated"],
                result["unchanged"],
                0,
            )

            if audit != expected_audit:
                raise AssertionError("Audit verification failed")

    expected_checkpoint = (
        upper if result["status"] == "SUCCESS"
        else checkpoint
    )

    if after_checkpoint != expected_checkpoint:
        raise AssertionError("Checkpoint verification failed")

    if after_total != (
        before_total + result.get("inserted", 0)
    ):
        raise AssertionError("Raw count verification failed")

    print(f"Raw records after: {after_total}")
    print(f"Checkpoint after: {utc_text(after_checkpoint)}")
    print("PostgreSQL verification: PASS")
    print("APPLY NEXT WINDOW: PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}")
        print("APPLY NEXT WINDOW: FAILED")
        print("Inspect PostgreSQL state before retrying.")
        sys.exit(1)