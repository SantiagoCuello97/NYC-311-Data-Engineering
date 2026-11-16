"""Apply and verify a bounded five-ID NYC311 reconciliation."""

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from id_reconciliation import classify_record
from reconciliation_apply import reconcile_known_records
from run_id_reconciliation import fetch_by_ids
from window_extraction import timestamp

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def connect(readonly=False):
    return psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
        options=(
            "-c default_transaction_read_only=on"
            if readonly else None
        ),
    )


def read_controls(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT completed_through
            FROM window_sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        checkpoint = cur.fetchone()

        cur.execute("""
            SELECT watermark_updated_at, watermark_unique_key
            FROM sync_state
            WHERE pipeline_name = 'nyc311'
        """)
        watermark = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM raw_service_requests")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM ingestion_runs")
        runs = cur.fetchone()[0]

    if checkpoint is None or watermark is None:
        raise RuntimeError("Expected state row missing")

    return checkpoint[0], watermark, total, runs


def main():
    print("=== SERVICEPULSE - STEP 60 LIVE RECONCILIATION ===")
    print("Mode: APPLY (five known IDs maximum)", flush=True)

    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("Database configuration missing")

    # Read a bounded sample without modifying the database.
    with connect(readonly=True) as conn:
        before_controls = read_controls(conn)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT unique_key, raw_payload
                FROM raw_service_requests
                ORDER BY unique_key
                LIMIT 5
            """)
            selected = cur.fetchall()

    if len(selected) != 5:
        raise RuntimeError("Expected exactly five stored IDs")

    snapshot = dict(selected)
    keys = list(snapshot)

    print(f"Selected IDs: {len(keys)}")
    print(f"Raw rows before: {before_controls[2]}")
    print("Fetching source versions...", flush=True)

    source_rows = fetch_by_ids(keys)
    source_by_id = {
        row["unique_key"]: row for row in source_rows
    }

    # Require all five records to be available for this
    # particular controlled demonstration.
    if len(source_by_id) != 5 or set(source_by_id) != set(keys):
        raise RuntimeError("Source ID set incomplete or invalid")

    print(f"Source IDs found: {len(source_rows)}")
    print("Applying under PostgreSQL transaction...", flush=True)

    with connect() as conn:
        with conn.transaction():
            # Refuse changes if the baseline unexpectedly moved.
            current_controls = read_controls(conn)
            if current_controls != before_controls:
                raise RuntimeError("Database baseline changed")

            result = reconcile_known_records(
                conn,
                snapshot,
                source_rows,
            )

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT unique_key, source_updated_at, raw_payload
                    FROM raw_service_requests
                    WHERE unique_key = ANY(%s)
                    ORDER BY unique_key
                """, (keys,))
                verified = cur.fetchall()

            if len(verified) != 5:
                raise AssertionError("Verified row count mismatch")

            for key, stored_time, stored_payload in verified:
                current = source_by_id[key]
                current_time = timestamp(current[":updated_at"])

                if stored_time < current_time:
                    raise AssertionError(
                        "A newer source timestamp was not applied"
                    )

                if stored_time == current_time:
                    if stored_payload != current:
                        raise AssertionError(
                            "Source payload verification failed"
                        )

                print(
                    f"ID {key}: "
                    f"{classify_record(snapshot[key], current)['status']}",
                    flush=True,
                )

            after_controls = read_controls(conn)

            if after_controls != before_controls:
                raise AssertionError(
                    "Checkpoint, audit or row count changed"
                )

            if result["updated"] > 5 or result["deleted"] != 0:
                raise AssertionError("Unexpected reconciliation metrics")

            print("Transactional verification: PASS", flush=True)

    # This line is reached only after the transaction commits.
    print("")
    print(f"Updated: {result['updated']}")
    print(f"Timestamp only: {result['timestamp_only']}")
    print(f"Business changed: {result['business_changed']}")
    print(f"Stale source: {result['stale_source']}")
    print(f"Not found: {result['not_found']}")
    print(f"Deleted: {result['deleted']}")
    print("Raw row count: UNCHANGED")
    print("Incremental checkpoint: UNCHANGED")
    print("Legacy watermark: UNCHANGED")
    print("Ingestion run count: UNCHANGED")
    print("Transaction: COMMITTED")
    print("STEP 60: SUCCESS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}")
        print("STEP 60: FAILED")
        print("Inspect database state before retrying.")
        sys.exit(1)