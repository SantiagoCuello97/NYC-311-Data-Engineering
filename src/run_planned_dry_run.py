"""Plan and inspect the next NYC 311 window without DB writes."""

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


def main():
    print("=== SERVICEPULSE - PLANNED LIVE DRY RUN ===")
    print("Mode: READ ONLY")
    print("Window limit: 60 minutes")
    print("Safety lag: 10 minutes")
    print("Maximum records: 2500")
    print("Page size: 500")
    print("", flush=True)

    load_dotenv(ROOT / ".env")

    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("Database credentials missing")

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=password,
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT completed_through
                FROM window_sync_state
                WHERE pipeline_name = 'nyc311'
                """
            )
            row = cur.fetchone()

    if row is None or row[0] is None:
        raise ValueError("Completed checkpoint unavailable")

    checkpoint = row[0].astimezone(timezone.utc)
    now = datetime.now(timezone.utc)

    # The Socrata queries operate at millisecond precision.
    now = now.replace(
        microsecond=(now.microsecond // 1000) * 1000
    )

    print(f"Current checkpoint: {utc_text(checkpoint)}")

    window = plan_next_window(
        checkpoint,
        now,
        window_minutes=60,
        safety_lag_minutes=10,
    )

    if window is None:
        print("Eligible window: NONE")
        print("No data extraction required")
        print("Database writes: 0")
        print("PLANNED DRY RUN: PASS")
        return 0

    lower, upper = window

    print(f"Window lower: {utc_text(lower)}")
    print(f"Window upper: {utc_text(upper)}")
    print("Checking live source...", flush=True)

    result = run_incremental_window(
        conn=None,
        lower=lower,
        upper=upper,
        count_source=count_source_records,
        fetch_page=socrata_page,
        page_size=500,
        max_records=2500,
        apply=False,
    )

    print(f"Result status: {result['status']}")
    print(f"Source count before: {result['source_count_before']}")
    print(f"Pages extracted: {result['pages']}")
    print(f"Records extracted: {result['rows_read']}")
    print(f"Source count after: {result['source_count_after']}")
    print(f"Checkpoint advanced: {result['checkpoint_advanced']}")
    print("Database writes: 0")
    print("PLANNED DRY RUN: PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}")
        print("Database writes: 0")
        print("PLANNED DRY RUN: FAILED")
        sys.exit(1)