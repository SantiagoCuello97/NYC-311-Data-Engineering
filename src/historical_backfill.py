"""Bounded historical NYC311 backfill, separate from incremental checkpoints."""

import argparse
import os
import sys
import time
from datetime import datetime

import psycopg
import requests
from dotenv import load_dotenv
from pathlib import Path

from ingest import ingest_records
from window_extraction import API_URL

ROOT = Path(__file__).resolve().parents[1]
START = "2026-10-01T00:00:00"
END = "2026-10-08T00:00:00"
PAGE_SIZE = 500

load_dotenv(ROOT / ".env")


def connect():
    return psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
    )


def fetch_page(after, limit):
    bounds = (
        f"created_date >= '{START}' "
        f"AND created_date < '{END}'"
    )

    if after is not None:
        date, key = after
        bounds += (
            f" AND (created_date > '{date}' "
            f"OR (created_date = '{date}' "
            f"AND unique_key > '{key}'))"
        )

    params = {
        "$select": (
            "unique_key,created_date,closed_date,"
            "status,agency,complaint_type,:updated_at"
        ),
        "$where": bounds,
        "$order": "created_date ASC, unique_key ASC",
        "$limit": limit,
    }

    for attempt in range(1, 6):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=(5, 90),
            )

            if (
                response.status_code in (429, 500, 502, 503, 504)
                and attempt < 5
            ):
                print(
                    f"HTTP {response.status_code}; retry {attempt}/3",
                    flush=True,
                )
                time.sleep(attempt * 2)
                continue

            response.raise_for_status()
            rows = response.json()

            if not isinstance(rows, list):
                raise ValueError("Unexpected API response")

            return rows

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            print(
                f"Temporary network error: {type(exc).__name__}",
                flush=True,
            )
            if attempt == 5:
                raise
            time.sleep(attempt * 2)

    raise RuntimeError("API retries exhausted")


def controls(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM public.raw_service_requests"
        )
        count = cur.fetchone()[0]

        cur.execute(
            "SELECT completed_through "
            "FROM public.window_sync_state "
            "WHERE pipeline_name = 'nyc311'"
        )
        checkpoint = cur.fetchone()

        cur.execute(
            "SELECT COUNT(*) FROM public.ingestion_runs"
        )
        run_count = cur.fetchone()[0]

    if checkpoint is None:
        raise RuntimeError("Missing incremental checkpoint")

    return count, checkpoint[0], run_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit", type=int, required=True
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue after last saved historical record",
    )
    args = parser.parse_args()

    if not 1 <= args.limit <= 50000:
        raise ValueError("Limit must be between 1 and 50000")

    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("DB_PASSWORD is missing")

    print("=== SERVICEPULSE - HISTORICAL BACKFILL ===")
    print(f"Historical interval: {START} to {END}")
    print(f"Maximum source rows: {args.limit}")
    print(f"Page size: {PAGE_SIZE}")
    print("Mode: APPLY")
    print("Incremental checkpoint: PROTECTED", flush=True)

    processed = 0
    total_inserted = 0
    total_updated = 0
    total_unchanged = 0
    after = None
    completed = False

    with connect() as conn:
        initial_count, checkpoint, run_count = controls(conn)
        print(f"Initial raw rows: {initial_count}")

        if args.resume:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM public.raw_service_requests
                    WHERE raw_payload ->> 'created_date' >= %s
                      AND raw_payload ->> 'created_date' < %s
                    """,
                    (START, END),
                )
                historical_count = cur.fetchone()[0]

                cur.execute(
                    """
                    SELECT
                        raw_payload ->> 'created_date',
                        unique_key
                    FROM public.raw_service_requests
                    WHERE raw_payload ->> 'created_date' >= %s
                      AND raw_payload ->> 'created_date' < %s
                    ORDER BY
                        raw_payload ->> 'created_date' DESC,
                        unique_key DESC
                    LIMIT 1
                    """,
                    (START, END),
                )
                latest = cur.fetchone()

            if historical_count < 26000:
                raise RuntimeError(
                    "Expected at least 26000 committed historical rows"
                )

            if historical_count > args.limit:
                raise RuntimeError(
                    "Stored historical count exceeds target"
                )

            if latest is None:
                raise RuntimeError("Historical resume cursor missing")

            processed = historical_count
            after = (latest[0], latest[1])

            print(f"Existing historical IDs: {historical_count}")
            print(f"Remaining target rows: {args.limit - processed}")
            print("Resume cursor: RECOVERED FROM POSTGRESQL")
            print("Previous pages: SKIPPED", flush=True)

        while processed < args.limit:
            remaining = args.limit - processed
            rows = fetch_page(after, min(PAGE_SIZE, remaining))

            if not rows:
                completed = True
                break

            keys = []

            for row in rows:
                key = row.get("unique_key")
                date = row.get("created_date")

                if not key or not date or ":updated_at" not in row:
                    raise ValueError("Required source field missing")

                if not START <= date < END:
                    raise ValueError("Record outside historical interval")

                position = (date, key)

                if after is not None and position <= after:
                    raise ValueError("Pagination order violation")

                keys.append(key)
                after = position

            if len(set(keys)) != len(keys):
                raise ValueError("Duplicate IDs within page")

            with conn.transaction():
                inserted, updated, unchanged = ingest_records(
                    conn, rows
                )

                if inserted + updated + unchanged != len(rows):
                    raise RuntimeError("UPSERT count mismatch")

                _, current_checkpoint, current_runs = controls(conn)

                if current_checkpoint != checkpoint:
                    raise RuntimeError("Incremental checkpoint changed")

                if current_runs != run_count:
                    raise RuntimeError("Ingestion run history changed")

            processed += len(rows)
            total_inserted += inserted
            total_updated += updated
            total_unchanged += unchanged

            print(
                f"Progress: {processed}/{args.limit}; "
                f"inserted={total_inserted}; "
                f"updated={total_updated}; "
                f"unchanged={total_unchanged}",
                flush=True,
            )

            if len(rows) < min(PAGE_SIZE, remaining):
                completed = True
                break

        final_count, final_checkpoint, final_runs = controls(conn)

        if final_checkpoint != checkpoint or final_runs != run_count:
            raise RuntimeError("State changed unexpectedly")

        if final_count != initial_count + total_inserted:
            raise RuntimeError("Unexpected final row count")

    print("")
    print("--- SUMMARY ---")
    print(f"Processed source rows: {processed}")
    print(f"Inserted: {total_inserted}")
    print(f"Updated: {total_updated}")
    print(f"Unchanged: {total_unchanged}")
    print(f"Final PostgreSQL rows: {final_count}")
    print("Incremental checkpoint: UNCHANGED")
    print("Ingestion runs: UNCHANGED")
    print("Transaction mode: COMMITTED PER PAGE")
    print("BACKFILL: SUCCESS")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}")
        print(
            "BACKFILL: FAILED; previously committed pages may remain"
        )
        sys.exit(1)