"""Bounded live NYC 311 ingestion with an atomic window checkpoint."""

import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

from ingest import ingest_records
from completed_window import write_checkpoint
from window_extraction import extract_window, utc_text


ROOT = Path(__file__).resolve().parents[1]
API = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

LOWER = datetime.fromisoformat(
    "2026-11-15T02:58:20.910+00:00"
)
UPPER = LOWER + timedelta(milliseconds=1)

PAGE_SIZE = 500
MAX_RECORDS = 2500

REPORT = []


def log(message):
    print(message, flush=True)
    REPORT.append(message)


def count_source():
    params = {
        "$select": "count(*) AS total",
        "$where": (
            f":updated_at >= '{utc_text(LOWER)}' "
            f"AND :updated_at < '{utc_text(UPPER)}'"
        ),
    }

    for attempt in range(1, 4):
        try:
            response = requests.get(
                API,
                params=params,
                timeout=(5, 30),
            )

            if response.status_code in (
                429, 500, 502, 503, 504
            ):
                if attempt < 3:
                    time.sleep(attempt)
                    continue

            response.raise_for_status()
            rows = response.json()

            if (
                not isinstance(rows, list)
                or len(rows) != 1
                or "total" not in rows[0]
            ):
                raise ValueError("Invalid source count response")

            total = int(rows[0]["total"])

            if total < 0:
                raise ValueError("Negative source count")

            return total

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ):
            if attempt == 3:
                raise

            time.sleep(attempt)

    raise RuntimeError("Source count retries exhausted")


def main():
    log("=== SERVICEPULSE - LIVE INCREMENTAL WINDOW ===")
    log(f"Window lower: {utc_text(LOWER)}")
    log(f"Window upper: {utc_text(UPPER)}")
    log(f"Maximum records: {MAX_RECORDS}")

    load_dotenv(ROOT / ".env")

    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("Database credentials not configured")

    log("")
    log("--- SOURCE PREFLIGHT ---")

    expected = count_source()
    log(f"Source count before extraction: {expected}")

    if expected == 0:
        raise ValueError("Unexpected empty source window")

    if expected > MAX_RECORDS:
        raise ValueError("Source exceeds safety limit")

    log("")
    log("--- EXTRACTING ALL PAGES ---")

    extraction = extract_window(
        lower=LOWER,
        upper=UPPER,
        page_size=PAGE_SIZE,
        max_records=MAX_RECORDS,
    )

    records = extraction["records"]

    log(f"Pages extracted: {extraction['pages']}")
    log(f"Records extracted: {len(records)}")

    if not extraction["complete"]:
        raise ValueError("Extraction not complete")

    if len(records) != expected:
        raise ValueError("Source/extraction count mismatch")

    after_count = count_source()
    log(f"Source count after extraction: {after_count}")

    if after_count != len(records):
        raise ValueError("Source count changed during extraction")

    log("Source count reconciliation: PASS")

    log("")
    log("--- POSTGRESQL TRANSACTION ---")

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        autocommit=True,
    ) as conn:

        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT completed_through
                    FROM window_sync_state
                    WHERE pipeline_name = 'nyc311'
                    FOR UPDATE
                    """
                )
                state = cur.fetchone()

            if state is None:
                raise RuntimeError("Window checkpoint missing")

            current_checkpoint = state[0]

            if current_checkpoint == UPPER:
                log("Window already committed: SKIPPED")
                return 0

            if current_checkpoint is not None:
                raise RuntimeError(
                    "Unexpected checkpoint. Refusing bootstrap."
                )

            inserted, updated, unchanged = ingest_records(
                conn, records
            )

            write_checkpoint(conn, "nyc311", UPPER)

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_runs (
                        started_at,
                        finished_at,
                        status,
                        rows_read,
                        rows_inserted,
                        rows_updated,
                        rows_unchanged,
                        rows_rejected
                    )
                    VALUES (
                        NOW(), NOW(), 'SUCCESS',
                        %s, %s, %s, %s, 0
                    )
                    RETURNING run_id
                    """,
                    (
                        len(records),
                        inserted,
                        updated,
                        unchanged,
                    ),
                )
                run_id = cur.fetchone()[0]

    log(f"Run ID: {run_id}")
    log(f"Inserted: {inserted}")
    log(f"Updated: {updated}")
    log(f"Unchanged: {unchanged}")
    log(f"Checkpoint completed through: {utc_text(UPPER)}")
    log("Transaction: COMMITTED")
    log("LIVE WINDOW INGESTION: SUCCESS")
    return 0


if __name__ == "__main__":
    code = 1

    try:
        code = main()
    except Exception as exc:
        log("LIVE WINDOW INGESTION: FAILED")
        log(f"Error type: {type(exc).__name__}")
        log("No checkpoint advancement confirmed.")
    finally:
        output = os.getenv("SERVICEPULSE_REPORT_PATH")

        if output:
            Path(output).write_text(
                "\n".join(REPORT) + "\n",
                encoding="utf-8",
            )

    sys.exit(code)