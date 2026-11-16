"""Read-only five-ID reconciliation against NYC 311."""

import os
import sys
import time
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

from id_reconciliation import compare_known_records
from window_extraction import API_URL

ROOT = Path(__file__).resolve().parents[1]
SELECT = (
    "unique_key,created_date,closed_date,"
    "status,agency,complaint_type,:updated_at"
)


def fetch_by_ids(keys):
    if not keys or len(keys) > 5:
        raise ValueError("Expected 1 to 5 identifiers")

    if not all(k.isdigit() for k in keys):
        raise ValueError("Invalid identifier format")

    quoted = ",".join(f"'{k}'" for k in keys)

    params = {
        "$select": SELECT,
        "$where": f"unique_key in ({quoted})",
        "$limit": 10,
    }

    for attempt in range(1, 4):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=(5, 35),
            )

            if (
                response.status_code
                in (429, 500, 502, 503, 504)
                and attempt < 3
            ):
                print(
                    f"Retry after HTTP {response.status_code}",
                    flush=True,
                )
                time.sleep(attempt)
                continue

            response.raise_for_status()
            rows = response.json()

            if not isinstance(rows, list):
                raise ValueError("Invalid API response")

            return rows

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            print(
                f"Network attempt {attempt}/3: "
                f"{type(exc).__name__}",
                flush=True,
            )

            if attempt == 3:
                raise

            time.sleep(attempt)

    raise RuntimeError("Retries exhausted")


def main():
    print("=== SERVICEPULSE - ID RECONCILIATION ===")
    print("Mode: READ ONLY")
    print("Maximum IDs: 5")

    load_dotenv(ROOT / ".env")

    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("Database credentials missing")

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=os.environ["DB_PASSWORD"],
        connect_timeout=10,
        options="-c default_transaction_read_only=on",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT unique_key, raw_payload
                FROM raw_service_requests
                ORDER BY unique_key
                LIMIT 5
            """)
            rows = cur.fetchall()

    if len(rows) != 5:
        raise ValueError("Expected five stored records")

    stored = dict(rows)
    keys = list(stored)

    print("Retrieving current source records...", flush=True)
    source = fetch_by_ids(keys)
    results = compare_known_records(stored, source)

    counts = {
        "UNCHANGED": 0,
        "TIMESTAMP_ONLY": 0,
        "BUSINESS_CHANGED": 0,
        "STALE_SOURCE": 0,
        "NOT_FOUND": 0,
    }

    for key, result in results.items():
        status = result["status"]
        counts[status] += 1

        print(
            f"ID {key}: {status}; "
            f"changed business fields="
            f"{','.join(result['changed_fields']) or 'none'}"
        )

    print("")
    print("--- SUMMARY ---")
    print(f"Stored IDs reviewed: {len(stored)}")
    print(f"Source IDs returned: {len(source)}")

    for status, count in counts.items():
        print(f"{status}: {count}")

    print("Database writes: 0")
    print("ID RECONCILIATION: PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Error type: {type(exc).__name__}")
        print("Database writes: 0")
        print("ID RECONCILIATION: FAILED")
        sys.exit(1)