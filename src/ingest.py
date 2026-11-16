from dotenv import load_dotenv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg
import requests
from psycopg.types.json import Jsonb


API_URL = (
    "https://data.cityofnewyork.us/"
    "resource/erm2-nwe9.json"
)

FIELDS = (
    "unique_key,created_date,closed_date,"
    "status,agency,complaint_type,:updated_at"
)

report = []


def log(message):
    print(message, flush=True)
    report.append(message)


def parse_timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def fetch_records():
    params = {
        "$select": FIELDS,
        "$order": ":updated_at DESC,unique_key DESC",
        "$limit": 20,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    records = response.json()

    if not isinstance(records, list):
        raise ValueError("Expected JSON array")

    if not records:
        raise ValueError("API returned no records")

    keys = set()

    for record in records:
        key = record.get("unique_key")
        updated = record.get(":updated_at")

        if not key or not updated:
            raise ValueError(
                "Missing unique_key or :updated_at"
            )

        parse_timestamp(updated)

        if key in keys:
            raise ValueError("Duplicate key in API response")

        keys.add(key)

    return records


def ingest_records(conn, records):
    inserted = 0
    updated = 0
    unchanged = 0

    with conn.cursor() as cur:
        for record in records:
            key = record["unique_key"]
            source_updated = parse_timestamp(
                record[":updated_at"]
            )

            cur.execute(
                """
                SELECT source_updated_at, raw_payload
                FROM raw_service_requests
                WHERE unique_key = %s
                FOR UPDATE
                """,
                (key,),
            )

            existing = cur.fetchone()

            if existing:
                previous_timestamp, previous_payload = existing

                if (
                    source_updated < previous_timestamp
                    or (
                        source_updated == previous_timestamp
                        and previous_payload == record
                    )
                ):
                    unchanged += 1
                    continue

            cur.execute(
                """
                INSERT INTO raw_service_requests (
                    unique_key,
                    source_updated_at,
                    created_date,
                    closed_date,
                    status,
                    agency,
                    complaint_type,
                    raw_payload
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (unique_key)
                DO UPDATE SET
                    source_updated_at =
                        EXCLUDED.source_updated_at,
                    created_date =
                        EXCLUDED.created_date,
                    closed_date =
                        EXCLUDED.closed_date,
                    status =
                        EXCLUDED.status,
                    agency =
                        EXCLUDED.agency,
                    complaint_type =
                        EXCLUDED.complaint_type,
                    raw_payload =
                        EXCLUDED.raw_payload,
                    last_seen_at = NOW()
                """,
                (
                    key,
                    source_updated,
                    record.get("created_date"),
                    record.get("closed_date"),
                    record.get("status"),
                    record.get("agency"),
                    record.get("complaint_type"),
                    Jsonb(record),
                ),
            )

            if existing:
                updated += 1
            else:
                inserted += 1

    return inserted, updated, unchanged


def main():
    log("=== SERVICEPULSE - FIRST INGESTION ===")

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    password = os.environ.get("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD is not configured")

    log("")
    log("--- FETCHING NYC 311 ---")

    records = fetch_records()
    log(f"API records received: {len(records)}")

    log("")
    log("--- POSTGRESQL INGESTION ---")

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=password,
        connect_timeout=10,
    ) as conn:

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (status)
                VALUES ('RUNNING')
                RETURNING run_id
                """
            )
            run_id = cur.fetchone()[0]

        conn.commit()

        try:
            with conn.transaction():
                inserted, updated, unchanged = ingest_records(
                    conn, records
                )

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE ingestion_runs
                        SET status = 'SUCCESS',
                            finished_at = NOW(),
                            rows_read = %s,
                            rows_inserted = %s,
                            rows_updated = %s,
                            rows_unchanged = %s
                        WHERE run_id = %s
                        """,
                        (
                            len(records),
                            inserted,
                            updated,
                            unchanged,
                            run_id,
                        ),
                    )
        except Exception as exc:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE ingestion_runs
                        SET status = 'FAILED',
                            finished_at = NOW(),
                            error_message = %s
                        WHERE run_id = %s
                        """,
                        (type(exc).__name__, run_id),
                    )
            raise

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw_service_requests"
            )
            total = cur.fetchone()[0]

    log(f"Run ID: {run_id}")
    log(f"Inserted: {inserted}")
    log(f"Updated: {updated}")
    log(f"Unchanged: {unchanged}")
    log(f"Total stored records: {total}")
    log("Ingestion status: SUCCESS")

    return 0


if __name__ == "__main__":
    try:
        result = main()
    except Exception as exc:
        log(f"FAILED: {type(exc).__name__}")
        result = 1
    finally:
        output = os.environ.get(
            "SERVICEPULSE_REPORT_PATH"
        )
        if output:
            Path(output).write_text(
                "\n".join(report) + "\n",
                encoding="utf-8",
            )

    sys.exit(result)