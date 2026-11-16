import getpass
import os
import sys
from pathlib import Path

import psycopg


PROJECT = Path(__file__).resolve().parents[1]
SQL_FILE = PROJECT / "sql" / "001_init.sql"
report = []


def log(message):
    print(message, flush=True)
    report.append(message)


def main():
    log("=== SERVICEPULSE - DATABASE INITIALIZATION ===")

    password = getpass.getpass(
        "Password for servicepulse_user: "
    )

    with psycopg.connect(
        host="127.0.0.1",
        port=5432,
        dbname="servicepulse",
        user="servicepulse_user",
        password=password,
        connect_timeout=10,
    ) as conn:
        log("PostgreSQL connection: SUCCESS")

        sql = SQL_FILE.read_text(encoding="utf-8")

        with conn.cursor() as cur:
            cur.execute(sql)

        conn.commit()
        log("Schema initialization: SUCCESS")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN (
                      'raw_service_requests',
                      'ingestion_runs'
                  )
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                log(f"Table found: {table}")

            if set(tables) != {
                "raw_service_requests",
                "ingestion_runs",
            }:
                raise RuntimeError("Missing required tables")

            cur.execute(
                "SELECT COUNT(*) FROM raw_service_requests"
            )
            log(f"Raw records: {cur.fetchone()[0]}")

            cur.execute(
                "SELECT COUNT(*) FROM ingestion_runs"
            )
            log(f"Ingestion runs: {cur.fetchone()[0]}")

    log("ALL DATABASE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except Exception as exc:
        log(f"FAILED: {type(exc).__name__}")
        log(f"Error detail: {str(exc)}")
        exit_code = 1
    finally:
        output = os.environ.get("SERVICEPULSE_REPORT_PATH")
        if output:
            Path(output).write_text(
                "\n".join(report) + "\n",
                encoding="utf-8",
            )

    sys.exit(exit_code)
