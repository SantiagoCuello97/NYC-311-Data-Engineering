"""Initialize the completed-window checkpoint."""

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
report = []


def log(message):
    print(message, flush=True)
    report.append(message)


def main():
    load_dotenv(ROOT / ".env")
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD missing")

    sql = (
        ROOT / "sql" / "003_window_checkpoint.sql"
    ).read_text(encoding="utf-8-sig")

    with psycopg.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "servicepulse"),
        user=os.getenv("DB_USER", "servicepulse_user"),
        password=password,
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

            cur.execute("""
                SELECT
                    completed_through,
                    last_success_at
                FROM window_sync_state
                WHERE pipeline_name = 'nyc311'
            """)

            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Checkpoint not found")

            log("Checkpoint table: FOUND")
            log(f"Completed through: {row[0]}")
            log(f"Last success: {row[1]}")

            cur.execute("""
                SELECT COUNT(*)
                FROM raw_service_requests
            """)
            log(f"Raw records: {cur.fetchone()[0]}")

            cur.execute("""
                SELECT watermark_updated_at
                FROM sync_state
                WHERE pipeline_name = 'nyc311'
            """)
            state = cur.fetchone()

            if state is None:
                raise RuntimeError("Existing sync state missing")

            log(f"Original watermark: {state[0]}")

        conn.commit()

    log("WINDOW CHECKPOINT INITIALIZATION: PASS")
    return 0


if __name__ == "__main__":
    try:
        code = main()
    except Exception as exc:
        log(f"FAILED: {type(exc).__name__}")
        code = 1
    finally:
        output = os.getenv("SERVICEPULSE_REPORT_PATH")
        if output:
            Path(output).write_text(
                "\n".join(report) + "\n",
                encoding="utf-8",
            )

    sys.exit(code)