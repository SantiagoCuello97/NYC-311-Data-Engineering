import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
REPORT = []

def log(message):
    print(message, flush=True)
    REPORT.append(message)

def main():
    load_dotenv(ROOT / ".env")
    password = os.getenv("DB_PASSWORD")

    if not password:
        raise RuntimeError("DB_PASSWORD is missing")

    sql = (ROOT / "sql" / "002_sync_state.sql").read_text(
        encoding="utf-8-sig"
    )

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
                    pipeline_name,
                    watermark_updated_at,
                    watermark_unique_key,
                    last_success_at
                FROM sync_state
                WHERE pipeline_name = 'nyc311'
            """)

            row = cur.fetchone()

            if row is None:
                raise RuntimeError("Sync state not found")

            log(f"Pipeline: {row[0]}")
            log(f"Watermark timestamp: {row[1]}")
            log(f"Watermark key: {row[2]}")
            log(f"Last success: {row[3]}")

            cur.execute("""
                SELECT COUNT(*) FROM raw_service_requests
            """)
            log(f"Existing raw records: {cur.fetchone()[0]}")

        conn.commit()

    log("SYNC STATE INITIALIZATION: PASS")
    return 0

if __name__ == "__main__":
    code = 1

    try:
        code = main()
    except Exception as exc:
        log(f"FAILED: {type(exc).__name__}")
        log(f"Details: {str(exc).splitlines()[0]}")
    finally:
        path = os.getenv("SERVICEPULSE_REPORT_PATH")
        if path:
            Path(path).write_text(
                "\n".join(REPORT) + "\n",
                encoding="utf-8"
            )

    sys.exit(code)