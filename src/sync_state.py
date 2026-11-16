"""Persistent synchronization cursor for NYC 311."""

from datetime import datetime


def get_watermark(conn, pipeline_name="nyc311"):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT watermark_updated_at, watermark_unique_key
            FROM sync_state
            WHERE pipeline_name = %s
            """,
            (pipeline_name,),
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError("Pipeline sync state not found")

    return row


def advance_watermark(
    conn,
    timestamp: datetime,
    unique_key: str,
    pipeline_name="nyc311",
):
    """Advance a cursor inside the caller's transaction.

    The caller must commit only after the corresponding
    ingestion batch has completed successfully.
    """
    if timestamp.tzinfo is None:
        raise ValueError("Watermark must be timezone-aware")

    if not unique_key:
        raise ValueError("Watermark key cannot be empty")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT watermark_updated_at, watermark_unique_key
            FROM sync_state
            WHERE pipeline_name = %s
            FOR UPDATE
            """,
            (pipeline_name,),
        )
        current = cur.fetchone()

        if current is None:
            raise ValueError("Pipeline sync state not found")

        old_time, old_key = current

        if old_time is not None:
            old_cursor = (old_time, old_key)
            new_cursor = (timestamp, unique_key)

            if new_cursor <= old_cursor:
                return False

        cur.execute(
            """
            UPDATE sync_state
            SET watermark_updated_at = %s,
                watermark_unique_key = %s,
                last_success_at = NOW()
            WHERE pipeline_name = %s
            """,
            (timestamp, unique_key, pipeline_name),
        )

    return True