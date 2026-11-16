"""Atomic ingestion of a validated NYC 311 batch."""

from ingest import ingest_records, parse_timestamp
from sync_state import advance_watermark


def process_batch(conn, records):
    """Write records and checkpoint in one transaction.

    Only use this checkpoint operation after the caller
    has established that the extraction window is complete.
    """

    if not records:
        raise ValueError("Empty batch cannot advance watermark")

    cursors = []

    for record in records:
        timestamp = parse_timestamp(record[":updated_at"])
        key = record["unique_key"]

        if not key:
            raise ValueError("Missing unique_key")

        cursors.append((timestamp, key))

    highest_timestamp, highest_key = max(cursors)

    with conn.transaction():
        inserted, updated, unchanged = ingest_records(
            conn, records
        )

        advanced = advance_watermark(
            conn,
            highest_timestamp,
            highest_key,
        )

    return {
        "rows_read": len(records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "watermark_advanced": advanced,
    }