"""Atomic storage and checkpointing of complete NYC 311 windows."""

from ingest import ingest_records
from window_extraction import extract_window


def write_checkpoint(conn, pipeline_name, upper):
    """Update the completed boundary in the active transaction."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE window_sync_state
            SET completed_through = %s,
                last_success_at = NOW()
            WHERE pipeline_name = %s
            """,
            (upper, pipeline_name),
        )

        if cur.rowcount != 1:
            raise RuntimeError("Checkpoint update failed")


def run_completed_window(
    conn,
    lower,
    upper,
    fetch_page,
    page_size=100,
    max_records=1000,
    pipeline_name="nyc311",
):
    """Extract a full window before atomically writing data and state.

    The first window is explicitly bootstrapped by the caller.
    Subsequent windows must start at completed_through.
    """

    extraction = extract_window(
        lower=lower,
        upper=upper,
        fetch_page=fetch_page,
        page_size=page_size,
        max_records=max_records,
    )

    if not extraction["complete"]:
        raise RuntimeError("Incomplete extraction")

    records = extraction["records"]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT completed_through
                FROM window_sync_state
                WHERE pipeline_name = %s
                FOR UPDATE
                """,
                (pipeline_name,),
            )
            row = cur.fetchone()

        if row is None:
            raise ValueError("Pipeline checkpoint missing")

        previous_upper = row[0]

        if previous_upper is not None:
            if upper == previous_upper and lower < upper:
                return {
                    "status": "SKIPPED",
                    "rows_read": 0,
                    "inserted": 0,
                    "updated": 0,
                    "unchanged": 0,
                    "checkpoint_advanced": False,
                }

            if lower != previous_upper:
                raise ValueError(
                    "Window must start at previous checkpoint"
                )

            if upper <= previous_upper:
                raise ValueError("Window cannot move backward")

        if records:
            inserted, updated, unchanged = ingest_records(
                conn, records
            )
        else:
            inserted = updated = unchanged = 0

        write_checkpoint(conn, pipeline_name, upper)

    return {
        "status": "SUCCESS",
        "rows_read": len(records),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "checkpoint_advanced": True,
    }