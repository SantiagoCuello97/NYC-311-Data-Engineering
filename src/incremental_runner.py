"""Reusable, bounded incremental window execution."""

from completed_window import write_checkpoint
from ingest import ingest_records
from window_extraction import extract_window


def run_incremental_window(
    conn,
    lower,
    upper,
    count_source,
    fetch_page,
    *,
    page_size=500,
    max_records=2500,
    apply=False,
    pipeline_name="nyc311",
):
    """Validate a complete source window before any DB writes.

    count_source(lower, upper) must return a nonnegative integer.
    A stable count is not a transactional source snapshot.
    """

    if max_records < 1 or page_size < 1:
        raise ValueError("Invalid extraction limits")

    if lower.tzinfo is None or upper.tzinfo is None:
        raise ValueError("Timezone-aware bounds required")

    if lower >= upper:
        raise ValueError("Invalid window bounds")

    if apply and conn is None:
        raise ValueError("Database connection required for apply")

    # Fast preflight: avoid API calls for invalid or completed windows.
    # The checkpoint is checked again under FOR UPDATE before commit.
    if apply:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT completed_through
                FROM window_sync_state
                WHERE pipeline_name = %s
                """,
                (pipeline_name,),
            )
            state = cur.fetchone()

        if state is None or state[0] is None:
            raise ValueError(
                "Checkpoint missing or not bootstrapped"
            )

        previous = state[0]

        if upper == previous:
            return {
                "status": "SKIPPED",
                "rows_read": 0,
                "pages": 0,
                "checkpoint_advanced": False,
            }

        if lower != previous or upper <= previous:
            raise ValueError("Non-contiguous or backward window")

    before = count_source(lower, upper)

    if type(before) is not int or before < 0:
        raise ValueError("Invalid source count")

    if before > max_records:
        raise ValueError("Source exceeds safety limit")

    extraction = extract_window(
        lower,
        upper,
        fetch_page=fetch_page,
        page_size=page_size,
        max_records=max_records,
    )

    records = extraction["records"]

    if not extraction["complete"]:
        raise RuntimeError("Incomplete extraction")

    if len(records) != before:
        raise ValueError("Source/extraction count mismatch")

    after = count_source(lower, upper)

    if after != before:
        raise ValueError("Source count changed during extraction")

    result = {
        "status": "DRY_RUN",
        "rows_read": len(records),
        "pages": extraction["pages"],
        "source_count_before": before,
        "source_count_after": after,
        "checkpoint_advanced": False,
    }

    if not apply:
        return result

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

        if row is None or row[0] is None:
            raise ValueError(
                "Checkpoint missing or not bootstrapped"
            )

        previous = row[0]

        if upper == previous:
            result["status"] = "SKIPPED"
            return result

        if lower != previous or upper <= previous:
            raise ValueError("Non-contiguous or backward window")

        if records:
            inserted, updated, unchanged = ingest_records(
                conn, records
            )
        else:
            inserted = updated = unchanged = 0

        write_checkpoint(conn, pipeline_name, upper)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (
                    started_at, finished_at, status,
                    rows_read, rows_inserted, rows_updated,
                    rows_unchanged, rows_rejected
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

    result.update({
        "status": "SUCCESS",
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "run_id": run_id,
        "checkpoint_advanced": True,
    })

    return result