"""Integration of bounded extraction and atomic storage.

This module demonstrates the component integration.
It must not be used as a production incremental scheduler:
a completed-window checkpoint is still required.
"""

from window_extraction import extract_window
from incremental_batch import process_batch


def run_window(
    conn,
    lower,
    upper,
    fetch_page,
    page_size=100,
    max_records=1000,
):
    """Extract completely before changing PostgreSQL."""

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

    if not records:
        return {
            "pages": extraction["pages"],
            "rows_read": 0,
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "watermark_advanced": False,
        }

    result = process_batch(conn, records)
    result["pages"] = extraction["pages"]

    return result