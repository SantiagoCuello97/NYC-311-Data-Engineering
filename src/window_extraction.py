"""Bounded, descending keyset extraction for NYC 311."""

from datetime import datetime, timezone
import time

import requests

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

FIELDS = (
    "unique_key,created_date,closed_date,"
    "status,agency,complaint_type,:updated_at"
)


def timestamp(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def utc_text(value):
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def socrata_page(lower, upper, after, limit):
    """Fetch one page, retrying transient failures."""

    where = (
        f":updated_at >= '{utc_text(lower)}' "
        f"AND :updated_at < '{utc_text(upper)}'"
    )

    if after is not None:
        ts, key = after

        if not key.isdigit():
            raise ValueError("Invalid cursor key")

        where += (
            f" AND (:updated_at < '{utc_text(ts)}' "
            f"OR (:updated_at = '{utc_text(ts)}' "
            f"AND unique_key < '{key}'))"
        )

    params = {
        "$select": FIELDS,
        "$where": where,
        "$order": ":updated_at DESC,unique_key DESC",
        "$limit": limit,
    }

    for attempt in range(3):
        try:
            response = requests.get(
                API_URL,
                params=params,
                timeout=(5, 20),
            )

            if response.status_code in (
                429, 500, 502, 503, 504
            ):
                if attempt < 2:
                    time.sleep(attempt + 1)
                    continue

            response.raise_for_status()

            rows = response.json()

            if not isinstance(rows, list):
                raise ValueError("Expected JSON array")

            return rows

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ):
            if attempt == 2:
                raise

            time.sleep(attempt + 1)

    raise RuntimeError("API retries exhausted")


def extract_window(
    lower,
    upper,
    fetch_page=socrata_page,
    page_size=100,
    max_records=1000,
):
    """Return only a complete, verified extraction window.

    The interval is [lower, upper). The source is live,
    so this is not a database snapshot.
    """

    if lower.tzinfo is None or upper.tzinfo is None:
        raise ValueError("Timezone-aware bounds required")

    if lower >= upper:
        raise ValueError("Invalid extraction window")

    if page_size < 1 or max_records < 1:
        raise ValueError("Invalid extraction limits")

    records = []
    seen = set()
    after = None
    pages = 0

    while True:
        page = fetch_page(
            lower, upper, after, page_size
        )
        pages += 1

        if not isinstance(page, list):
            raise ValueError("Invalid page response")

        if len(page) > page_size:
            raise ValueError("Page size exceeded")

        for row in page:
            if not isinstance(row, dict):
                raise ValueError("Invalid record")

            key = row.get("unique_key")
            raw_time = row.get(":updated_at")

            if not isinstance(key, str) or not key.isdigit():
                raise ValueError("Invalid unique_key")

            if not isinstance(raw_time, str):
                raise ValueError("Missing update timestamp")

            current = (timestamp(raw_time), key)

            if not lower <= current[0] < upper:
                raise ValueError("Record outside window")

            if after is not None and current >= after:
                raise ValueError("Cursor did not decrease")

            if key in seen:
                raise ValueError("Duplicate unique_key")

            if len(records) >= max_records:
                raise ValueError("Extraction limit exceeded")

            records.append(row)
            seen.add(key)
            after = current

        if len(page) < page_size:
            break

    return {
        "records": records,
        "pages": pages,
        "count": len(records),
        "complete": True,
    }