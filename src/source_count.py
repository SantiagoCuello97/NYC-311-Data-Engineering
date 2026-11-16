"""Bounded NYC 311 source count with transient-error retries."""

import time
import requests

from window_extraction import API_URL, utc_text


def count_source_records(
    lower,
    upper,
    *,
    get=requests.get,
    sleep=time.sleep,
):
    """Count records in the half-open interval [lower, upper)."""

    if (
        lower.tzinfo is None
        or upper.tzinfo is None
        or lower >= upper
    ):
        raise ValueError("Invalid timezone-aware window")

    params = {
        "$select": "count(*) AS total",
        "$where": (
            f":updated_at >= '{utc_text(lower)}' "
            f"AND :updated_at < '{utc_text(upper)}'"
        ),
    }

    for attempt in range(3):
        try:
            response = get(
                API_URL,
                params=params,
                timeout=(5, 30),
            )

            if (
                response.status_code
                in (429, 500, 502, 503, 504)
                and attempt < 2
            ):
                sleep(attempt + 1)
                continue

            response.raise_for_status()
            rows = response.json()

            if (
                not isinstance(rows, list)
                or len(rows) != 1
                or not isinstance(rows[0], dict)
            ):
                raise ValueError("Unexpected count response")

            value = rows[0].get("total")

            if (
                isinstance(value, bool)
                or not isinstance(value, (str, int))
                or not str(value).isdigit()
            ):
                raise ValueError("Invalid source count")

            return int(value)

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ):
            if attempt == 2:
                raise

            sleep(attempt + 1)

    raise RuntimeError("Source count retries exhausted")