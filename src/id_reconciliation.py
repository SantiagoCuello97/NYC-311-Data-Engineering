"""Classify NYC 311 changes by stable request identifier."""

from window_extraction import timestamp

UPDATE_FIELD = ":updated_at"


def classify_record(stored, current):
    """Classify an existing record without changing either payload."""

    if stored.get("unique_key") != current.get("unique_key"):
        raise ValueError("Record ID mismatch")

    if UPDATE_FIELD not in stored or UPDATE_FIELD not in current:
        raise ValueError("Missing update timestamp")

    old_ts = timestamp(stored[UPDATE_FIELD])
    new_ts = timestamp(current[UPDATE_FIELD])

    old_business = {
        k: v for k, v in stored.items()
        if k != UPDATE_FIELD
    }
    new_business = {
        k: v for k, v in current.items()
        if k != UPDATE_FIELD
    }

    changed_fields = sorted(
        k
        for k in old_business.keys() | new_business.keys()
        if old_business.get(k) != new_business.get(k)
    )

    if new_ts < old_ts:
        status = "STALE_SOURCE"
    elif changed_fields:
        status = "BUSINESS_CHANGED"
    elif new_ts > old_ts:
        status = "TIMESTAMP_ONLY"
    else:
        status = "UNCHANGED"

    return {
        "status": status,
        "changed_fields": changed_fields,
        "timestamp_changed": new_ts != old_ts,
    }


def compare_known_records(stored_by_id, source_rows):
    """Compare a bounded group of known IDs.

    Missing source IDs are reported, never interpreted
    automatically as deletions.
    """

    source_by_id = {}

    for record in source_rows:
        key = record.get("unique_key")

        if not isinstance(key, str) or not key:
            raise ValueError("Invalid source record ID")

        if key in source_by_id:
            raise ValueError("Duplicate source ID")

        if key not in stored_by_id:
            raise ValueError("Unexpected source ID")

        source_by_id[key] = record

    results = {}

    for key, stored in stored_by_id.items():
        if key not in source_by_id:
            results[key] = {
                "status": "NOT_FOUND",
                "changed_fields": [],
                "timestamp_changed": False,
            }
        else:
            results[key] = classify_record(
                stored, source_by_id[key]
            )

    return results