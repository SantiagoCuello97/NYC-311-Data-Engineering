"""Pure comparison logic for historical source reconciliation."""


def compare_records(source_records, stored_records):
    """Compare two complete record sets by unique_key.

    stored_records is a mapping from ID to raw JSON payload.
    """

    source_by_id = {}

    for record in source_records:
        key = record.get("unique_key")

        if not isinstance(key, str) or not key:
            raise ValueError("Invalid source unique_key")

        if key in source_by_id:
            raise ValueError("Duplicate source unique_key")

        source_by_id[key] = record

    source_ids = set(source_by_id)
    stored_ids = set(stored_records)

    missing = sorted(source_ids - stored_ids)
    extra = sorted(stored_ids - source_ids)

    changed = sorted(
        key
        for key in source_ids & stored_ids
        if source_by_id[key] != stored_records[key]
    )

    return {
        "source_count": len(source_ids),
        "stored_count": len(stored_ids),
        "matching": (
            len(source_ids & stored_ids) - len(changed)
        ),
        "missing": missing,
        "changed": changed,
        "extra": extra,
        "is_match": not (missing or changed or extra),
    }