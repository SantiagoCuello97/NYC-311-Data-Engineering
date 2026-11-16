"""Conservative, transaction-safe known-ID reconciliation."""

from id_reconciliation import compare_known_records
from ingest import ingest_records


def reconcile_known_records(conn, stored_by_id, source_rows):
    """Re-read and lock current DB versions before reconciliation.

    stored_by_id identifies the records selected for review.
    It is not trusted as the authoritative current DB state.

    Missing source records are retained.
    Missing database records are rejected, never inserted.
    No transaction is explicitly committed here.
    """

    # Validate source IDs against the requested snapshot first.
    compare_known_records(stored_by_id, source_rows)

    keys = sorted(stored_by_id)

    counts = {
        "timestamp_only": 0,
        "business_changed": 0,
        "unchanged": 0,
        "stale_source": 0,
        "not_found": 0,
    }

    with conn.transaction():

        if keys:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT unique_key, raw_payload
                    FROM raw_service_requests
                    WHERE unique_key = ANY(%s)
                    ORDER BY unique_key
                    FOR UPDATE
                    """,
                    (keys,),
                )
                current_rows = cur.fetchall()

            current_stored = dict(current_rows)

            if set(current_stored) != set(keys):
                raise ValueError(
                    "Stored record disappeared during reconciliation"
                )
        else:
            current_stored = {}

        # This comparison uses locked, current DB versions.
        classifications = compare_known_records(
            current_stored, source_rows
        )

        source_by_id = {
            record["unique_key"]: record
            for record in source_rows
        }

        eligible = []

        for key, item in classifications.items():
            status = item["status"]

            if status == "TIMESTAMP_ONLY":
                counts["timestamp_only"] += 1
                eligible.append(source_by_id[key])

            elif status == "BUSINESS_CHANGED":
                counts["business_changed"] += 1
                eligible.append(source_by_id[key])

            elif status == "UNCHANGED":
                counts["unchanged"] += 1

            elif status == "STALE_SOURCE":
                counts["stale_source"] += 1

            elif status == "NOT_FOUND":
                counts["not_found"] += 1

            else:
                raise ValueError(
                    "Unknown reconciliation status"
                )

        if eligible:
            inserted, updated, unchanged = ingest_records(
                conn, eligible
            )
        else:
            inserted = updated = unchanged = 0

        if inserted:
            raise RuntimeError(
                "Reconciliation unexpectedly inserted a record"
            )

    return {
        **counts,
        "updated": updated,
        "upsert_unchanged": unchanged,
        "deleted": 0,
    }