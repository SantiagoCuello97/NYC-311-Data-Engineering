-- dbt singular tests pass when this query returns zero rows.

select request_id
from {{ ref('fct_service_requests') }}
where
    resolution_hours < 0

    or is_closed is distinct from (closed_at is not null)

    or has_invalid_dates is distinct from (
        created_at is not null
        and closed_at is not null
        and closed_at < created_at
    )

    or (
        resolution_hours is not null
        and (
            created_at is null
            or closed_at is null
            or closed_at < created_at
        )
    )

    or (
        resolution_hours is null
        and created_at is not null
        and closed_at is not null
        and closed_at >= created_at
    )