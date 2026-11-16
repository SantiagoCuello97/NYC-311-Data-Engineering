-- Grain: one row per NYC 311 service request.
-- Invalid date pairs are flagged, not silently corrected.

with staging as (

    select *
    from {{ ref('stg_service_requests') }}

),

features as (

    select
        request_id,
        source_updated_at,
        created_at,
        closed_at,
        status,
        agency,
        complaint_type,
        ingested_at,

        (closed_at is not null) as is_closed,

        (
            created_at is not null
            and closed_at is not null
            and closed_at < created_at
        ) as has_invalid_dates

    from staging

)

select
    request_id,
    source_updated_at,
    created_at,
    closed_at,
    status,
    agency,
    complaint_type,
    ingested_at,
    is_closed,
    has_invalid_dates,

    case
        when created_at is not null
         and closed_at is not null
         and closed_at >= created_at
        then round(
            (
                extract(epoch from (closed_at - created_at))
                / 3600.0
            )::numeric,
            2
        )
        else null
    end as resolution_hours

from features