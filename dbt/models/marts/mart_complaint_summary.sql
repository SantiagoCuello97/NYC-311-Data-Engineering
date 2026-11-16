-- Grain: created month + agency + complaint type.
-- Missing dimension values are retained as NULL.
-- Closure means a recorded closed_at timestamp.

with requests as (

    select *
    from {{ ref('fct_service_requests') }}

),

aggregated as (

    select
        date_trunc('month', created_at)::date as created_month,
        agency,
        complaint_type,

        count(*)::bigint as total_requests,

        count(*) filter (
            where is_closed
        )::bigint as requests_with_close_date,

        count(*) filter (
            where not is_closed
        )::bigint as requests_without_close_date,

        count(*) filter (
            where has_invalid_dates
        )::bigint as invalid_date_requests,

        count(resolution_hours)::bigint
            as valid_resolution_requests,

        round(
            avg(resolution_hours)::numeric, 2
        ) as avg_resolution_hours

    from requests

    group by
        date_trunc('month', created_at)::date,
        agency,
        complaint_type

)

select *
from aggregated