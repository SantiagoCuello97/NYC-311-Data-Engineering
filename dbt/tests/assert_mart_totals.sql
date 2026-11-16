-- Return one failing row if aggregate metrics do not reconcile.
with fact_totals as (
    select
        count(*) as total_requests,
        count(*) filter (where is_closed) as closed_requests,
        count(*) filter (where not is_closed) as open_requests,
        count(*) filter (where has_invalid_dates) as invalid_dates,
        count(resolution_hours) as valid_durations
    from {{ ref('fct_service_requests') }}
),
mart_totals as (
    select
        coalesce(sum(total_requests), 0) as total_requests,
        coalesce(sum(requests_with_close_date), 0)
            as closed_requests,
        coalesce(sum(requests_without_close_date), 0)
            as open_requests,
        coalesce(sum(invalid_date_requests), 0)
            as invalid_dates,
        coalesce(sum(valid_resolution_requests), 0)
            as valid_durations
    from {{ ref('mart_complaint_summary') }}
)
select *
from fact_totals f
cross join mart_totals m
where
    f.total_requests <> m.total_requests
    or f.closed_requests <> m.closed_requests
    or f.open_requests <> m.open_requests
    or f.invalid_dates <> m.invalid_dates
    or f.valid_durations <> m.valid_durations