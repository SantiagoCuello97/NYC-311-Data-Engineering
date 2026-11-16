-- Each combination of dimensions must appear exactly once.
with duplicate_groups as (
    select
        created_month,
        agency,
        complaint_type
    from {{ ref('mart_complaint_summary') }}
    group by created_month, agency, complaint_type
    having count(*) > 1
),
invalid_metrics as (
    select
        created_month,
        agency,
        complaint_type
    from {{ ref('mart_complaint_summary') }}
    where total_requests <= 0
       or requests_with_close_date < 0
       or requests_without_close_date < 0
       or invalid_date_requests < 0
       or valid_resolution_requests < 0
       or requests_with_close_date
            + requests_without_close_date <> total_requests
       or invalid_date_requests > requests_with_close_date
       or valid_resolution_requests + invalid_date_requests
            <> requests_with_close_date
       or (
           valid_resolution_requests = 0
           and avg_resolution_hours is not null
       )
       or (
           valid_resolution_requests > 0
           and avg_resolution_hours is null
       )
       or avg_resolution_hours < 0
)
select * from duplicate_groups
union all
select * from invalid_metrics