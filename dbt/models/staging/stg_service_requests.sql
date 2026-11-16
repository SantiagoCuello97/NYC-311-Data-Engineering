-- One row per NYC 311 service request.
-- Preserve the original identifiers and timestamps.

with source_data as (

    select *
    from {{ source('nyc311', 'raw_service_requests') }}

),

renamed as (

    select
        unique_key as request_id,
        source_updated_at,
        created_date as created_at,
        closed_date as closed_at,

        nullif(trim(status), '') as status,
        nullif(trim(agency), '') as agency,
        nullif(trim(complaint_type), '') as complaint_type,

        ingested_at,
        last_seen_at

    from source_data

)

select *
from renamed