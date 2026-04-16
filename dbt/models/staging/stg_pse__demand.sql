{{ config(materialized='view') }}

select
    date(doba) as demand_date,
    cast(udtczas as timestamp) as demand_timestamp,
    cast(zap_kse as numeric) as demand_mw,
    ingested_at
from {{ source('energy_raw', 'pse_demand') }}
where zap_kse is not null
