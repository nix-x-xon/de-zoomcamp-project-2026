{{ config(materialized='view') }}

select
    date,
    timestamp as price_timestamp,
    zone as zone_code,
    cast(price_eur_mwh as numeric) as price_eur_mwh
from {{ source('energy_raw', 'entsoe_eu_prices') }}
where price_eur_mwh is not null
