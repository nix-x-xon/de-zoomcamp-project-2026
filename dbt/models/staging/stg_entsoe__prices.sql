{{ config(materialized='view') }}

with src as (
    select
        timestamp as price_timestamp,
        date(timestamp) as price_date,
        country,
        price_eur_mwh,
        ingested_at
    from {{ source('energy_raw', 'entsoe_prices_pl') }}
)

select
    price_date,
    price_timestamp,
    country,
    cast(price_eur_mwh as numeric) as price_eur_mwh,
    ingested_at
from src
where price_eur_mwh is not null
