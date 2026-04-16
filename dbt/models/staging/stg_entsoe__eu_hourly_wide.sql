{{ config(materialized='view') }}

-- Prices-only wide view. entsoe_eu_generation is not yet populated (the
-- ENTSO-E generation endpoint returns payloads entsoe-py 0.7.11 cannot parse
-- for post-2024 data), so renewable/fossil/residual-load columns are NULL.
-- Structure is kept identical to the Spark-produced wide table so downstream
-- marts can run unchanged; adding generation later only requires swapping the
-- source here.
with prices as (
    select
        date,
        timestamp as ts_utc,
        zone as zone_code,
        price_eur_mwh
    from {{ source('energy_raw', 'entsoe_eu_prices') }}
),

with_rolling as (
    select
        *,
        avg(price_eur_mwh) over (
            partition by zone_code
            order by ts_utc
            rows between 167 preceding and current row
        ) as price_7d_mean
    from prices
)

select
    date,
    ts_utc,
    zone_code,
    price_eur_mwh,
    price_7d_mean,
    cast(null as float64) as total_mw,
    cast(null as float64) as renewable_mw,
    cast(null as float64) as fossil_mw,
    cast(null as float64) as renewable_share,
    cast(null as float64) as fossil_share,
    cast(null as float64) as residual_load_mw
from with_rolling
