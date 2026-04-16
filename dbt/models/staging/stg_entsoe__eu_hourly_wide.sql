{{ config(materialized='view') }}

-- Thin staging view over the Spark-produced wide table.
-- The Spark job already cleaned and enriched; we only cast types and rename.
select
    date,
    timestamp as ts_utc,
    zone as zone_code,
    price_eur_mwh,
    price_7d_mean,
    total_mw,
    renewable_mw,
    fossil_mw,
    renewable_share,
    fossil_share,
    residual_load_mw
from {{ source('energy_raw', 'entsoe_eu_hourly_wide') }}
