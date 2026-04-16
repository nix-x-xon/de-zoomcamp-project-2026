{{ config(materialized='view') }}

-- Enriches the wide hourly table with region / country / primary_fuel via the seed.
-- Downstream marts consume this instead of calling the seed directly.
select
    w.date,
    w.ts_utc,
    w.zone_code,
    r.country,
    r.region,
    r.primary_fuel,
    r.zone_name,
    r.timezone,
    w.price_eur_mwh,
    w.price_7d_mean,
    w.total_mw,
    w.renewable_mw,
    w.fossil_mw,
    w.renewable_share,
    w.fossil_share,
    w.residual_load_mw
from {{ ref('stg_entsoe__eu_hourly_wide') }} w
left join {{ ref('dim_region_mapping') }} r
    on w.zone_code = r.zone_code
