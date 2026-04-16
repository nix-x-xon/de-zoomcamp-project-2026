{{
    config(
        materialized='table',
        partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['region', 'zone_code']
    )
}}

-- Daily aggregations per zone, with region tag for filtered dashboards.
-- Keeps zone-level detail so drill-down works; dashboard GROUP BYs on region.
with base as (
    select
        date,
        region,
        country,
        zone_code,
        zone_name,
        primary_fuel,
        price_eur_mwh,
        total_mw,
        renewable_share,
        fossil_share,
        residual_load_mw
    from {{ ref('int_eu_hourly_with_region') }}
)

select
    date,
    region,
    country,
    zone_code,
    zone_name,
    primary_fuel,
    avg(price_eur_mwh) as avg_price_eur_mwh,
    min(price_eur_mwh) as min_price_eur_mwh,
    max(price_eur_mwh) as max_price_eur_mwh,
    stddev_samp(price_eur_mwh) as price_std,
    countif(price_eur_mwh < 0) as negative_price_hours,
    avg(renewable_share) as avg_renewable_share,
    avg(fossil_share) as avg_fossil_share,
    sum(total_mw) as total_generation_mwh,
    avg(residual_load_mw) as avg_residual_load_mw
from base
group by 1,2,3,4,5,6
