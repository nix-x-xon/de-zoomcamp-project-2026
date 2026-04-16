{{
    config(
        materialized='table',
        partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['region']
    )
}}

-- Region-level rollup for the headline dashboard tiles
-- (Eastern vs Southern comparison in one row per region per day).
with zone_daily as (
    select * from {{ ref('fct_eu_daily_by_region') }}
)

select
    date,
    region,
    count(distinct zone_code) as zones_reporting,
    avg(avg_price_eur_mwh) as regional_avg_price_eur_mwh,
    avg(price_std) as regional_avg_price_std,
    sum(negative_price_hours) as regional_negative_price_hours,
    avg(avg_renewable_share) as regional_renewable_share,
    avg(avg_fossil_share) as regional_fossil_share,
    sum(total_generation_mwh) as regional_generation_mwh
from zone_daily
group by 1,2
