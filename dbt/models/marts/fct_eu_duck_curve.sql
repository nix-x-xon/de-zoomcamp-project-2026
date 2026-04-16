{{
    config(
        materialized='table',
        partition_by={'field': 'date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['region', 'zone_code']
    )
}}

-- Hourly price profile by (date, zone) to expose the duck curve.
-- Southern countries show a midday collapse; Eastern countries stay flat.
select
    date,
    extract(hour from ts_utc) as hour_utc,
    region,
    country,
    zone_code,
    avg(price_eur_mwh) as avg_price_eur_mwh,
    avg(renewable_share) as avg_renewable_share
from {{ ref('int_eu_hourly_with_region') }}
where price_eur_mwh is not null
group by 1,2,3,4,5
