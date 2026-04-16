{{
    config(
        materialized='table',
        partition_by={'field': 'price_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['country']
    )
}}

with daily as (
    select
        price_date,
        country,
        avg(price_eur_mwh) as avg_price_eur_mwh,
        min(price_eur_mwh) as min_price_eur_mwh,
        max(price_eur_mwh) as max_price_eur_mwh,
        count(*) as hours_observed
    from {{ ref('stg_entsoe__prices') }}
    group by price_date, country
)

select * from daily
