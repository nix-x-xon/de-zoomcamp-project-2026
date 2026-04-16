{{
    config(
        materialized='table',
        partition_by={'field': 'price_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['ticker']
    )
}}

-- One row per (date, ticker) with 5-day and 20-day returns for downstream analytics
with base as (
    select
        price_date,
        ticker,
        close_price
    from {{ ref('stg_yahoo__prices') }}
),

with_returns as (
    select
        price_date,
        ticker,
        close_price,
        (close_price / lag(close_price, 5) over (partition by ticker order by price_date)) - 1
            as return_5d,
        (close_price / lag(close_price, 20) over (partition by ticker order by price_date)) - 1
            as return_20d
    from base
)

select * from with_returns
