{{ config(materialized='view') }}

select
    date as price_date,
    ticker,
    cast(open as numeric) as open_price,
    cast(high as numeric) as high_price,
    cast(low as numeric) as low_price,
    cast(close as numeric) as close_price,
    cast(volume as int64) as volume
from {{ source('energy_raw', 'yahoo_prices') }}
where close is not null
