{{
    config(
        materialized='incremental',
        unique_key=['demand_date', 'interval_id'],
        partition_by={'field': 'demand_date', 'data_type': 'date', 'granularity': 'month'},
        cluster_by=['interval_id']
    )
}}

-- Intraday 15-minute demand mart. Incremental: only append rows newer than max(event_ts).
select
    demand_date,
    interval_id,
    event_ts,
    demand_mw,
    -- 4-interval rolling average (1-hour smoothed)
    avg(demand_mw) over (
        partition by demand_date
        order by interval_id
        rows between 3 preceding and current row
    ) as demand_mw_1h_avg
from {{ ref('stg_pse__demand_stream') }}

{% if is_incremental() %}
where event_ts > (select coalesce(max(event_ts), timestamp('1970-01-01')) from {{ this }})
{% endif %}
