{{ config(materialized='view') }}

-- Real-time demand stream from Kafka → BQ sink.
-- Deduplicates on (demand_date, interval_id) keeping the latest ingest.
with ranked as (
    select
        event_ts,
        demand_date,
        interval_id,
        demand_mw,
        ingested_at,
        row_number() over (
            partition by demand_date, interval_id
            order by ingested_at desc
        ) as rn
    from {{ source('energy_raw', 'pse_demand_stream') }}
)

select
    event_ts,
    demand_date,
    interval_id,
    demand_mw,
    ingested_at
from ranked
where rn = 1
