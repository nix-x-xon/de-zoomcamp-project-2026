# Architecture

## Data Flow

```
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│  PSE API      │  │  ENTSO-E API  │  │ Yahoo Finance │
│  (demand,     │  │  (prices,     │  │  (FX, TTF,    │
│   gen mix)    │  │   generation) │  │   coal, EUA)  │
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        └──────────┬───────┴──────────┬───────┘
                   │ Python ingestion │
                   │ (Docker tasks)   │
                   ▼                  ▼
            ┌────────────────────────────┐
            │  GCS bucket (raw parquet)  │
            │  /source/dataset/dt=YYYY…  │
            └──────────────┬─────────────┘
                           │  (Kestra → BQ Load)
                           ▼
            ┌────────────────────────────┐
            │ BQ: energy_raw.*           │
            │  - partitioned by day      │
            │  - clustered by ticker/ctr │
            └──────────────┬─────────────┘
                           │  (dbt run)
                           ▼
            ┌────────────────────────────┐
            │ BQ: energy_marts.*         │
            │  - stg_* views             │
            │  - fct_* tables            │
            └──────────────┬─────────────┘
                           ▼
                  Looker Studio Dashboard
```

## Partitioning & Clustering

| Table                              | Partition (grain / field)     | Cluster by            |
|------------------------------------|-------------------------------|-----------------------|
| `raw.pse_demand`                   | DAY (load-time pseudo column) | —                     |
| `raw.entsoe_prices_pl`             | DAY (load-time pseudo column) | —                     |
| `raw.yahoo_prices`                 | DAY (load-time pseudo column) | —                     |
| `raw.pse_demand_stream`            | DAY / `event_ts`              | `interval_id`         |
| `raw.entsoe_eu_prices`             | DAY / `date`                  | `zone`                |
| `raw.entsoe_eu_generation`         | DAY / `date`                  | `zone`, `fuel_type`   |
| `raw.entsoe_eu_hourly_wide`        | MONTH / `date` (Spark-managed)| `zone`                |
| `marts.fct_daily_prices`           | MONTH / `price_date`          | `country`             |
| `marts.fct_commodity_drivers`      | MONTH / `price_date`          | `ticker`              |
| `marts.fct_intraday_demand`        | MONTH / `demand_date`         | `interval_id`         |
| `marts.fct_eu_daily_by_region`     | MONTH / `date`                | `region`, `zone_code` |
| `marts.fct_eu_duck_curve`          | MONTH / `date`                | `region`, `zone_code` |
| `marts.fct_eu_regional_summary`    | MONTH / `date`                | `region`              |

**Why:** All analytical queries filter by date range first (trend analysis, backtests, duck-curve
comparisons), then narrow to specific zones, countries, tickers, or regions. Raw hourly-cadence
tables keep DAY partitioning to match the upstream ingestion grain. The marts switch to MONTH
partitioning because the dashboard queries span multi-week windows and MONTH partitions cut the
partition-metadata overhead on tables with low per-day row counts. Clustering on
`region` / `zone` / `country` / `ticker` speeds up the filter/group-by patterns in each mart
without over-slicing.

## Choice Notes

- **Kestra over Airflow**: simpler UI, YAML flows, aligned with course.
- **dbt + BigQuery**: industry-standard ELT pattern; partitioning handled in config.
- **Parquet landing**: columnar, compressed, cheap BQ loads.
- **Daily schedule**: matches upstream data cadence (PSE + ENTSO-E update daily).

## Streaming Layer

For intraday demand (15-minute PSE updates) the pipeline uses Kafka + Schema Registry:

```
PSE API → producer.py → Kafka topic `pse.demand.v1` (Avro) → consumer.py → BQ streaming insert
```

- **Topic:** `pse.demand.v1` (1 partition, keyed by `{date}-{interval_id}`)
- **Schema:** `streaming/schemas/pse_demand.avsc` (registered in Confluent Schema Registry)
- **Dedup:** handled in `stg_pse__demand_stream` via `row_number()` on late-arriving updates
- **Mart:** `fct_intraday_demand` is incremental — only new `event_ts` rows are appended on each dbt run

Why Kafka for this: PSE updates demand every 15 minutes but producers can republish corrections
throughout the day. A log-based queue with Avro schema enforcement handles both the real-time
flow and schema evolution cleanly.

## Spark Batch Layer (EU-wide ENTSO-E)

A PySpark application (local mode in Docker) handles the multi-country hourly ENTSO-E
workload that would be overkill for pandas and verbose in pure BigQuery SQL.

```
ENTSO-E API (14 zones)
    │  Python + entsoe-py (Spark driver)
    │  Rate-limited to 350 req/min
    ▼
GCS  entsoe/eu_prices/country={zone}/dt={YYYY-MM-DD}/data.parquet
     entsoe/eu_generation/country={zone}/dt={YYYY-MM-DD}/data.parquet
    │  Spark load job (BQ connector, write.format("bigquery"))
    ▼
BQ energy_raw.entsoe_eu_prices       (partition: date; cluster: zone)
BQ energy_raw.entsoe_eu_generation   (partition: date; cluster: zone, fuel_type)
    │  Spark transform:
    │   - pivot generation (long → wide, one col per fuel)
    │   - join prices on (zone, timestamp)
    │   - compute renewable_share, fossil_share, residual_load
    │   - 168-hour rolling price mean
    ▼
BQ energy_raw.entsoe_eu_hourly_wide  (partition: month; cluster: zone)
    │  dbt:  seed → staging → intermediate → marts
    ▼
fct_eu_daily_by_region | fct_eu_duck_curve | fct_eu_regional_summary
```

### Why Spark, not BQ SQL

- Schema unification across ~14 zones (different resolutions, DST edge cases)
- Long→wide pivot of generation by ~15 fuel types
- 168-hour rolling price windows per zone
- Self-join for price spreads vs reference zone

These are shuffle-heavy operations where Spark's DataFrame API is both concise and faster
than equivalent BigQuery nested SQL, while also being cheaper for full-table transforms.

### Regional dimension

The `dbt/seeds/dim_region_mapping.csv` file maps zone codes → `region` (Eastern / Southern)
plus country, timezone, and primary_fuel metadata. The intermediate model
`int_eu_hourly_with_region` left-joins this seed; all downstream marts carry `region`
as a column for dashboard filtering and group-by comparisons.
