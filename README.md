# Polish Energy Market Data Platform

End-to-end data engineering pipeline ingesting Polish & European energy market data into a cloud data warehouse with dbt transformations and a BI dashboard.

Built for the **DataTalks.Club Data Engineering Zoomcamp 2026** final project.

## Problem Statement

Energy traders and analysts need unified access to:
- Day-ahead electricity prices across European bidding zones
- Generation mix (wind, solar, gas, coal, nuclear, hydro) by zone
- Commodity drivers (TTF gas, coal, EUA carbon) and FX rates
- Real-time Polish grid demand

This project consolidates fragmented sources (PSE, ENTSO-E, Yahoo Finance) into a queryable warehouse and tells a **comparative story: Eastern vs Southern Europe**.

**Eastern group** (4 countries): Poland, Czech Republic, Hungary, Romania — coal/nuclear legacy, winter peaking, weaker interconnection, recent events: Baltic synchronization (Feb 2025), end of Russian gas transit (Jan 2025).

**Southern group** (4 countries, 10 zones): Spain, Portugal, Italy (7 bidding zones), Greece — high solar penetration, duck curve pricing, negative-price frequency, Apr 2025 Iberian blackout case study.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Sources: PSE | ENTSO-E (14 EU zones) | Yahoo Finance                │
└──────┬────────────────┬─────────────────────────────┬────────────────┘
       │ (batch daily)  │ (Spark backfill + daily)    │ (streaming 15m)
       ▼                ▼                             ▼
  ┌──────────┐   ┌──────────────────────┐   ┌─────────────────────┐
  │ GCS raw  │   │ GCS entsoe/eu_*/...  │   │ Kafka pse.demand.v1 │
  │ parquet  │   │ (country=XX/dt=…)    │   │ Avro + SR           │
  └────┬─────┘   └──────────┬───────────┘   └──────────┬──────────┘
       │ (load)             │ (Spark transform)        │ (consumer)
       └─────────────────┬──┴──────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────────────┐
│ BigQuery energy_raw — partitioned by date, clustered                 │
│  entsoe_eu_prices | entsoe_eu_generation | entsoe_eu_hourly_wide     │
│  pse_demand | entsoe_prices_pl | yahoo_prices | pse_demand_stream    │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ (dbt: seeds → staging → intermediate → marts)
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│ energy_marts: fct_eu_daily_by_region | fct_eu_duck_curve             │
│               fct_eu_regional_summary | fct_daily_prices | ...       │
└──────────────────────────────┬───────────────────────────────────────┘
                               ▼
             Looker Studio dashboard — Eastern vs Southern comparison
```

## Tech Stack

| Layer            | Tool                |
|------------------|---------------------|
| Cloud            | GCP                 |
| IaC              | Terraform           |
| Containerization | Docker              |
| Orchestration    | Kestra              |
| Data Lake        | Google Cloud Storage|
| Streaming        | Kafka + Schema Registry (Avro) |
| Batch processing | Apache Spark (PySpark, local mode in Docker) |
| Warehouse        | BigQuery            |
| Transformations  | dbt + Spark         |
| Dashboard        | Looker Studio       |
| Language         | Python 3.12         |

## Repository Structure

```
.
├── terraform/            # GCP infrastructure as code
├── ingestion/            # Batch: Python scripts to fetch source data → GCS
├── streaming/            # Kafka producer/consumer (Avro) for real-time PSE demand
├── spark/                # PySpark jobs: EU ENTSO-E backfill + daily + wide-table transform
├── orchestration/kestra/ # Kestra flow definitions (YAML)
├── dbt/                  # dbt: seeds + staging + intermediate + marts (regional)
├── dashboard/            # Dashboard export / screenshots
├── data/                 # Local seeds & samples (gitignored)
└── docs/                 # Design notes, ERD, data dictionary
```

## Quick Start

See [docs/setup.md](docs/setup.md) for the full reproducibility guide.

```bash
# 1. Provision infra
cd terraform && terraform init && terraform apply

# 2. Start orchestration
docker compose up -d

# 3. Trigger ingestion flow in Kestra UI (http://localhost:8080)

# 4. Run dbt transforms
cd dbt && dbt run && dbt test
```

## Dashboard

See [`dashboard/README.md`](dashboard/README.md) for the Looker Studio setup, BigQuery source tables, and page-by-page chart list. Shareable link is pasted there after publishing.

## Reproducibility

Full step-by-step guide: [`docs/setup.md`](docs/setup.md). Expect ~30 min end-to-end on a machine with Docker Desktop, gcloud SDK, and Terraform already installed. An open GCP billing account is required.

## Status

End-to-end pipeline live in production: GCP infra provisioned, all Docker images built, 3 years of PSE + ENTSO-E PL data ingested, 2 years (2024-2025) of prices across 14 EU bidding zones, all 13 dbt models build + 25 data tests pass. Streaming stack runs via `docker compose up -d`.

Known limitation: ENTSO-E generation for non-PL zones in 2024-2025 returns payloads that `entsoe-py 0.7.11` cannot parse; the EU-wide fuel-mix columns (renewable share, residual load) are currently NULL. Polish generation is populated in full.
