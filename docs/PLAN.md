# Project Plan & State

Living document. Updated each session so Claude can resume without losing context.

## Config (already decided)

| Key                 | Value                                  |
|---------------------|----------------------------------------|
| GCP project ID      | `de-zoomcamp-energy-mf01`              |
| Region              | `europe-central2` (Warsaw)             |
| Raw GCS bucket      | `de-zoomcamp-energy-raw-mf01`          |
| Spark temp bucket   | `de-zoomcamp-energy-raw-mf01-spark-temp` |
| BQ dataset (raw)    | `energy_raw`                           |
| BQ dataset (marts)  | `energy_marts`                         |
| Service account     | `de-zoomcamp@<project>.iam.gserviceaccount.com` (planned, not created) |
| Key file            | `./gcp-key.json` (planned, not created) |

Source of truth: `.env` and `terraform/terraform.tfvars`.

## Status checklist

### Done
- [x] Repo scaffolding (all subdirs, Dockerfiles, compose, README, architecture docs)
- [x] Terraform v1.14.8 installed
- [x] gcloud SDK 565.0.0 installed
- [x] `.env` populated with project/region/bucket/dataset names
- [x] `terraform/terraform.tfvars` populated
- [x] **Deep exploration** confirmed: ingestion, streaming, spark, dbt, kestra flows are ALL real implementations (not stubs). 534 LOC Spark, 384 LOC dbt across 14 models + seed + tests.
- [x] GitHub repo created: https://github.com/nix-x-xon/de-zoomcamp-project-2026 (public), `origin` wired
- [x] `CLAUDE.md` added with resume protocol, undercover mode, squash-merge policy
- [x] `terraform/main.tf` patched: added 4 raw BQ tables (`pse_demand`, `entsoe_prices_pl`, `yahoo_prices`, `pse_demand_stream`)
- [x] `orchestration/kestra/flows/daily_ingestion.yml` patched: added GCP key volume mount to fetch_pse, fetch_entsoe_pl, fetch_yahoo
- [x] GCP project, SA, APIs, service account key all created (Phase 1)
- [x] `terraform apply` complete — 6 raw BQ tables + 2 GCS buckets + 2 datasets live
- [x] Initial commit pushed to https://github.com/nix-x-xon/de-zoomcamp-project-2026
- [x] Branch renamed master → main
- [x] Fixed PSE API endpoint: retired `zap-kse` → `his-wlk-cal` in `ingestion/src/fetch_pse.py` and `streaming/src/producer.py`
- [x] Upgraded `entsoe-py 0.6.7 → 0.7.11` (old version couldn't reach 2025-11+ data) and `yfinance 0.2.40 → 1.3.0`
- [x] Replaced delisted Yahoo ticker `MTF=F` with `NG=F` (Henry Hub gas)
- [x] Fixed ENTSO-E generation melt for 0.7.x MultiIndex columns
- [x] Fixed Spark Dockerfile: base image `apache/spark-py:v3.5.0 → apache/spark:3.5.8-python3`, entrypoint `python → python3`, GCS connector URL moved to Maven Central
- [x] dbt `deps` + `seed` — `dim_region_mapping` (16 rows) loaded to `energy_marts_seeds`
- [x] Yahoo backfill complete — 4,173 rows loaded to `energy_raw.yahoo_prices`
- [x] PSE v2 backfill (clean schema) — 64,120 rows in `energy_raw.pse_demand`
- [x] ENTSO-E PL prices backfill — 42,741 rows in `energy_raw.entsoe_prices_pl`
- [x] ENTSO-E PL generation backfill — 2,079,162 rows in `energy_raw.entsoe_generation_pl`
- [x] Patched `spark/jobs/load_raw_to_bq.py` to skip missing GCS paths gracefully
- [x] Partial EU prices loaded — 48,352 rows (PL + CZ so far; more coming)
- [x] dbt: 8 non-EU-wide models built; 13/14 tests PASS (commodity ticker accepted_values fixed)

### In flight
- [ ] Spark EU backfill 2024-2025 across 14 zones — prices succeeding, generation failing per-zone with ValueError (entsoe-py 0.7.11 parse issue for some datasets). Will retry; currently on CZ.

### Pending — GCP bootstrap
- [ ] `gcloud auth login` (**user action — interactive**, run as `! gcloud auth login`)
- [ ] Create GCP project: `gcloud projects create de-zoomcamp-energy-mf01`
- [ ] Link billing account (manual in Cloud Console if not set)
- [ ] `gcloud config set project de-zoomcamp-energy-mf01`
- [ ] Enable APIs: `gcloud services enable storage.googleapis.com bigquery.googleapis.com`
- [ ] Create service account `de-zoomcamp` + grant `roles/storage.admin` and `roles/bigquery.admin`
- [ ] Download key to `./gcp-key.json` (gitignored)
- [ ] Set `GOOGLE_APPLICATION_CREDENTIALS=./gcp-key.json` (already in `.env`)

### Pending — infra
- [ ] `cd terraform && terraform init`
- [ ] `terraform plan`
- [ ] `terraform apply` (creates 2 GCS buckets + 2 BQ datasets + 3 BQ tables)

### Pending — data sources
- [ ] Register ENTSO-E API key at https://transparency.entsoe.eu/ → put in `.env` `ENTSOE_API_KEY=`
- [ ] Build ingestion image
- [ ] Backfill PSE + ENTSO-E + Yahoo data

### Pending — transforms & dashboard
- [ ] dbt profiles.yml (copy from example, fill project + keyfile)
- [ ] dbt seed / run / test
- [ ] Spark backfill (ENTSO-E EU wide table, 2022-2025)
- [ ] Kestra flow imports
- [ ] Kafka producer/consumer for `pse.demand.v1` stream
- [ ] Looker Studio dashboard — Eastern vs Southern comparison

### Pending — repo hygiene
- [x] GitHub repo created: https://github.com/nix-x-xon/de-zoomcamp-project-2026 (public) — `origin` wired
- [ ] Initial git commit (nothing committed yet — `master` is empty)
- [ ] `git push -u origin master` (or rename to `main` first)

## Next action when resuming

The immediate blocker is **gcloud auth**. In the next session:

1. User runs `! gcloud auth login` (interactive browser flow — only user can do it).
2. Claude continues from the "GCP bootstrap" section above, running the non-interactive steps.

## Notes

- `gcp-key.json` MUST stay out of git (check `.gitignore`).
- `ENTSOE_API_KEY` in `.env` is currently empty.
- `master` branch has zero commits — make the initial commit after gcloud bootstrap succeeds so the first commit reflects a working baseline.
