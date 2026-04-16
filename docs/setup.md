# Setup Guide

Full reproducibility instructions for the Polish Energy Market Data Platform.

## Prerequisites

- Google Cloud account with billing enabled
- `gcloud`, `terraform`, `docker`, `python3.12` installed locally
- ENTSO-E API key (free registration at https://transparency.entsoe.eu/)

## 1. GCP Project & Service Account

```bash
export PROJECT_ID=your-gcp-project-id
gcloud projects create $PROJECT_ID
gcloud config set project $PROJECT_ID
gcloud services enable storage.googleapis.com bigquery.googleapis.com

# Service account for Terraform + dbt + ingestion
gcloud iam service-accounts create de-zoomcamp \
    --display-name="DE Zoomcamp pipeline"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:de-zoomcamp@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:de-zoomcamp@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud iam service-accounts keys create gcp-key.json \
    --iam-account=de-zoomcamp@$PROJECT_ID.iam.gserviceaccount.com
```

## 2. Configure local env

```bash
cp .env.example .env
# edit .env with your values
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# edit tfvars
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# edit profiles.yml with your project & keyfile path
```

## 3. Provision infra

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

Outputs will show the created bucket and datasets.

## 4. Build ingestion image

```bash
cd ingestion
docker build -t de-zoomcamp-ingestion:latest .
```

## 5. Start orchestration

```bash
docker compose up -d
# Open http://localhost:8080 and import flows from orchestration/kestra/flows/
```

## 6. Backfill historical data

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
    -v $(pwd)/gcp-key.json:/tmp/gcp-key.json:ro \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json \
    -e GCS_BUCKET=$GCS_BUCKET \
    -e ENTSOE_API_KEY=$ENTSOE_API_KEY \
    de-zoomcamp-ingestion:latest \
    src.fetch_entsoe --country PL --start 2023-01-01 --end 2026-04-15
```

## 7. Start the streaming layer (optional)

```bash
# Build image
cd streaming && docker build -t de-zoomcamp-streaming:latest .

# Kafka + Schema Registry already run from `docker compose up -d`
# Launch producer (polls PSE every 60s, publishes to pse.demand.v1)
docker run --rm --network=de-zoomcamp-project_default \
    -e KAFKA_BOOTSTRAP=kafka:29092 \
    -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    de-zoomcamp-streaming:latest src.producer

# Launch consumer (sinks to BigQuery)
docker run --rm --network=de-zoomcamp-project_default \
    -v $(pwd)/../gcp-key.json:/tmp/gcp-key.json \
    -e KAFKA_BOOTSTRAP=kafka:29092 \
    -e SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    -e GCP_PROJECT=$PROJECT_ID \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json \
    de-zoomcamp-streaming:latest src.consumer
```

- Kafka UI: http://localhost:8082
- Schema Registry: http://localhost:8081

## 8. Run dbt

```bash
cd dbt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## 9. EU ENTSO-E via Spark (batch)

Build the Spark image and run the backfill + daily + transform chain.

```bash
cd spark
docker build -t de-zoomcamp-spark:latest .
```

**One-time historical backfill** (choose your year range; e.g. 2022-2025 for a focused dataset):

```bash
docker run --rm \
    -v $(pwd)/../gcp-key.json:/tmp/gcp-key.json \
    -e GCS_BUCKET=$GCS_BUCKET \
    -e SPARK_TEMP_BUCKET=$SPARK_TEMP_BUCKET \
    -e ENTSOE_API_KEY=$ENTSOE_API_KEY \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json \
    de-zoomcamp-spark:latest \
    jobs.backfill_entsoe --start 2022 --end 2025
```

**Load raw parquet into BQ**:

```bash
docker run --rm \
    -v $(pwd)/../gcp-key.json:/tmp/gcp-key.json \
    -e GCS_BUCKET=$GCS_BUCKET \
    -e SPARK_TEMP_BUCKET=$SPARK_TEMP_BUCKET \
    -e GCP_PROJECT=$GCP_PROJECT \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json \
    de-zoomcamp-spark:latest jobs.load_raw_to_bq
```

**Build the wide table**:

```bash
docker run --rm \
    -v $(pwd)/../gcp-key.json:/tmp/gcp-key.json \
    -e GCS_BUCKET=$GCS_BUCKET \
    -e SPARK_TEMP_BUCKET=$SPARK_TEMP_BUCKET \
    -e GCP_PROJECT=$GCP_PROJECT \
    -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-key.json \
    de-zoomcamp-spark:latest jobs.transform_eu_wide --mode=full
```

**Seed + run dbt** to build the regional marts:

```bash
cd ../dbt
dbt deps
dbt seed
dbt run
dbt test
```

The daily path is wired up in Kestra (`spark_daily` flow, 07:00 UTC cron).

## 10. Connect Looker Studio

1. Go to https://lookerstudio.google.com/
2. Create a BigQuery data source → select `energy_marts.fct_daily_prices`
3. Build at least 2 charts (e.g., price trend line + commodity correlation heatmap)
4. Copy public link to main README
