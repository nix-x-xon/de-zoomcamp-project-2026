terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Raw data lake bucket
resource "google_storage_bucket" "raw_data" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }
}

# Raw dataset (landing zone after GCS load)
resource "google_bigquery_dataset" "raw" {
  dataset_id    = var.bq_dataset_raw
  friendly_name = "Raw energy market data"
  location      = var.region
  description   = "Landing zone for PSE, ENTSO-E, Yahoo Finance extracts"
}

# Marts dataset (dbt-managed)
resource "google_bigquery_dataset" "marts" {
  dataset_id    = var.bq_dataset_marts
  friendly_name = "Energy analytics marts"
  location      = var.region
  description   = "dbt-managed analytics models (staging + marts)"
}

# --- EU-wide ENTSO-E tables produced by Spark ---

resource "google_bigquery_table" "entsoe_eu_prices" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "entsoe_eu_prices"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["zone"]

  schema = jsonencode([
    { name = "date",         type = "DATE",      mode = "REQUIRED" },
    { name = "timestamp",    type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "zone",         type = "STRING",    mode = "REQUIRED" },
    { name = "price_eur_mwh", type = "FLOAT64",  mode = "NULLABLE" }
  ])
}

resource "google_bigquery_table" "entsoe_eu_generation" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "entsoe_eu_generation"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }
  clustering = ["zone", "fuel_type"]

  schema = jsonencode([
    { name = "date",           type = "DATE",      mode = "REQUIRED" },
    { name = "timestamp",      type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "zone",           type = "STRING",    mode = "REQUIRED" },
    { name = "fuel_type",      type = "STRING",    mode = "REQUIRED" },
    { name = "generation_mw",  type = "FLOAT64",   mode = "NULLABLE" }
  ])
}

# Note: entsoe_eu_hourly_wide is intentionally NOT declared here.
# Spark's BigQuery connector auto-creates it on first write (jobs.transform_eu_wide)
# with a schema inferred from the dynamic fuel_* pivot columns. Declaring it in TF
# without a schema is rejected by BQ when field-based partitioning or clustering is set.

# --- Raw tables loaded from parquet (schemas auto-inferred by BQ load) ---

resource "google_bigquery_table" "pse_demand" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "pse_demand"
  deletion_protection = false
  description         = "PSE 15-min demand (batch). Loaded from gs://{bucket}/pse/pse_demand/dt=*/data.parquet."

  time_partitioning {
    type = "DAY"
  }
  # Schema inferred from parquet on first load.
}

resource "google_bigquery_table" "entsoe_prices_pl" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "entsoe_prices_pl"
  deletion_protection = false
  description         = "ENTSO-E day-ahead prices for Poland (batch). Source: ingestion/src/fetch_entsoe.py."

  time_partitioning {
    type = "DAY"
  }
}

resource "google_bigquery_table" "yahoo_prices" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "yahoo_prices"
  deletion_protection = false
  description         = "Yahoo Finance FX + commodity OHLCV (EURPLN, USDPLN, TTF_GAS, BRENT, COAL_API2)."

  time_partitioning {
    type = "DAY"
  }
  # Clustering requires a declared schema; load job will add it if needed.
}

# Streaming sink — consumer writes explicit schema so we declare it here too.
resource "google_bigquery_table" "pse_demand_stream" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "pse_demand_stream"
  deletion_protection = false
  description         = "Real-time PSE demand sink from Kafka consumer (streaming/src/consumer.py)."

  time_partitioning {
    type  = "DAY"
    field = "event_ts"
  }
  clustering = ["interval_id"]

  schema = jsonencode([
    { name = "event_ts",     type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "demand_date",  type = "DATE",      mode = "REQUIRED" },
    { name = "interval_id",  type = "INTEGER",   mode = "REQUIRED" },
    { name = "demand_mw",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "source",       type = "STRING",    mode = "NULLABLE" },
    { name = "ingested_at",  type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

# Staging bucket for Spark BigQuery connector (temp parquet during writes)
resource "google_storage_bucket" "spark_temp" {
  name                        = "${var.gcs_bucket_name}-spark-temp"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}
