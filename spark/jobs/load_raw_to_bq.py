"""Load GCS parquet partitions into BigQuery raw tables via Spark BQ connector.

Reads from entsoe/{eu_prices,eu_generation}/country=*/dt=<date_range>/*.parquet,
appends to energy_raw.entsoe_eu_prices and energy_raw.entsoe_eu_generation.

Tables are partitioned by DATE(timestamp), clustered by zone.

Usage:
    python -m jobs.load_raw_to_bq                      # loads yesterday
    python -m jobs.load_raw_to_bq --start 2015 --end 2025  # bulk load year range
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def write_to_bq(df: DataFrame, table: str, partition_field: str, cluster_fields: list[str]) -> None:
    (
        df.write.format("bigquery")
        .option("table", table)
        .option("partitionField", partition_field)
        .option("partitionType", "DAY")
        .option("clusteredFields", ",".join(cluster_fields))
        .option("writeMethod", "direct")
        .mode("append")
        .save()
    )


def _read_or_skip(spark: SparkSession, pattern: str):
    try:
        return spark.read.parquet(pattern)
    except Exception as exc:  # pyspark raises AnalysisException for PATH_NOT_FOUND
        log.warning("Skipping %s (%s)", pattern, exc.__class__.__name__)
        return None


def load_prices(spark: SparkSession, pattern: str, table: str) -> None:
    df = _read_or_skip(spark, pattern)
    if df is None:
        return
    df = df.withColumn("date", F.to_date("timestamp"))
    cnt = df.count()
    if cnt == 0:
        log.warning("No price rows matched %s", pattern)
        return
    write_to_bq(df.select("date", "timestamp", "zone", "price_eur_mwh"),
                table, "date", ["zone"])
    log.info("Loaded %d price rows -> %s", cnt, table)


def load_generation(spark: SparkSession, pattern: str, table: str) -> None:
    df = _read_or_skip(spark, pattern)
    if df is None:
        return
    df = df.withColumn("date", F.to_date("timestamp"))
    cnt = df.count()
    if cnt == 0:
        log.warning("No generation rows matched %s", pattern)
        return
    write_to_bq(df.select("date", "timestamp", "zone", "fuel_type", "generation_mw"),
                table, "date", ["zone", "fuel_type"])
    log.info("Loaded %d generation rows -> %s", cnt, table)


def run(date_filter: str | None, project: str) -> None:
    bucket = os.environ["GCS_BUCKET"]
    spark = get_spark("load_entsoe_to_bq")

    if date_filter:
        prices_pattern = f"gs://{bucket}/entsoe/eu_prices/country=*/dt={date_filter}/*.parquet"
        gen_pattern = f"gs://{bucket}/entsoe/eu_generation/country=*/dt={date_filter}/*.parquet"
    else:
        prices_pattern = f"gs://{bucket}/entsoe/eu_prices/country=*/dt=*/*.parquet"
        gen_pattern = f"gs://{bucket}/entsoe/eu_generation/country=*/dt=*/*.parquet"

    load_prices(spark, prices_pattern, f"{project}.energy_raw.entsoe_eu_prices")
    load_generation(spark, gen_pattern, f"{project}.energy_raw.entsoe_eu_generation")
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="Specific dt= partition, e.g. 2026-04-14 or 2025-*")
    p.add_argument("--yesterday", action="store_true", help="Load yesterday's partition")
    args = p.parse_args()

    project = os.environ["GCP_PROJECT"]
    if args.yesterday:
        run((date.today() - timedelta(days=1)).isoformat(), project)
    else:
        run(args.date, project)
