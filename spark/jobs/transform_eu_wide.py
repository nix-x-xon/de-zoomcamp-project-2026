"""The core Spark transformation: builds the EU-wide hourly wide table.

Reads raw entsoe_eu_prices + entsoe_eu_generation from BigQuery, pivots generation
by fuel type, joins prices on (zone, timestamp), computes derived metrics
(renewable share, residual load, price spread vs DE_LU if present), and writes
to energy_raw.entsoe_eu_hourly_wide (partitioned by date, clustered by zone).

This is the genuine Spark workload: multi-country pivot + join at hourly granularity
across ~13 zones x ~96k hours = ~1.2M rows after pivot.

Usage:
    python -m jobs.transform_eu_wide --mode full
    python -m jobs.transform_eu_wide --mode incremental --date 2026-04-14
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .common import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

# ENTSO-E PSR types we care about (B-codes). See entsoe-py mappings.py for the full list.
RENEWABLE_FUELS = {
    "Wind Onshore", "Wind Offshore", "Solar", "Hydro Run-of-river and poundage",
    "Hydro Water Reservoir", "Hydro Pumped Storage", "Biomass", "Geothermal", "Marine"
}
FOSSIL_FUELS = {
    "Fossil Gas", "Fossil Hard coal", "Fossil Brown coal/Lignite", "Fossil Oil",
    "Fossil Oil shale", "Fossil Peat", "Fossil Coal-derived gas"
}


def read_source(spark: SparkSession, project: str, table: str, date_filter: str | None) -> DataFrame:
    df = spark.read.format("bigquery").option("table", f"{project}.energy_raw.{table}").load()
    if date_filter:
        df = df.where(F.col("date") == F.lit(date_filter))
    return df


def pivot_generation(gen: DataFrame) -> DataFrame:
    """Pivot generation long -> wide: one column per fuel_type."""
    # Replace problematic characters in fuel names for column names
    gen = gen.withColumn(
        "fuel_col",
        F.regexp_replace(F.lower("fuel_type"), "[^a-z0-9]+", "_")
    )
    pivoted = (
        gen.groupBy("date", "timestamp", "zone")
        .pivot("fuel_col")
        .agg(F.sum("generation_mw"))
    )
    return pivoted


def add_aggregates(wide: DataFrame, fuel_cols: list[str]) -> DataFrame:
    """Add total_mw, renewable_share, fossil_share, residual_load."""
    def match_any(col_list: list[str], keywords: set[str]) -> list[str]:
        return [
            c for c in col_list
            if any(k.lower().replace(" ", "_").replace("-", "_").replace("/", "_") in c for k in keywords)
        ]

    renewable_cols = match_any(fuel_cols, RENEWABLE_FUELS)
    fossil_cols = match_any(fuel_cols, FOSSIL_FUELS)

    def safe_sum(cols: list[str]):
        if not cols:
            return F.lit(0.0)
        return sum(F.coalesce(F.col(c), F.lit(0.0)) for c in cols)

    total = safe_sum(fuel_cols)
    renewable = safe_sum(renewable_cols)
    fossil = safe_sum(fossil_cols)
    wind_cols = [c for c in fuel_cols if "wind" in c]
    solar_cols = [c for c in fuel_cols if "solar" in c]

    return (
        wide
        .withColumn("total_mw", total)
        .withColumn("renewable_mw", renewable)
        .withColumn("fossil_mw", fossil)
        .withColumn("renewable_share",
                    F.when(total > 0, renewable / total).otherwise(F.lit(None)))
        .withColumn("fossil_share",
                    F.when(total > 0, fossil / total).otherwise(F.lit(None)))
        .withColumn("residual_load_mw", total - safe_sum(wind_cols) - safe_sum(solar_cols))
    )


def add_rolling_windows(df: DataFrame) -> DataFrame:
    """7-day rolling price mean per zone (168 hours)."""
    w = Window.partitionBy("zone").orderBy(F.col("timestamp").cast("long")) \
              .rangeBetween(-168 * 3600, 0)
    return df.withColumn("price_7d_mean", F.avg("price_eur_mwh").over(w))


def transform(spark: SparkSession, project: str, date_filter: str | None) -> DataFrame:
    prices = read_source(spark, project, "entsoe_eu_prices", date_filter) \
        .select("date", "timestamp", "zone", "price_eur_mwh")

    gen = read_source(spark, project, "entsoe_eu_generation", date_filter) \
        .select("date", "timestamp", "zone", "fuel_type", "generation_mw")

    pivoted = pivot_generation(gen)
    fuel_cols = [c for c in pivoted.columns if c not in {"date", "timestamp", "zone"}]
    log.info("Detected %d fuel columns: %s", len(fuel_cols), fuel_cols)

    enriched = add_aggregates(pivoted, fuel_cols)
    joined = enriched.join(prices, ["date", "timestamp", "zone"], "left")
    windowed = add_rolling_windows(joined)
    return windowed


def write_output(df: DataFrame, project: str, mode: str) -> None:
    table = f"{project}.energy_raw.entsoe_eu_hourly_wide"

    if mode == "full":
        # Full rebuild: overwrite the whole table.
        (
            df.write.format("bigquery")
            .option("table", table)
            .option("partitionField", "date")
            .option("partitionType", "MONTH")
            .option("clusteredFields", "zone")
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save()
        )
    else:
        # Incremental: delete overlapping date partitions before append so
        # re-running the same --date is idempotent.
        dates = [r["date"].isoformat() for r in df.select("date").distinct().collect()]
        if dates:
            client = bigquery.Client(project=project)
            try:
                client.get_table(table)
                in_list = ",".join(f"DATE '{d}'" for d in dates)
                client.query(
                    f"DELETE FROM `{project}.energy_raw.entsoe_eu_hourly_wide` "
                    f"WHERE date IN ({in_list})"
                ).result()
                log.info("Deleted %d overlapping partitions before append", len(dates))
            except Exception:
                log.info("Target %s does not exist yet; skipping pre-delete", table)
        (
            df.write.format("bigquery")
            .option("table", table)
            .option("partitionField", "date")
            .option("partitionType", "MONTH")
            .option("clusteredFields", "zone")
            .option("writeMethod", "direct")
            .mode("append")
            .save()
        )
    log.info("Wrote wide table -> %s", table)


def run(mode: str, date_arg: str | None) -> None:
    project = os.environ["GCP_PROJECT"]
    spark = get_spark(f"transform_eu_wide_{mode}")
    date_filter = None
    if mode == "incremental":
        date_filter = date_arg or (date.today() - timedelta(days=1)).isoformat()
        log.info("Incremental transform for %s", date_filter)
    else:
        log.info("Full transform")
    wide = transform(spark, project, date_filter)
    write_output(wide, project, mode)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p.add_argument("--date", help="ISO date for incremental mode")
    args = p.parse_args()
    run(args.mode, args.date)
