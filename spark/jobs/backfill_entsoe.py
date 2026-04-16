"""Historical backfill of ENTSO-E day-ahead prices and generation for EU zones.

Iterates (zone x year) chunks (ENTSO-E API enforces a 1-year max per request),
fetches via entsoe-py, and writes partitioned parquet to GCS.

A checkpoint file tracks completed (zone, year) pairs to allow resume after failure.

Usage:
    python -m jobs.backfill_entsoe --start 2015 --end 2025
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd
from entsoe import EntsoePandasClient
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

from .common import ENTSOE_LIMITER, get_spark, load_zones

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

CHECKPOINT_BLOB = "entsoe/_checkpoints/backfill_progress.json"


def load_checkpoint(bucket_name: str) -> set[str]:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(CHECKPOINT_BLOB)
    if not blob.exists():
        return set()
    return set(json.loads(blob.download_as_text()))


def save_checkpoint(bucket_name: str, done: set[str]) -> None:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(CHECKPOINT_BLOB)
    blob.upload_from_string(json.dumps(sorted(done)))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=5, max=120))
def fetch_prices(client: EntsoePandasClient, zone: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    ENTSOE_LIMITER.acquire()
    series = client.query_day_ahead_prices(zone, start=start, end=end)
    df = series.reset_index()
    df.columns = ["timestamp", "price_eur_mwh"]
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    df["zone"] = zone
    return df


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=5, max=120))
def fetch_generation(client: EntsoePandasClient, zone: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    ENTSOE_LIMITER.acquire()
    df = client.query_generation(zone, start=start, end=end, psr_type=None)
    df = df.reset_index()
    ts_col = df.columns[0]
    df = df.melt(id_vars=ts_col, var_name="fuel_type", value_name="generation_mw")
    df = df.rename(columns={ts_col: "timestamp"})
    df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
    df["zone"] = zone
    # fuel_type can be a tuple (psr_type, direction) - keep psr_type only
    df["fuel_type"] = df["fuel_type"].astype(str)
    return df


def write_year(spark, df: pd.DataFrame, zone: str, year: int, dataset: str) -> str:
    """Write a year-chunk as a single parquet partition under country=zone/dt=YYYY-12-31/."""
    if df.empty:
        log.warning("Empty dataframe for %s/%s/%s - skipping", dataset, zone, year)
        return ""
    bucket = os.environ["GCS_BUCKET"]
    path = f"gs://{bucket}/entsoe/{dataset}/country={zone}/dt={year}-12-31/data.parquet"
    sdf = spark.createDataFrame(df)
    sdf.coalesce(1).write.mode("overwrite").parquet(path)
    log.info("Wrote %d rows -> %s", len(df), path)
    return path


def run(start_year: int, end_year: int, zones_filter: list[str] | None = None) -> None:
    bucket = os.environ["GCS_BUCKET"]
    api_key = os.environ["ENTSOE_API_KEY"]
    client = EntsoePandasClient(api_key=api_key)
    spark = get_spark("entsoe_backfill")

    done = load_checkpoint(bucket)
    zones = load_zones()
    if zones_filter:
        zones = [z for z in zones if z["code"] in zones_filter]

    log.info("Backfill %s-%s across %d zones (checkpoint has %d done)",
             start_year, end_year, len(zones), len(done))

    for zone in zones:
        code = zone["code"]
        for year in range(start_year, end_year + 1):
            key = f"{code}:{year}"
            if key in done:
                log.info("Skip %s (checkpoint)", key)
                continue
            start_ts = pd.Timestamp(f"{year}-01-01", tz="UTC")
            end_ts = pd.Timestamp(f"{year + 1}-01-01", tz="UTC")
            try:
                prices = fetch_prices(client, code, start_ts, end_ts)
                write_year(spark, prices, code, year, "eu_prices")
            except Exception as exc:
                log.error("prices %s %s failed: %s", code, year, exc)
            try:
                gen = fetch_generation(client, code, start_ts, end_ts)
                write_year(spark, gen, code, year, "eu_generation")
            except Exception as exc:
                log.error("generation %s %s failed: %s", code, year, exc)
            done.add(key)
            save_checkpoint(bucket, done)

    spark.stop()
    log.info("Backfill complete")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=int, default=2015)
    p.add_argument("--end", type=int, default=date.today().year)
    p.add_argument("--zones", nargs="*", help="Filter to specific zone codes")
    args = p.parse_args()
    run(args.start, args.end, args.zones)
