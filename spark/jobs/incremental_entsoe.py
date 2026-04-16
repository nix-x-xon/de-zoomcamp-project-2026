"""Daily incremental fetch of ENTSO-E prices & generation for all EU zones.

Fetches a single day (default: yesterday, UTC) and writes per-day parquet to GCS.
Designed for daily Kestra trigger after ENTSO-E publishes final settled data (~06:00 UTC).

Usage:
    python -m jobs.incremental_entsoe --date 2026-04-14
"""
from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta

import pandas as pd
from entsoe import EntsoePandasClient

from .backfill_entsoe import fetch_generation, fetch_prices
from .common import get_spark, load_zones

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def run(target_date: date, zones_filter: list[str] | None = None) -> None:
    bucket = os.environ["GCS_BUCKET"]
    api_key = os.environ["ENTSOE_API_KEY"]
    client = EntsoePandasClient(api_key=api_key)
    spark = get_spark(f"entsoe_incremental_{target_date}")

    zones = load_zones()
    if zones_filter:
        zones = [z for z in zones if z["code"] in zones_filter]

    start = pd.Timestamp(target_date, tz="UTC")
    end = start + pd.Timedelta(days=1)

    for zone in zones:
        code = zone["code"]
        try:
            prices = fetch_prices(client, code, start, end)
            if not prices.empty:
                path = f"gs://{bucket}/entsoe/eu_prices/country={code}/dt={target_date}/data.parquet"
                spark.createDataFrame(prices).coalesce(1).write.mode("overwrite").parquet(path)
                log.info("prices %s: %d rows -> %s", code, len(prices), path)
        except Exception as exc:
            log.error("prices %s failed: %s", code, exc)

        try:
            gen = fetch_generation(client, code, start, end)
            if not gen.empty:
                path = f"gs://{bucket}/entsoe/eu_generation/country={code}/dt={target_date}/data.parquet"
                spark.createDataFrame(gen).coalesce(1).write.mode("overwrite").parquet(path)
                log.info("generation %s: %d rows -> %s", code, len(gen), path)
        except Exception as exc:
            log.error("generation %s failed: %s", code, exc)

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", type=date.fromisoformat,
                   default=date.today() - timedelta(days=1))
    p.add_argument("--zones", nargs="*")
    args = p.parse_args()
    run(args.date, args.zones)
