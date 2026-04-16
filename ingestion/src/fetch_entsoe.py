"""Fetch day-ahead prices & generation from ENTSO-E Transparency Platform.

Requires ENTSOE_API_KEY env var (free registration at transparency.entsoe.eu).

Usage:
    python -m src.fetch_entsoe --country PL --start 2026-01-01 --end 2026-04-15
"""
from __future__ import annotations

import argparse
import os
from datetime import date

import pandas as pd
from entsoe import EntsoePandasClient

from .gcs import upload_parquet


def run(country: str, start: date, end: date) -> None:
    client = EntsoePandasClient(api_key=os.environ["ENTSOE_API_KEY"])
    start_ts = pd.Timestamp(start, tz="Europe/Warsaw")
    end_ts = pd.Timestamp(end, tz="Europe/Warsaw")

    prices = client.query_day_ahead_prices(country, start=start_ts, end=end_ts)
    prices_df = prices.reset_index()
    prices_df.columns = ["timestamp", "price_eur_mwh"]
    prices_df["country"] = country
    upload_parquet(prices_df, source="entsoe", dataset=f"prices_{country.lower()}",
                   partition_date=end)

    gen = client.query_generation(country, start=start_ts, end=end_ts, psr_type=None)
    # In entsoe-py 0.7.x columns are MultiIndex (fuel_type, direction); drop direction.
    gen.columns = [c[0] if isinstance(c, tuple) else c for c in gen.columns]
    gen_df = gen.reset_index(names="timestamp").melt(
        id_vars="timestamp",
        var_name="fuel_type",
        value_name="generation_mw",
    )
    gen_df["country"] = country
    upload_parquet(gen_df, source="entsoe", dataset=f"generation_{country.lower()}",
                   partition_date=end)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--country", default="PL")
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    args = p.parse_args()
    run(args.country, args.start, args.end)
