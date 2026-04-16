"""Fetch Polish grid demand & generation mix from PSE open data API.

Docs: https://api.raporty.pse.pl/

Usage:
    python -m src.fetch_pse --start 2026-01-01 --end 2026-04-15
"""
from __future__ import annotations

import argparse
from datetime import date, timedelta

import httpx
import pandas as pd

from .gcs import upload_parquet

PSE_BASE = "https://api.raporty.pse.pl/api"


def fetch_demand(day: date) -> pd.DataFrame:
    url = f"{PSE_BASE}/zap-kse"
    params = {"$filter": f"doba eq '{day.isoformat()}'"}
    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()
    records = resp.json().get("value", [])
    df = pd.DataFrame(records)
    df["ingested_at"] = pd.Timestamp.utcnow()
    return df


def run(start: date, end: date) -> None:
    day = start
    while day <= end:
        try:
            df = fetch_demand(day)
            if not df.empty:
                upload_parquet(df, source="pse", dataset="demand", partition_date=day)
        except Exception as exc:
            print(f"[pse] {day} failed: {exc}")
        day += timedelta(days=1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    args = p.parse_args()
    run(args.start, args.end)
