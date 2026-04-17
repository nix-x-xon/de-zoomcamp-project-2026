"""Fetch Polish grid demand & generation mix from PSE open data API.

Docs: https://api.raporty.pse.pl/ (his-wlk-cal endpoint; old zap-kse retired end-2025)
Coverage: 2024-06-14 onwards.

Usage:
    python -m src.fetch_pse --start 2024-06-14 --end 2026-04-15
"""
from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta

import httpx
import pandas as pd

from .gcs import upload_parquet

PSE_BASE = "https://api.raporty.pse.pl/api"
FETCH_RETRIES = 3
RETRY_BASE_DELAY = 2


def _interval_id_from_dtime(dtime_str: str) -> int:
    # dtime marks end-of-interval (15-min cadence); 00:15 → 1, 24:00 → 96
    t = datetime.fromisoformat(dtime_str)
    return (t.hour * 60 + t.minute) // 15 or 96


def fetch_demand(day: date) -> pd.DataFrame:
    url = f"{PSE_BASE}/his-wlk-cal"
    params = {"$filter": f"business_date eq '{day.isoformat()}'"}
    last_exc: Exception | None = None
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = httpx.get(url, params=params, timeout=30)
            resp.raise_for_status()
            break
        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            # Don't retry 4xx — those are our fault (bad date, retired endpoint).
            if isinstance(exc, httpx.HTTPStatusError) and 400 <= exc.response.status_code < 500:
                raise
            if attempt < FETCH_RETRIES:
                time.sleep(RETRY_BASE_DELAY ** attempt)
    else:
        raise last_exc  # type: ignore[misc]
    records = resp.json().get("value", [])
    df = pd.DataFrame(records)
    if df.empty:
        return df
    # Rename new-API columns to the legacy names the dbt staging model depends on.
    df = df.rename(columns={
        "business_date": "doba",
        "dtime": "udtczas",
        "demand": "zap_kse",
    })
    # Enforce consistent dtypes across days (some days have all-null numeric cols
    # that would otherwise be inferred as INT32 on one file and DOUBLE on another,
    # breaking a multi-file BQ load).
    df["zap_kse"] = df["zap_kse"].astype(float)
    df["znacznik"] = df["udtczas"].map(_interval_id_from_dtime).astype("int64")
    df["ingested_at"] = pd.Timestamp.utcnow()
    # Keep only the columns dbt consumes — avoids INT32/DOUBLE schema drift on
    # the many optional columns (jgm2, jgw2, jgz*, etc.).
    return df[["doba", "udtczas", "zap_kse", "znacznik", "ingested_at"]]


def run(start: date, end: date) -> None:
    day = start
    while day <= end:
        try:
            df = fetch_demand(day)
            if not df.empty:
                upload_parquet(df, source="pse", dataset="demand", partition_date=day)
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            # Log and skip the day on known-transient / known-data errors so the
            # backfill keeps moving. Anything else (auth, OS, bug) propagates.
            print(f"[pse] {day} failed: {exc}")
        day += timedelta(days=1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    args = p.parse_args()
    run(args.start, args.end)
