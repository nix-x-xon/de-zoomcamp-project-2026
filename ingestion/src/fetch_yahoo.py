"""Fetch FX rates & commodity futures from Yahoo Finance.

Usage:
    python -m src.fetch_yahoo --start 2026-01-01 --end 2026-04-15
"""
from __future__ import annotations

import argparse
from datetime import date

import pandas as pd
import yfinance as yf

from .gcs import upload_parquet

TICKERS = {
    "EURPLN": "EURPLN=X",
    "USDPLN": "USDPLN=X",
    "TTF_GAS": "TTF=F",
    "BRENT": "BZ=F",
    "NAT_GAS_HH": "NG=F",
}


def run(start: date, end: date) -> None:
    frames = []
    for name, ticker in TICKERS.items():
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
        if df.empty:
            print(f"[yahoo] no data for {ticker}")
            continue
        # yfinance 1.x returns a MultiIndex (field, ticker); collapse to single level.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df.reset_index().rename(columns=str.lower)
        df["ticker"] = name
        frames.append(df[["date", "ticker", "open", "high", "low", "close", "volume"]])

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        upload_parquet(combined, source="yahoo", dataset="prices", partition_date=end)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    args = p.parse_args()
    run(args.start, args.end)
