"""Shared helpers for Spark ENTSO-E jobs.

Provides:
- SparkSession builder (GCS + BQ connectors pre-configured via spark-defaults.conf)
- Bidding-zone config loader
- ENTSO-E API rate limiter (400 req/min, 10-min ban on breach)
- GCS path builder (Hive-style dt= partitioning, consistent with batch ingestion)
"""
from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from datetime import date
from pathlib import Path
from threading import Lock

from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

CONF_DIR = Path(os.getenv("SPARK_CONF_DIR", "/app/conf"))
ZONES_FILE = CONF_DIR / "bidding_zones.json"


def get_spark(app_name: str) -> SparkSession:
    """Build a SparkSession using the GCP service-account credentials from env."""
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    temp_bucket = os.getenv("SPARK_TEMP_BUCKET", os.environ["GCS_BUCKET"])

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
        .config("temporaryGcsBucket", temp_bucket)
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_zones() -> list[dict]:
    """Return the list of bidding zones from conf/bidding_zones.json."""
    return json.loads(ZONES_FILE.read_text())["zones"]


def zones_by_region(region: str | None = None) -> list[dict]:
    zones = load_zones()
    if region is None:
        return zones
    return [z for z in zones if z["region"].lower() == region.lower()]


def gcs_path(source: str, dataset: str, zone: str, partition_date: date | str) -> str:
    bucket = os.environ["GCS_BUCKET"]
    if hasattr(partition_date, "isoformat"):
        partition_date = partition_date.isoformat()
    return f"gs://{bucket}/{source}/{dataset}/country={zone}/dt={partition_date}/data.parquet"


class RateLimiter:
    """Sliding-window rate limiter for the ENTSO-E REST API.

    Default: 350 requests per 60-second window (leaving headroom under the 400/min hard cap).
    Shared across threads via a class-level Lock.
    """

    def __init__(self, max_requests: int = 350, window_seconds: int = 60) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self._events: deque[float] = deque()
        self._lock = Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            while self._events and now - self._events[0] > self.window:
                self._events.popleft()
            if len(self._events) >= self.max_requests:
                sleep_for = self.window - (now - self._events[0]) + 0.1
                log.warning("Rate limit reached, sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)
            self._events.append(time.monotonic())


ENTSOE_LIMITER = RateLimiter()
