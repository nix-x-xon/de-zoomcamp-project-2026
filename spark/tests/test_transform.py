"""Smoke tests for the EU-wide transform using a local Spark session."""
from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from jobs.transform_eu_wide import add_aggregates, pivot_generation


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[2]")
        .appName("test")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_pivot_and_aggregates(spark):
    ts = datetime(2026, 4, 14, 12, 0)
    rows = [
        (ts.date(), ts, "PL", "Wind Onshore", 1500.0),
        (ts.date(), ts, "PL", "Solar", 500.0),
        (ts.date(), ts, "PL", "Fossil Hard coal", 8000.0),
        (ts.date(), ts, "ES", "Solar", 12000.0),
        (ts.date(), ts, "ES", "Fossil Gas", 2000.0),
    ]
    gen = spark.createDataFrame(rows, ["date", "timestamp", "zone", "fuel_type", "generation_mw"])
    wide = pivot_generation(gen)
    fuel_cols = [c for c in wide.columns if c not in {"date", "timestamp", "zone"}]
    enriched = add_aggregates(wide, fuel_cols).orderBy("zone").collect()

    pl, es = enriched
    assert pl["zone"] == "PL"
    assert pl["total_mw"] == pytest.approx(10000.0)
    assert pl["renewable_share"] == pytest.approx(0.20)
    assert pl["fossil_share"] == pytest.approx(0.80)

    assert es["zone"] == "ES"
    assert es["renewable_share"] == pytest.approx(12000 / 14000, rel=1e-3)
