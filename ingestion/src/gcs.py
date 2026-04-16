"""Helpers for uploading parquet files to GCS with date partitioning."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import storage


def upload_parquet(
    df: pd.DataFrame,
    source: str,
    dataset: str,
    partition_date: date | None = None,
    bucket: str | None = None,
) -> str:
    """Write df to parquet and upload to gs://{bucket}/{source}/{dataset}/dt=YYYY-MM-DD/data.parquet.

    Returns the gs:// URI of the uploaded object.
    """
    bucket_name = bucket or os.environ["GCS_BUCKET"]
    partition_date = partition_date or date.today()

    local_dir = Path("/tmp") / source / dataset
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / f"{partition_date.isoformat()}.parquet"
    df.to_parquet(local_path, index=False)

    blob_path = f"{source}/{dataset}/dt={partition_date.isoformat()}/data.parquet"
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_path).upload_from_filename(local_path)

    uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Uploaded {len(df)} rows → {uri}")
    return uri
