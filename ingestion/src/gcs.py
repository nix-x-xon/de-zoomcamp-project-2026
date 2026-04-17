"""Helpers for uploading parquet files to GCS with date partitioning."""
from __future__ import annotations

import os
import tempfile
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

    # Use the OS temp dir (works on Linux, macOS, Windows) and clean up after upload
    # so long backfills don't fill /tmp.
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        local_path = Path(tmp.name)
    try:
        df.to_parquet(local_path, index=False)
        blob_path = f"{source}/{dataset}/dt={partition_date.isoformat()}/data.parquet"
        client = storage.Client()
        client.bucket(bucket_name).blob(blob_path).upload_from_filename(local_path)
    finally:
        local_path.unlink(missing_ok=True)

    uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Uploaded {len(df)} rows → {uri}")
    return uri
