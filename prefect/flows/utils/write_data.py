from pathlib import Path
import polars as pl
import pandas as pd

from prefect import task
from prefect_gcp import GcsBucket


@task(log_prints=True)
def write_to_local(df: pl.DataFrame, out_path: Path) -> Path:
    """Write Polars DataFrame to local file."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return out_path


@task(log_prints=True, retries=3)
def write_to_gcs(
    df: pl.DataFrame, out_path: Path, gcs_bucket_block_name: str = "chess-ratings-dev"
) -> Path:
    """Write Polars DataFrame to parquet file in GCS bucket."""
    df: pd.DataFrame = df.to_pandas()  # prefect-gcp requires pandas dataframe
    gcs_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_bucket_block.upload_from_dataframe(
        df=df, to_path=out_path, serialization_format="parquet"
    )
    return out_path


@task(retries=3)
def check_if_file_exists_in_gcs(file_path: Path) -> bool:
    """Determine if a filepath already exists in a GCS Bucket."""
    gcs_bucket_block = GcsBucket.load("chess-ratings-dev")
    blobs = gcs_bucket_block.list_blobs()
    paths = [Path(blob.name) for blob in blobs]
    if file_path in paths:
        return True
    return False
