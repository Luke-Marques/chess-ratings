from pathlib import Path
import polars as pl
import pandas as pd

from prefect import task
from prefect_gcp import GcsBucket


@task(log_prints=True, cache_result_in_memory=False, persist_result=False)
def write_to_local(df: pl.DataFrame, out_path: Path) -> Path:
    """
    Writes a Polars DataFrame to a local parquet file, whilst ensuring parent-directory
    exists.

    Args:
        df (pl.DataFrame): The Polars DataFrame to be written.
        out_path (Path): The path to the output parquet file.

    Returns:
        Path: The path to the output parquet file.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return out_path


@task(log_prints=True, retries=3, cache_result_in_memory=False, persist_result=False)
def write_to_gcs(
    df: pl.DataFrame, out_path: Path, gcs_bucket_block_name: str = "chess-ratings-dev"
) -> Path:
    """
    Writes a Polars DataFrame to a parquet file in a Google Cloud Storage (GCS) bucket
    using the specified output path and GCS bucket Prefect block name.

    Args:
        df (pl.DataFrame): The Polars DataFrame to be written to GCS.
        out_path (Path): The output path in GCS where the DataFrame will be stored.
        gcs_bucket_block_name (str, optional):
            The name of the GCS bucket Prefect block. Defaults to "chess-ratings-dev".

    Returns:
        Path: The output path where the DataFrame was stored in GCS.
    """
    df: pd.DataFrame = df.to_pandas()  # prefect-gcp requires pandas dataframe
    gcs_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    gcs_bucket_block.upload_from_dataframe(
        df=df, to_path=out_path, serialization_format="parquet"
    )
    return out_path


@task(retries=3)
def check_if_file_exists_in_gcs(
    file_path: Path, gcs_bucket_block_name: str = "chess-ratings-dev"
) -> bool:
    """
    Check if a file exists in a Google Cloud Storage (GCS) bucket.

    Args:
        file_path (Path): The path of the file to check.

    Returns:
        bool: True if the file exists in the GCS bucket, False otherwise.
    """
    gcs_bucket_block = GcsBucket.load(gcs_bucket_block_name)
    blobs = gcs_bucket_block.list_blobs()
    paths = [Path(blob.name) for blob in blobs]
    if file_path in paths:
        return True
    return False
