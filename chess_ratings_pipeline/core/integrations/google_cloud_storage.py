from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from prefect_gcp.cloud_storage import GcsBucket

from prefect import flow, get_run_logger, task


@task(log_prints=True)
def check_if_file_exists_locally(file_path: Path) -> bool:
    """
    Check if a file exists locally.

    Args:
        file_path (Path): The path of the file to check.

    Returns:
        bool: True if the file exists locally, False otherwise.
    """
    # Create Prefect info logger
    logger = get_run_logger()
    logger.info(f"Checking if file {file_path} exists locally.")

    # Check if file path exists locally
    if file_path.exists():
        logger.info(f"File {file_path} exists locally.")
        return True
    logger.info(f"File {file_path} does not exist locally.")
    return False


@flow(log_prints=True, cache_result_in_memory=False)
def write_dataframe_to_local(
    df: pd.DataFrame | pl.DataFrame, destination: Path, overwrite_existing: bool = True
) -> Path | None:
    """
    Writes a DataFrame to a local file in parquet format.

    Args:
        df (pd.DataFrame | pl.DataFrame):
            The DataFrame to be written.
        destination (Path):
            The desired out file path to write DataFrame to.
        overwrite_existing (bool, optional):
            Whether to overwrite the file if it already exists. Defaults to True.

    Returns:
        Path | None:
            The path to the written file, or None if the file already exists and
            `overwrite_existing` is False.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log task start message
    start_time = datetime.now()
    start_message = f"""Starting `write_dataframe_to_local` flow at {start_time} (local time).
        Inputs:
            df (pd.DataFrame | pl.DataFrame): \n\t{df}
            destination (Path): {destination}"""
    logger.info(start_message)

    if not overwrite_existing:
        # Check if file exists locally
        file_exists: bool = check_if_file_exists_locally(destination)
        # If file exists display message and return None
        if file_exists:
            logger.info(
                f"File {destination} already exists locally and `overwrite_existing=False`. Skipping."
            )
            return None

    # Ensure parent directory of destination file path exists
    logger.info(
        f"Ensuring parent directory of destination file path {destination} exists."
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    # Write df to parquet file at destination
    logger.info(f"Writing DataFrame to parquet file at {destination}.")
    df.write_parquet(destination)
    logger.info(f"Successfully wrote DataFrame to parquet file at {destination}.")

    # Log task end message
    end_time = datetime.now()
    time_taken = end_time - start_time
    end_message = f"""Finished `write_dataframe_to_local` flow at {end_time} (local time).
        Time taken: {time_taken}."""
    logger.info(end_message)

    return destination


@task(log_prints=True, retries=3, retry_delay_seconds=5, timeout_seconds=500)
def check_if_file_exists_in_gcs(file_path: Path, gcs_bucket_block: GcsBucket) -> bool:
    """
    Check if a file exists in a Google Cloud Storage (GCS) bucket.

    Args:
        file_path (Path): The path of the file to check.

    Returns:
        bool: True if the file exists in the GCS bucket, False otherwise.
    """
    # Create Prefect info logger
    logger = get_run_logger()
    logger.info(f"Checking if file {file_path} exists in GCS bucket.")

    # Get list of blobs in GCS bucket
    blobs = gcs_bucket_block.list_blobs()

    # Check if file path exists in list of blobs
    paths = [Path(blob.name) for blob in blobs]
    if file_path in paths:
        logger.info(f"File {file_path} exists in GCS bucket.")
        return True
    logger.info(f"File {file_path} does not exist in GCS bucket.")
    return False


@flow(
    log_prints=True,
    retries=3,
    retry_delay_seconds=5,
    timeout_seconds=500,
    cache_result_in_memory=False,
)
def write_dataframe_to_gcs(
    df: pd.DataFrame | pl.DataFrame,
    destination: Path,
    gcs_bucket_block: GcsBucket,
    overwrite_existing: bool = True,
) -> Path | None:
    """
    Writes a DataFrame to a parquet file in Google Cloud Storage (GCS) bucket.

    Args:
        df (pd.DataFrame | pl.DataFrame):
            The DataFrame to be loaded to GCS.
        destination (Path):
            The destination path in the GCS bucket. Must have parquet file extension.
        gcs_bucket_block_name (str):
            The name of the GCS bucket block.
        overwrite_existing (bool, optional):
            Whether to overwrite the file if it already exists in the GCS bucket.
            Defaults to True.

    Returns:
        Path: The destination file path in the GCS bucket.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log task start message
    start_time = datetime.now()
    start_message = f"""Starting `write_dataframe_to_gcs` flow at {start_time} (local time).
        Inputs:
            df (pd.DataFrame | pl.DataFrame): \n\t{df}
            destination (Path): {destination}
            gcs_bucket_block (GcsBucket): {gcs_bucket_block}
            overwrite_existing (bool): {overwrite_existing}"""
    logger.info(start_message)

    if not overwrite_existing:
        # Check if file exists in GCS bucket
        file_exists = check_if_file_exists_in_gcs(
            file_path=destination, gcs_bucket_block=gcs_bucket_block
        )
        # If file exists display message and return None
        if file_exists:
            logger.info(
                f"File {destination} already exists in GCS bucket and "
                f"{overwrite_existing=}. Skipping."
            )
            return None

    # Write df to parquet file in GCS bucket
    logger.info(
        f"Writing DataFrame to parquet file at {destination} in GCS bucket {gcs_bucket_block.bucket}."
    )
    gcs_bucket_block.upload_from_dataframe(
        df=df, to_path=destination, serialization_format="parquet"
    )
    logger.info(
        f"Successfully wrote DataFrame to parquet file at {destination} in GCS bucket {gcs_bucket_block.bucket}."
    )

    # Log task end message
    end_time = datetime.now()
    time_taken = end_time - start_time
    end_message = f"""Finished `write_dataframe_to_gcs` flow at {end_time} (local time).
        Time taken: {time_taken}."""
    logger.info(end_message)

    return destination
