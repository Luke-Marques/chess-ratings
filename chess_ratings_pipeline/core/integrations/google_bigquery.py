from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

from google.cloud.bigquery import CreateDisposition, SourceFormat, WriteDisposition
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials

from prefect import flow, get_run_logger, task


def prettify_list(list: list, indent_size=4, initial_indent=0, seperator=",") -> str:
    """
    Returns a string representation of the given list, with each element on a new line
    and indented.

    Args:
        list (list): The list to be prettified.
        indent_size (int): The number of spaces to indent each line. Default is 4.
        initial_indent (int):
            The number of spaces to indent the first and last line (the brackets).
            Default is 0.
        seperator (str): The separator to use between elements in list. Default is ",".

    Returns:
        str: The prettified string representation of the list.
    """
    prettified_list = (
        f"{" " * initial_indent}"
        f"[\n{" " * (initial_indent + indent_size)}"
        f"{(seperator + "\n" + (" " * (initial_indent+indent_size))).join(list)}"
        f"\n{" " * initial_indent}]"
    )
    return prettified_list


@flow(log_prints=True, retries=3)
def load_file_gcs_to_bq(
    gcs_file: Path,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    dataset: str,
    table_name: str,
    location: Optional[str] = "europe_north1",
) -> None:
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `load_file_from_gcs_to_bigquery` flow at {start_time} (local time).
    Inputs:
        gcs_file (Path): {gcs_file}
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        dataset (str): {dataset}
        table_name (str): {table_name}
        location (str): {location}"""
    logger.info(start_message)

    # Define GCS bucket prefix to prepend to GCS file path
    GCS_BUCKET_PREFIX = f"gs://{gcs_bucket_block.bucket}"

    # Load file to BigQuery table
    logger.info(f"Loading {gcs_file} to BQ Warehouse...")
    bigquery_load_cloud_storage(
        dataset=dataset,
        table=table_name,
        uri=f"{GCS_BUCKET_PREFIX}/{gcs_file}",
        gcp_credentials=gcp_credentials_block,
        location=location,
        job_config={
            "autodetect": True,
            "source_format": SourceFormat.PARQUET,
            "create_disposition": CreateDisposition.CREATE_IF_NEEDED,
            "write_disposition": WriteDisposition.WRITE_APPEND,
        },
    )
    logger.info("Finished loading file to BQ Warehouse.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `load_file_from_gcs_to_bigquery` flow at {end_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)


@flow(log_prints=True)
def load_files_gcs_to_bq(
    *gcs_files: Path | Iterable[Path],
    table_name: str,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    dataset: str,
    location: Optional[str] = "europe_north1",
) -> None:
    # Create Prefect info logger
    logger = get_run_logger()

    # Unpack nested-lists of file paths to get a flat list of file paths
    gcs_files: List[Path] = [
        gcs_file
        for gcs_file in gcs_files
        for gcs_file in (gcs_file if isinstance(gcs_file, Iterable) else [gcs_file])
    ]

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `load_file_from_gcs_to_bigquery` flow at {start_time} (local time).
    Inputs:
        gcs_files (List[Path]):
    {prettify_list(gcs_files, initial_indent=8, indent_size=4)}
        table_name (str): {table_name}
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        dataset (str): {dataset}
        location (str): {location}"""
    print(start_message)
    logger.info(start_message)

    # Load each file to BigQuery table
    logger.info(f"Loading {len(gcs_files)} files to BQ Warehouse...")
    for index, gcs_file in enumerate(gcs_files):
        logger.info(f"Loading {gcs_file} ({index}/{len(gcs_files)}) to BQ Warehouse...")
        load_file_gcs_to_bq(
            gcs_file,
            gcp_credentials_block,
            gcs_bucket_block,
            dataset,
            table_name,
            location,
        )
        logger.info(f"Finished loading {gcs_file} to BQ Warehouse.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `load_files_from_gcs_to_bigquery` flow at {end_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)
