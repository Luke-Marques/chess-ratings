from datetime import datetime, timedelta
from itertools import product
from pathlib import Path
from typing import List, Tuple

import polars as pl
from prefect import flow
from prefect.logging import get_run_logger
from prefect.runtime import flow_run
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from chess_ratings_pipeline.core.integrations.fide import (
    FideGameFormat,
    check_valid_month,
    check_valid_year,
    clean_fide_ratings,
    extract_fide_ratings,
    generate_file_path,
)
from chess_ratings_pipeline.core.integrations.google_cloud_storage import (
    write_dataframe_to_gcs,
    write_dataframe_to_local,
)
from chess_ratings_pipeline.core.integrations.google_bigquery import (
    create_external_bq_table,
)


def generate_extract_single_fide_ratings_dataset_flow_name() -> str:
    """
    provided parameters to the flow.

    Returns:
        str: The generated flow name.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    year: int = parameters["year"]
    month: int = parameters["month"]
    fide_game_format: FideGameFormat = parameters["fide_game_format"]
    name = f"{flow_name}-{year}-{month}-{fide_game_format.value.lower()}"
    return name


def generate_elt_fide_ratings_flow_name() -> str:
    """
    Generates a flow name for the elt_fide_ratings flow based on the parameters provided
    to the flow.

    Returns:
        str: The generated flow name.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    years: List[int] | int = parameters["years"]
    months: List[int] | int = parameters["months"]
    fide_game_format: str = parameters["fide_game_format"]
    if isinstance(years, int) and isinstance(months, int):
        name = f"{flow_name}-{years}-{months}-{fide_game_format}"
    elif isinstance(years, int) and not isinstance(months, int):
        name = f"{flow_name}-{years}-{min(months)}-to-{max(months)}-{fide_game_format}"
    elif not isinstance(years, int) and isinstance(months, int):
        name = f"{flow_name}-{min(years)}-to-{max(years)}-{months}-{fide_game_format}"
    else:
        name = (
            f"{flow_name}-{min(years)}-to-{max(years)}-{min(months)}-to-{max(months)}-"
            f"{fide_game_format}"
        )
    return name


@flow(
    flow_run_name=generate_extract_single_fide_ratings_dataset_flow_name,
    log_prints=True,
)
def extract_single_fide_ratings_dataset(
    year: int,
    month: int,
    fide_game_format: FideGameFormat,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
) -> None:
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `extract_single_fide_ratings_dataset` sub-flow at {start_time} (local time).
    Inputs:
        year (int): {year}
        month (int): {month}
        fide_game_format (str): {fide_game_format}
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}"""
    logger.info(start_message)

    # Validate year and month values
    logger.info(f"Validating {year = } and {month = } values...")
    check_valid_year(year)
    check_valid_month(month)
    logger.info(f"Validated {year = } and {month = } values.")

    # Extract FIDE ratings dataset from web and convert from compressed XML to DataFrame
    logger.info(
        f"Extracting FIDE ratings data for {year}-{month} {fide_game_format.value}..."
    )
    fide_ratings: pl.DataFrame = extract_fide_ratings(
        year, month, fide_game_format.value
    )
    logger.info(
        f"Extracted FIDE ratings data for {year}-{month} {fide_game_format.value}."
    )
    logger.info(f"FIDE Ratings DataFrame shape: {fide_ratings.shape}")

    # Apply initial cleaning/pre-processing to FIDE ratings DataFrame
    logger.info(
        f"Cleaning FIDE ratings data for {year}-{month} {fide_game_format.value}..."
    )
    fide_ratings: pl.DataFrame = clean_fide_ratings(fide_ratings, year, month)
    logger.info(
        f"Cleaned FIDE ratings data for {year}-{month} {fide_game_format.value}."
    )
    logger.info(f"Cleaned FIDE Ratings DataFrame shape: {fide_ratings.shape}")

    # Generate destination parquet file path for writing of FIDE ratings DataFrame
    logger.info(
        "Generating destination file path for FIDE ratings data for "
        f"{year}-{month} {fide_game_format.value}..."
    )
    destination: Path = generate_file_path(year, month, fide_game_format)
    logger.info(
        "Generated destination file path for FIDE ratings data for "
        f"{year}-{month} {fide_game_format.value}: {destination}"
    )

    # Write FIDE ratings DataFrame to a parquet file in a GCS bucket and/or locally
    logger.info(
        f"Writing FIDE ratings data for {year}-{month} {fide_game_format.value} "
        f"to GCS bucket {gcs_bucket_block.bucket} at {destination}..."
    )
    write_dataframe_to_gcs(
        fide_ratings, destination, gcs_bucket_block, overwrite_existing
    )
    logger.info(
        "Finished writing FIDE ratings data for "
        f"{year}-{month} {fide_game_format.value} "
        f"to GCS bucket {gcs_bucket_block.bucket} at {destination}."
    )
    if store_local:
        logger.info(
            f"Writing FIDE ratings data for {year}-{month} {fide_game_format.value} "
            f"to {destination}..."
        )
        write_dataframe_to_local(fide_ratings, destination, overwrite_existing)
        logger.info(
            "Finished writing FIDE ratings data for "
            f"{year}-{month} {fide_game_format.value} "
            f"to {destination}."
        )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `extract_single_fide_ratings_dataset` sub-flow at {start_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)


@flow(log_prints=True)
def load_fide_ratings_to_bq_external_table(
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    project: str = "fide-chess-ratings",
    bq_dataset_name: str = "chess_ratings",
    bq_table_name: str = "landing_fide__ratings",
) -> str:
    # Create Prefect logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"Starting `create_fide_ratings_external_bq_table` flow at {start_time} (local time)."
    logger.info(start_message)

    # Get list of parent directories containing FIDE ratings Parquet files in GCS bucket
    dirs: List[str] = gcs_bucket_block.list_folders(
        str(generate_file_path(2000, 1, FideGameFormat.STANDARD).parent.parent)
    )

    # Define URI patterns for FIDE ratings Parquet files in GCS bucket
    source_uris: List[str] = [
        f"gs://{gcs_bucket_block.bucket}/{dir}/*.parquet" for dir in dirs
    ]

    # Create external BigQuery table from list of URIs if table does not already exist
    create_external_bq_table(
        source_uris=source_uris,
        dataset=bq_dataset_name,
        table=bq_table_name,
        project=project,
        gcp_credentials=gcp_credentials_block,
        return_state=True,
    )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `create_fide_ratings_external_bq_table` flow at {end_time} (local time).
        Time taken: {time_taken}."""
    logger.info(end_message)


@flow(flow_run_name=generate_elt_fide_ratings_flow_name, log_prints=True)
def elt_fide_ratings(
    years: List[int] | int = datetime.today().year,
    months: List[int] | int = datetime.today().month,
    fide_game_format: str = "all",
    gcp_credentials_block_name: str = "gcp-creds-chess-ratings",
    gcs_bucket_block_name: str = "chess-ratings-dev",
    store_local: bool = False,
    overwrite_existing: bool = True,
    bq_dataset_name: str = "chess_ratings",
    bq_table_name: str = "landing_fide__ratings",
) -> None:
    """
    Prefect parent-flow for the Extract-Load-Transform (ELT) process for FIDE ratings
    data:
        - Extracts compressed XML FIDE ratings data from www.fide.com and converts to
          Polars DataFrame.
        - Applies initial cleaning/pre-processing to the FIDE ratings DataFrame.
        - Writes the FIDE ratings DataFrame to a parquet file in a GCS bucket.
        - Loads the FIDE ratings data from the GCS bucket to BigQuery.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `elt_fide_ratings` parent-flow at {start_time} (local time).
    Inputs:
        years (List[int] | int): {years}
        months (List[int] | int): {months}
        fide_game_format (str): {fide_game_format}
        gcp_credentials_block_name (str): {gcp_credentials_block_name}
        gcs_bucket_block_name (str): {gcs_bucket_block_name}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name (str): {bq_table_name}"""
    logger.info(start_message)

    # Convert int year/month values to lists
    if isinstance(years, int):
        years = [years]
    if isinstance(months, int):
        months = [months]

    # Convert fide_game_formats to GameFormat object(s)
    match fide_game_format:
        case "all":
            fide_game_formats = [
                FideGameFormat.STANDARD,
                FideGameFormat.RAPID,
                FideGameFormat.BLITZ,
            ]
        case "standard":
            fide_game_formats = [FideGameFormat.STANDARD]
        case "rapid":
            fide_game_formats = [FideGameFormat.RAPID]
        case "blitz":
            fide_game_formats = [FideGameFormat.BLITZ]
    del fide_game_format

    # Load GCP credentials Prefect block
    logger.info(
        f"Loading GCP credentials Prefect block {gcp_credentials_block_name}..."
    )
    gcp_credentials_block: GcpCredentials = GcpCredentials.load(
        gcp_credentials_block_name
    )
    logger.info(f"Loaded GCP credentials Prefect block {gcp_credentials_block_name}.")

    # Load GCS bucket Prefect block
    logger.info(f"Loading GCS bucket Prefect block {gcs_bucket_block_name}...")
    gcs_bucket_block: GcsBucket = GcsBucket.load(gcs_bucket_block_name)
    logger.info(
        f"Loaded GCS bucket Prefect block. Bucket name: {gcs_bucket_block.bucket}"
    )

    # Extract FIDE ratings data for each year/month and game format to GCS
    logger.info(
        "Running FIDE ratings ELT sub-flow for each year/month and game-format "
        "combination..."
    )
    date_game_format_combinations: List[Tuple[int, int, FideGameFormat]] = list(
        product(years, months, fide_game_formats)
    )
    for index, (year, month, fide_game_format) in enumerate(
        date_game_format_combinations
    ):
        logger.info(
            f"Submitting FIDE ratings extraction sub-flow for {year}-{month} "
            f"{fide_game_format.value}, dataset {index+1} of "
            f"{len(date_game_format_combinations)}..."
        )
        extract_single_fide_ratings_dataset(
            year,
            month,
            fide_game_format,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
            return_state=True,
        )
        logger.info(
            f"Finished extraction sub-flow for {year}-{month} {fide_game_format.value}."
        )

    # Load FIDE ratings data to BigQuery external table
    logger.info(
        f"Loading FIDE ratings data to BigQuery external table `{bq_dataset_name}.{bq_table_name}`..."
    )
    load_fide_ratings_to_bq_external_table(
        gcp_credentials_block=gcp_credentials_block,
        gcs_bucket_block=gcs_bucket_block,
        bq_dataset_name=bq_dataset_name,
        bq_table_name=bq_table_name,
        return_state=True,
    )
    logger.info("Finished loading FIDE ratings data to BigQuery external table.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `elt_fide_ratings` parent-flow at {start_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)


if __name__ == "__main__":
    elt_fide_ratings()
