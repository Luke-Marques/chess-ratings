from datetime import datetime
from typing import List, Literal

import polars as pl
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.profiles import (
    extract_titled_cdc_profiles,
    clean_cdc_profiles,
    generate_cdc_profiles_file_path,
)
from chess_ratings_pipeline.core.integrations.google_cloud_storage import (
    write_dataframe_to_gcs,
    write_dataframe_to_local,
)
from chess_ratings_pipeline.core.integrations.google_bigquery import load_file_gcs_to_bq
from prefect import flow
from prefect.logging import get_run_logger


@flow(log_prints=True)
def elt_single_title_cdc_profiles(
    chess_title: ChessTitle,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
    bq_dataset_name: str = "landing",
    bq_table_name: str = "cdc_profiles",
) -> None:
    """
    Extracts Chess.com player profiles for the specified ChessTitle, cleans and
    validates the data, writes it to a parquet file in a GCS bucket and/or locally, and
    loads it into a BigQuery data warehouse table.

    Args:
        chess_title (ChessTitle):
            The title of the chess players to extract profiles for.
        gcp_credentials_block (GcpCredentials):
            The GCP credentials block for authentication.
        gcs_bucket_block (GcsBucket):
            The GCS bucket block for writing the data.
        store_local (bool):
            Flag indicating whether to store the data locally.
        overwrite_existing (bool):
            Flag indicating whether to overwrite existing data.
        bq_dataset_name (str, optional):
            The name of the BigQuery dataset. Defaults to "landing".
        bq_table_name (str, optional):
            The name of the BigQuery table. Defaults to "cdc_profiles".

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `elt_single_title_cdc_profiles` sub-flow at {start_time} (local time).
    Inputs:
        chess_title (ChessTitle): {chess_title}
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name (str): {bq_table_name}"""
    logger.info(start_message)

    # Extract Chess.com player profiles for the specified ChessTitle
    logger.info(
        "Extracting Chess.com player profiles for "
        f"{chess_title.value} titled players on "
        f"{datetime.now().date} at {datetime.now().time}..."
    )
    cdc_profiles: pl.DataFrame = extract_titled_cdc_profiles(chess_title)
    logger.info(f"Extracted {cdc_profiles.shape[0]} player profiles.")
    logger.info(f"Profiles DataFrame: \n\t{cdc_profiles}")

    # Apply initial cleaning/pre-processing to player profiles DataFrame
    logger.info(
        f"Cleaning Chess.com {chess_title.value} titled player profiles DataFrame..."
    )
    cdc_profiles: pl.DataFrame = clean_cdc_profiles(cdc_profiles)
    logger.info(
        f"Cleaned Chess.com {chess_title.value} titled player profiles DataFrame: "
        f"\n\t{cdc_profiles}"
    )

    # Generate destination parquet file path for writing of player profiles DataFrame
    logger.info(
        "Generating destination parquet file path for writing of player profiles "
        "DataFrame..."
    )
    destination: str = generate_cdc_profiles_file_path(chess_title)
    logger.info(
        f"Generated destination parquet file path for Chess.com {chess_title.value} "
        f"titled player profiles: {destination}"
    )

    # Write player profiles DataFrame to parquet file in GCS bucket and/or locally
    logger.info(
        f"Writing cleaned Chess.com {chess_title.value} titled player profiles "
        f"DataFrame to GCS bucket {gcs_bucket_block.bucket} at {destination}..."
    )
    write_dataframe_to_gcs(
        cdc_profiles, destination, gcs_bucket_block, overwrite_existing
    )
    logger.info("Finished writing player profiles DataFrame to GCS bucket.")
    if store_local:
        logger.info(
            f"Writing cleaned Chess.com {chess_title.value} titled player profiles "
            f"DataFrame to {destination}..."
        )
        write_dataframe_to_local(cdc_profiles, destination, overwrite_existing)
        logger.info("Finished writing player profiles DataFrame locally.")

    # Load player profiles data from GCS bucket to BigQuery
    logger.info(
        f"Loading cleaned Chess.com {chess_title.value} titled player profiles "
        f"data to BigQuery data warehouse {bq_dataset_name}/{bq_table_name}..."
    )
    load_file_gcs_to_bq(
        gcs_file=destination,
        gcp_credentials_block=gcp_credentials_block,
        dataset=bq_dataset_name,
        table_name=bq_table_name,
    )
    logger.info("Finished loading player profiles data to BigQuery.")

    # Log flow end message
    end_time = datetime.now()
    time_taken = end_time - start_time
    end_message = f"""Finished `elt_single_title_cdc_profiles` sub-flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


@flow(log_prints=True)
def elt_cdc_profiles(
    chess_titles: ChessTitle | List[ChessTitle] | Literal["all"] = "all",
    gcp_credentials_block_name: str = "gcp-creds-chess-ratings",
    gcs_bucket_block_name: str = "chess-ratings-dev",
    store_local: bool = False,
    overwrite_existing: bool = True,
) -> None:
    """
    Extract, load, and transform Chess.com player profiles for the specified chess
    titles.

    Args:
        chess_titles (ChessTitle | List[ChessTitle] | Literal["all"], optional):
            The chess titles to process. Defaults to "all".
        gcp_credentials_block_name (str, optional):
            The name of the GCP credentials Prefect block. Defaults to
            "gcp-creds-chess-ratings".
        gcs_bucket_block_name (str, optional):
            The name of the GCS bucket Prefect block. Defaults to "chess-ratings-dev".
        store_local (bool, optional):
            Flag indicating whether to store the data locally. Defaults to False.
        overwrite_existing (bool, optional):
            Flag indicating whether to overwrite existing data. Defaults to True.

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `elt_cdc_profiles` flow at {start_time} (local time).
    Inputs:
        chess_titles (ChessTitle | List[ChessTitle] | Literal["all"]): {chess_titles}
        gcp_credentials_block_name (str): {gcp_credentials_block_name}
        gcs_bucket_block_name (str): {gcs_bucket_block_name}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}"""
    logger.info(start_message)

    # If chess_titles is "all", set it to list of all ChessTitle objects
    if chess_titles == "all":
        chess_titles = list(ChessTitle)

    # If chess_titles is not a list, convert it to a list
    if not isinstance(chess_titles, list):
        chess_titles = [chess_titles]

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

    # ELT Chess.com player profiles for each ChessTitle
    logger.info(
        "Running Chess.com player profiles ELT sub-flow for each chess title "
        "specified..."
    )
    for chess_title in chess_titles:
        logger.info(
            f"Running Chess.com player profiles ELT sub-flow for Chess.com players "
            f"with {chess_title.value} titles..."
        )
        elt_single_title_cdc_profiles(
            chess_title,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
        )
        logger.info(
            f"Finished Chess.com player profiles ELT sub-flow for Chess.com players "
            f"with {chess_title.value} titles."
        )

    # Log flow end message
    end_time = datetime.now()
    time_taken = end_time - start_time
    end_message = f"""Finished `elt_cdc_profiles` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


if __name__ == "__main__":
    elt_cdc_profiles()