from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Literal, Optional

import polars as pl
from prefect import flow
from prefect.logging import get_run_logger
from prefect.runtime import flow_run
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.stats import (
    clean_cdc_stats,
    extract_titled_cdc_stats,
    generate_cdc_stats_file_path,
)
from chess_ratings_pipeline.core.integrations.google_bigquery import load_file_gcs_to_bq
from chess_ratings_pipeline.core.integrations.google_cloud_storage import (
    write_dataframe_to_gcs,
    write_dataframe_to_local,
)


def generate_load_single_cdc_game_format_stats_flow_name() -> str:
    """
    Generates the name of the `load_single_cdc_game_format_stats` flow based on the parameters provided
    to the flow.

    Returns:
        str: The name of the `load_single_cdc_game_format_stats` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_title: ChessTitle = parameters["chess_title"]
    cdc_game_format: str = parameters["cdc_game_format"]
    name = f"{flow_name}-{chess_title.value}-{cdc_game_format}"
    return name


def generate_elt_single_title_cdc_stats_flow_name() -> str:
    """
    Generates the name of the `elt_single_title_cdc_stats` flow based on the parameters provided
    to the flow.

    Returns:
        str: The name of the `elt_single_title_cdc_stats` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_title: ChessTitle = parameters["chess_title"]
    name = f"{flow_name}-{chess_title.value}"
    return name


def generate_elt_cdc_stats_flow_name() -> str:
    """
    Generates the name of the `elt_cdc_stats` flow based on the parameters provided
    to the flow.

    Returns:
        str: The name of the `elt_cdc_stats` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_titles: List[ChessTitle] | ChessTitle | Literal["all"] = parameters[
        "chess_titles"
    ]
    if chess_titles == "all" or chess_titles == list(ChessTitle):
        name = f"{flow_name}-all-titles"
    elif isinstance(chess_titles, list):
        name = f"{flow_name}-{'-'.join([title.name.lower() for title in chess_titles])}"
    elif isinstance(chess_titles, ChessTitle):
        name = f"{flow_name}-{chess_titles.name.lower()}"
    return name


@flow(
    flow_run_name=generate_load_single_cdc_game_format_stats_flow_name, log_prints=True
)
def load_single_cdc_game_format_stats(
    chess_title: ChessTitle,
    cdc_game_format: str,
    stats_df: pl.DataFrame,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
    bq_dataset_name: str,
    bq_table_name_prefix: str,
) -> None:
    """
    Writes Chess.com player game statistics DataFrame to parquet file in GCS bucket
    and/or locally, and then loads the data file from GCS to BigQuery.

    Args:
        chess_title (ChessTitle):
            The title of the chess game.
        cdc_game_format (str):
            The Chess.com game format.
        stats_df (pl.DataFrame):
            The DataFrame containing the statistics.
        gcp_credentials_block (GcpCredentials):
            The GCP credentials block.
        gcs_bucket_block (GcsBucket):
            The GCS bucket block.
        store_local (bool):
            Flag indicating whether to store the statistics locally.
        overwrite_existing (bool):
            Flag indicating whether to overwrite existing statistics.
        bq_dataset_name (str):
            The name of the BigQuery dataset.
        bq_table_name_prefix (str):
            The prefix for the BigQuery table name.

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `load_single_cdc_game_format_stats` flow at {start_time} (local time).
    Inputs:
        chess_title (ChessTitle): {chess_title}
        cdc_game_format (str): {cdc_game_format}
        stats_df (pl.DataFrame): \n\t{stats_df}
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name_prefix (str): {bq_table_name_prefix}"""
    logger.info(start_message)

    # Generate destination parquet file path for writing of player game statistics
    logger.info(
        f"Generating destination parquet file path for {chess_title.value} titled "
        f"Chess.com players' {cdc_game_format} game statistics..."
    )
    destination: Path = generate_cdc_stats_file_path(chess_title, cdc_game_format)
    logger.info(f"File path: {destination}")

    # Write player game statistics to parquet file in GCS bucket and/or locally
    logger.info(
        f"Writing cleaned {chess_title.value} titled Chess.com players' "
        f"{cdc_game_format} game statistics to GCS bucket at {destination}..."
    )
    write_dataframe_to_gcs(stats_df, destination, gcs_bucket_block, overwrite_existing)
    logger.info("Finished writing game statistics to GCS bucket.")
    if store_local:
        logger.info(
            f"Writing cleaned {chess_title.value} titled Chess.com players' "
            f"{cdc_game_format} game statistics to {destination} locally..."
        )
        write_dataframe_to_local(stats_df, destination, overwrite_existing)
        logger.info("Finished writing game statistics locally.")

    # Load player game statistics data from GCS bucket to BigQuery
    bq_table_name = f"{bq_table_name_prefix}_{cdc_game_format}"
    logger.info(
        f"Loading cleaned Chess.com {chess_title.value} titled player "
        f"{cdc_game_format} statistics data to BigQuery data warehouse "
        f"{bq_dataset_name}/{bq_table_name}..."
    )
    load_file_gcs_to_bq(
        gcs_file=destination,
        gcp_credentials_block=gcp_credentials_block,
        gcs_bucket_block=gcs_bucket_block,
        dataset=bq_dataset_name,
        table_name=bq_table_name,
    )
    logger.info("Finished loading player game statistics data to BigQuery.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `load_single_cdc_game_format_stats` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


@flow(flow_run_name=generate_elt_single_title_cdc_stats_flow_name, log_prints=True)
def elt_single_title_cdc_stats(
    chess_title: ChessTitle,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
    bq_dataset_name: str,
    bq_table_name_prefix: str,
) -> None:
    """
    Extracts Chess.com player game statistics for the specified ChessTitle,
    applies cleaning/pre-processing, and loads the data into BigQuery.

    Args:
        chess_title (ChessTitle):
            The title of the chess players to extract statistics for.
        gcp_credentials_block (GcpCredentials):
            The GCP credentials block for authentication.
        gcs_bucket_block (GcsBucket):
            The GCS bucket block for storing intermediate data.
        store_local (bool):
            Flag indicating whether to store intermediate data locally.
        overwrite_existing (bool):
            Flag indicating whether to overwrite existing data.
        bq_dataset_name (str):
            The name of the BigQuery dataset to load the data into.
        bq_table_name_prefix (str):
            The prefix for the BigQuery table names.

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `elt_single_title_cdc_stats` flow at {start_time} (local time).
    Inputs:
        chess_title (ChessTitle): {chess_title}
        gcp_credentials_block: {gcp_credentials_block}
        gcs_bucket_block: {gcs_bucket_block}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name_prefix (str): {bq_table_name_prefix}"""
    logger.info(start_message)

    # Extract Chess.com player game statistics for the specified ChessTitle
    logger.info(
        "Extracting Chess.com player game statistics for "
        f"{chess_title.value} titled players on "
        f"{datetime.now().date} at {datetime.now().time}..."
    )
    cdc_stats: Dict[str, pl.DataFrame] = extract_titled_cdc_stats(chess_title)
    logger.info(
        "Extracted Chess.com player game statistics for the following Chess.com "
        f"game-formats: {cdc_stats.keys()}"
    )
    logger.info(
        "Extracted game statistics for "
        f"{max([len(stats_df) for stats_df in cdc_stats.values()]):_} players."
    )
    logger.info("Chess.com game statistics DataFrame(s):")
    for cdc_game_format, stats_df in cdc_stats.items():
        logger.info(f"{cdc_game_format}: {stats_df}")

    # Apply initial cleaning/pre-processing to player game statistics DataFrames
    logger.info(
        f"Cleaning Chess.com {chess_title.value} titled player game statistics "
        "DataFrames..."
    )
    for cdc_game_format, stats_df in cdc_stats.items():
        cdc_stats[cdc_game_format] = clean_cdc_stats(stats_df, cdc_game_format)
    logger.info("Cleaned Chess.com game statistics DataFrames.")
    for cdc_game_format, stats_df in cdc_stats.items():
        logger.info(
            f"{cdc_game_format} statistics DataFrame"
            f"\n\tShape: {stats_df.shape} "
            f"\n\tColumns: {stats_df.columns}"
        )

    # Load player game statistics data to GCS bucket and BigQuery
    for index, (cdc_game_format, stats_df) in enumerate(cdc_stats.items()):
        logger.info(
            f"Running LOAD sub-flow for {chess_title.value} titled Chess.com players' {cdc_game_format} statistics ({index+1:_} of {len(cdc_stats):_} game formats)..."
        )
        load_single_cdc_game_format_stats(
            chess_title,
            cdc_game_format,
            stats_df,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
            bq_dataset_name,
            bq_table_name_prefix,
        )
        logger.info(
            f"Completed LOAD sub-flow for {chess_title.value} titled Chess.com "
            f"players' {cdc_game_format} statistics."
        )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `elt_single_title_cdc_stats` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


@flow(flow_run_name=generate_elt_cdc_stats_flow_name, log_prints=True)
def elt_cdc_stats(
    chess_titles: Optional[List[ChessTitle] | ChessTitle] | Literal["all"] = "all",
    gcp_credentials_block_name: Optional[str] = "gcp-creds-chess-ratings",
    gcs_bucket_block_name: Optional[str] = "chess-ratings-dev",
    store_local: Optional[bool] = False,
    overwrite_existing: Optional[bool] = True,
    bq_dataset_name: Optional[str] = "landing",
    bq_table_name_prefix: Optional[str] = "cdc_stats",
) -> None:
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `elt_cdc_stats` flow at {start_time} (local time).
    Inputs:
        chess_titles (ChessTitle | List[ChessTitle] | Literal["all"]): {chess_titles}
        gcp_credentials_block_name (str): {gcp_credentials_block_name}
        gcs_bucket_block_name (str): {gcs_bucket_block_name}
        store_local (bool): {store_local}
        overwrite_existing (bool): {overwrite_existing}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name_prefix (str): {bq_table_name_prefix}"""
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

    # ELT Chess.com player game statistics for each ChessTitle
    logger.info(
        "Running Chess.com player game statistics ELT sub-flow for each chess title "
        "specified..."
    )
    for chess_title in chess_titles:
        logger.info(
            f"Running Chess.com player game statistics ELT sub-flow for Chess.com "
            f"players with {chess_title.value} titles..."
        )
        elt_single_title_cdc_stats(
            chess_title,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
            bq_dataset_name,
            bq_table_name_prefix,
        )
        logger.info(
            f"Completed Chess.com player game statistics ELT sub-flow for Chess.com "
            f"players with {chess_title.value} titles."
        )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `elt_cdc_stats` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


if __name__ == "__main__":
    elt_cdc_stats()
