from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Literal, Optional

import polars as pl
from prefect import flow
from prefect.logging import get_run_logger
from prefect.runtime import flow_run
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from google.cloud import bigquery

from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.stats import (
    clean_cdc_stats,
    extract_titled_cdc_stats,
    generate_cdc_stats_file_path,
)
from chess_ratings_pipeline.core.integrations.google_bigquery import (
    create_external_bq_table,
)
from chess_ratings_pipeline.core.integrations.google_cloud_storage import (
    write_dataframe_to_gcs,
    write_dataframe_to_local,
)
from chess_ratings_pipeline.core.integrations.dbt.dbt_cloud import run_dbt_job


def generate_extract_single_title_cdc_stats_flow_name() -> str:
    """
    Generates the name of the `elt_single_title_cdc_stats` flow based on the parameters provided
    to the flow.

    Returns:
        str: The name of the `elt_single_title_cdc_stats` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_title: ChessTitle = parameters["chess_title"]
    name = f"{flow_name}-{chess_title.name.lower()}"
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


@flow(flow_run_name=generate_extract_single_title_cdc_stats_flow_name, log_prints=True)
def extract_single_title_cdc_stats(
    chess_title: ChessTitle,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
) -> None:
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
        overwrite_existing (bool): {overwrite_existing}"""
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

    # Write player game statistics to parquet files in GCS bucket and/or locally
    for index, (cdc_game_format, stats_df) in enumerate(cdc_stats.items()):
        # Generate destination parquet file path for writing of player game statistics
        logger.info(f"Dataset {index} of {len(cdc_stats)}.")
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
        write_dataframe_to_gcs(
            stats_df, destination, gcs_bucket_block, overwrite_existing
        )
        logger.info("Finished writing game statistics to GCS bucket.")
        if store_local:
            logger.info(
                f"Writing cleaned {chess_title.value} titled Chess.com players' "
                f"{cdc_game_format} game statistics to {destination} locally..."
            )
            write_dataframe_to_local(stats_df, destination, overwrite_existing)
            logger.info("Finished writing game statistics locally.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `elt_single_title_cdc_stats` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


@flow(log_prints=True)
def load_cdc_stats_to_bq_external_table(
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    project: str = "fide-chess-ratings",
    bq_dataset_name: str = "chess_ratings",
    bq_table_name_prefix: str = "landing_cdc",
) -> str:
    # Create Prefect logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `load_cdc_profiles_to_bq_external_table` flow at {start_time} (local time).
    Inputs:
        gcp_credentials_block (GcpCredentials): {gcp_credentials_block}
        gcs_bucket_block (GcsBucket): {gcs_bucket_block}
        project (str): {project}
        bq_dataset_name (str): {bq_dataset_name}
        bq_table_name_prefix (str): {bq_table_name_prefix}"""
    logger.info(start_message)

    # Get list of directories in GCS bucket containing Chess.com stats Parquet files
    dirs: List[str] = gcs_bucket_block.list_folders(
        str(
            generate_cdc_stats_file_path(
                ChessTitle.GM, "chess_daily"
            ).parent.parent.parent
        )
    )

    # Define URI patterns for FIDE ratings Parquet files in GCS bucket
    source_uris: List[str] = [
        f"gs://{gcs_bucket_block.bucket}/{dir}/*.parquet" for dir in dirs
    ]

    # Create external BigQuery tables from list of URIs
    bq_schemas = {
        "chess": [
            bigquery.SchemaField("scrape_datetime", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("game_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("time_control", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("last_date", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("last_rating", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("last_rd", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("best_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("best_rating", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("best_game", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("record_win", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("record_loss", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("record_draw", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("record_time_per_move", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("record_timeout_percent", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("tournament_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("tournament_withdraw", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("tournament_points", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField(
                "tournament_highest_finish", "FLOAT64", mode="NULLABLE"
            ),
        ],
        "tactics": [
            bigquery.SchemaField("scrape_datetime", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("highest_rating", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("highest_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("lowest_rating", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("lowest_date", "DATETIME", mode="NULLABLE"),
        ],
        "lessons": [
            bigquery.SchemaField("scrape_datetime", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("highest_rating", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("highest_date", "DATETIME", mode="NULLABLE"),
            bigquery.SchemaField("lowest_rating", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("lowest_date", "DATETIME", mode="NULLABLE"),
        ],
        "puzzle_rush": [
            bigquery.SchemaField("scrape_datetime", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("daily_total_attempts", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("daily_score", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("best_total_attempts", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("best_score", "FLOAT64", mode="NULLABLE"),
        ],
    }
    for game_type, bq_schema in bq_schemas.items():
        source_uris: List[str] = [
            f"gs://{gcs_bucket_block.bucket}/{dir}/*.parquet"
            for dir in dirs
            if game_type in dir.split("/")[-2]
        ]
        table_name = f"{bq_table_name_prefix}__{game_type}_stats"
        if source_uris:
            create_external_bq_table(
                source_uris=source_uris,
                dataset=bq_dataset_name,
                table=table_name,
                project=project,
                schema=bq_schema,
                gcp_credentials=gcp_credentials_block,
                return_state=True,
            )

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `load_cdc_profiles_to_bq_external_table` flow at {end_time} (local time).
        Time taken: {time_taken}."""
    logger.info(end_message)


@flow(flow_run_name=generate_elt_cdc_stats_flow_name, log_prints=True)
def elt_cdc_stats(
    chess_titles: Optional[List[ChessTitle] | ChessTitle] | Literal["all"] = "all",
    gcp_credentials_block_name: Optional[str] = "gcp-creds-chess-ratings",
    gcs_bucket_block_name: Optional[str] = "chess-ratings-dev",
    store_local: Optional[bool] = False,
    overwrite_existing: Optional[bool] = True,
    bq_dataset_name: Optional[str] = "chess_ratings",
    bq_table_name_prefix: Optional[str] = "landing_cdc",
    dbt_job_id: int = 638701,
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

    # Extract Chess.com player game statistics for each ChessTitle to GCS bucket
    logger.info(
        "Running Chess.com player game statistics ELT sub-flow for each chess title "
        "specified..."
    )
    for chess_title in chess_titles:
        logger.info(
            f"Running Chess.com player game statistics ELT sub-flow for Chess.com "
            f"players with {chess_title.value} titles..."
        )
        extract_single_title_cdc_stats(
            chess_title,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
            return_state=True,
        )
        logger.info(
            f"Completed Chess.com player game statistics ELT sub-flow for Chess.com "
            f"players with {chess_title.value} titles."
        )

    # Load all Chess.com game statistics Parquet files from GCS bucket to BigQuery
    # external table
    logger.info(
        "Loading all Chess.com game statistics Parquet files from GCS bucket to "
        "BigQuery external table..."
    )
    load_cdc_stats_to_bq_external_table(
        gcp_credentials_block=gcp_credentials_block,
        gcs_bucket_block=gcs_bucket_block,
        bq_dataset_name=bq_dataset_name,
        bq_table_name_prefix=bq_table_name_prefix,
        return_state=True,
    )
    logger.info(
        "Completed loading all Chess.com game statistics Parquet files from GCS bucket "
        "to BigQuery external table."
    )

    # Run dbt models via dbt Cloud job
    logger.info("Running dbt models via dbt Cloud job...")
    run_dbt_job(dbt_job_id)
    logger.info("Finished running dbt models via dbt Cloud job.")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `elt_cdc_stats` flow at {end_time} (local time).
    Time taken: {time_taken}."""
    logger.info(end_message)


if __name__ == "__main__":
    elt_cdc_stats()
