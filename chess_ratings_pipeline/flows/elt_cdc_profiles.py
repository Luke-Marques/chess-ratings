from datetime import datetime
from typing import List, Literal

import polars as pl
from google.cloud import bigquery
from prefect import flow
from prefect.logging import get_run_logger
from prefect.runtime import flow_run
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.profiles import (
    clean_cdc_profiles,
    extract_titled_cdc_profiles,
    generate_cdc_profiles_file_path,
)
from chess_ratings_pipeline.core.integrations.dbt.dbt_core import trigger_dbt_flow
from chess_ratings_pipeline.core.integrations.google_bigquery import (
    create_external_bq_table,
)
from chess_ratings_pipeline.core.integrations.google_cloud_storage import (
    write_dataframe_to_gcs,
    write_dataframe_to_local,
)
from chess_ratings_pipeline.core.logging_messages.logging_messages import (
    generate_flow_end_log_message,
    generate_flow_start_log_message,
)


def generate_extract_single_title_cdc_profiles_flow_name() -> str:
    """
    Generates the name of the `elt_single_title_cdc_profiles` flow based on the
    parameters provided to the flow.

    Returns:
        str: The name of the `elt_single_title_cdc_profiles` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_title: ChessTitle = parameters["chess_title"]
    return f"{flow_name}-{chess_title.name.lower()}"


def generate_elt_cdc_profiles_flow_name() -> str:
    """
    Generates the name of the `elt_cdc_profiles` flow based on the parameters provided
    to the flow.

    Returns:
        str: The name of the `elt_cdc_profiles` flow.
    """
    flow_name = flow_run.flow_name
    parameters = flow_run.parameters
    chess_titles: ChessTitle | List[ChessTitle] | Literal["all"] = parameters[
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
    flow_run_name=generate_extract_single_title_cdc_profiles_flow_name, log_prints=True
)
def extract_single_title_cdc_profiles(
    chess_title: ChessTitle,
    gcs_bucket_block: GcsBucket,
    store_local: bool,
    overwrite_existing: bool,
) -> None:
    """
    Extracts Chess.com player profiles for a specified ChessTitle, performs
    initial cleaning/preprocessing, and writes cleaned dataset to Parquet file in a GCS
    bucket and/or locally.

    Args:
        chess_title (ChessTitle): The ChessTitle for which player profiles need to be extracted.
        gcs_bucket_block (GcsBucket): The GCS bucket where the player profiles will be stored.
        store_local (bool): Flag indicating whether to store the player profiles locally.
        overwrite_existing (bool): Flag indicating whether to overwrite existing player profiles.

    Returns:
        None
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "elt_single_title_cdc_profiles",
        start_time,
        chess_title,
        gcs_bucket_block,
        store_local,
        overwrite_existing,
    )
    logger.info(start_message)

    # Extract Chess.com player profiles for the specified ChessTitle
    cdc_profiles: pl.DataFrame = extract_titled_cdc_profiles(chess_title)

    # Apply initial cleaning/pre-processing to player profiles DataFrame
    cdc_profiles_clean: pl.DataFrame = clean_cdc_profiles(cdc_profiles)

    # Write player profiles DataFrame to parquet file in GCS bucket and/or locally
    destination: str = generate_cdc_profiles_file_path(chess_title)
    write_dataframe_to_gcs(
        cdc_profiles_clean, destination, gcs_bucket_block, overwrite_existing
    )
    if store_local:
        write_dataframe_to_local(cdc_profiles_clean, destination, overwrite_existing)

    # Log flow end message
    end_time = datetime.now()
    end_message: str = generate_flow_end_log_message(
        "extract_single_title_cdc_profiles", start_time, end_time
    )
    logger.info(end_message)


@flow(log_prints=True)
def load_cdc_profiles_to_bq_external_table(
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    project: str = "fide-chess-ratings",
    bq_dataset_name: str = "chess_ratings",
    bq_table_name: str = "landing_cdc__profiles",
) -> str:
    """
    Loads Chess.com player profiles data from Parquet files in a GCS bucket into an
    external BigQuery table.

    Args:
        gcp_credentials_block (GcpCredentials): The GCP credentials block for authentication.
        gcs_bucket_block (GcsBucket): The GCS bucket block for accessing the GCS bucket.
        project (str, optional): The GCP project ID. Defaults to "fide-chess-ratings".
        bq_dataset_name (str, optional): The BigQuery dataset name. Defaults to "chess_ratings".
        bq_table_name (str, optional): The BigQuery table name. Defaults to "landing_cdc__profiles".

    Returns:
        str: The state of the external BigQuery table creation process.
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "load_cdc_profiles_to_bq_external_table",
        start_time,
        gcp_credentials_block,
        gcs_bucket_block,
        project,
        bq_dataset_name,
        bq_table_name,
    )
    logger.info(start_message)

    # Get list of parent directories containing Chess.com player profiles Parquet files
    # in GCS bucket
    dirs: List[str] = gcs_bucket_block.list_folders(
        str(generate_cdc_profiles_file_path(ChessTitle.GM).parent.parent)
    )

    # Define URI patterns for FIDE ratings Parquet files in GCS bucket
    source_uris: List[str] = [
        f"gs://{gcs_bucket_block.bucket}/{dir}/*.parquet" for dir in dirs
    ]

    # Define BigQuery table schema
    bq_schema = [
        bigquery.SchemaField("avatar_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("api_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("profile_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("joined", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("last_online", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("is_streamer", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("fide", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("scrape_datetime", "DATETIME", mode="NULLABLE"),
    ]

    # Create external BigQuery table from list of URIs if table does not already exist
    create_external_bq_table(
        source_uris=source_uris,
        dataset=bq_dataset_name,
        table=bq_table_name,
        project=project,
        schema=bq_schema,
        gcp_credentials=gcp_credentials_block,
        return_state=True,
    )

    # Log flow end message
    end_time = datetime.now()
    end_message: str = generate_flow_end_log_message(
        "load_cdc_profiles_to_bq_external_table", start_time, end_time
    )
    logger.info(end_message)


@flow(flow_run_name=generate_elt_cdc_profiles_flow_name, log_prints=True)
def elt_cdc_profiles(
    chess_titles: ChessTitle | List[ChessTitle] | Literal["all"] = "all",
    gcp_credentials_block_name: str = "gcp-creds-chess-ratings",
    gcs_bucket_block_name: str = "chess-ratings-dev",
    store_local: bool = False,
    overwrite_existing: bool = True,
    bq_dataset_name: str = "chess_ratings",
    bq_table_name: str = "landing_cdc__profiles",
) -> None:
    """
    Extracts Chess.com player profiles for the specified ChessTitles, performs initial
    cleaning/preprocessing, writes cleaned datasets to Parquet files in a GCS bucket
    and/or locally, loads the Parquet files in the GCS bucket to an external BigQuery
    table, and runs all dbt models.

    Args:
        chess_titles (ChessTitle | List[ChessTitle] | Literal["all"], optional): The ChessTitles for which player profiles need to be extracted. Defaults to "all".
        gcp_credentials_block_name (str, optional): The name of the GCP credentials block. Defaults to "gcp-creds-chess-ratings".
        gcs_bucket_block_name (str, optional): The name of the GCS bucket block. Defaults to "chess-ratings-dev".
        store_local (bool, optional): Flag indicating whether to store the player profiles locally. Defaults to False.
        overwrite_existing (bool, optional): Flag indicating whether to overwrite existing player profiles. Defaults to True.
        bq_dataset_name (str, optional): The BigQuery dataset name. Defaults to "chess_ratings".
        bq_table_name (str, optional): The BigQuery table name. Defaults to "landing_cdc__profiles".

    Returns:
        None
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "elt_cdc_profiles",
        start_time,
        chess_titles,
        gcp_credentials_block_name,
        gcs_bucket_block_name,
        store_local,
        overwrite_existing,
        bq_dataset_name,
        bq_table_name,
    )
    logger.info(start_message)

    if chess_titles == "all":
        chess_titles = list(ChessTitle)
    elif not isinstance(chess_titles, list):
        chess_titles = [chess_titles]

    # Load GCP and GCS Prefect blocks
    gcp_credentials_block: GcpCredentials = GcpCredentials.load(
        gcp_credentials_block_name
    )
    gcs_bucket_block: GcsBucket = GcsBucket.load(gcs_bucket_block_name)

    # Extract Chess.com player profiles for each ChessTitle to GCS bucket
    for chess_title in chess_titles:
        extract_single_title_cdc_profiles(
            chess_title,
            gcp_credentials_block,
            gcs_bucket_block,
            store_local,
            overwrite_existing,
            return_state=True,
        )

    # Load Chess.com player profiles to BigQuery external table
    load_cdc_profiles_to_bq_external_table(
        gcp_credentials_block=gcp_credentials_block,
        gcs_bucket_block=gcs_bucket_block,
        bq_dataset_name=bq_dataset_name,
        bq_table_name=bq_table_name,
        return_state=True,
    )

    # Run dbt models via dbt Cloud job
    _: List[str] = trigger_dbt_flow(commands=["pwd", "dbt debug", "dbt build"])

    # Log flow end message
    end_time = datetime.now()
    end_message: str = generate_flow_end_log_message(
        "elt_cdc_profiles", start_time, end_time
    )
    logger.info(end_message)


if __name__ == "__main__":
    elt_cdc_profiles()
