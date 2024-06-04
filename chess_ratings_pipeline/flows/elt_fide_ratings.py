from datetime import datetime
from itertools import product
from pathlib import Path
from typing import List, Tuple

import polars as pl
from google.cloud import bigquery
from prefect import flow
from prefect.logging import get_run_logger
from prefect.runtime import flow_run
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from chess_ratings_pipeline.core.integrations.dbt.dbt_core import trigger_dbt_flow
from chess_ratings_pipeline.core.integrations.fide import (
    FideGameFormat,
    check_valid_month,
    check_valid_year,
    clean_fide_ratings,
    extract_fide_ratings,
    generate_file_path,
)
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


def generate_extract_single_fide_ratings_dataset_flow_name() -> str:
    """
    Generates a flow name for the extract_single_fide_ratings_dataset flow based on the
    parameters provided to the flow.

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
    """
    Extracts a single FIDE ratings dataset for a given year and month, applies initial
    cleaning/preprocessing, and writes cleaned dataset to Parquet file in a GCS bucket
    and/or locally.

    Args:
        year (int): The year of the ratings dataset.
        month (int): The month of the ratings dataset.
        fide_game_format (FideGameFormat): The FIDE game format.
        gcp_credentials_block (GcpCredentials): The Prefect GCP credentials block.
        gcs_bucket_block (GcsBucket): The Prefect GCS bucket block.
        store_local (bool): Flag indicating whether to store the dataset locally.
        overwrite_existing (bool): Flag indicating whether to overwrite existing dataset.

    Returns:
        None
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "extract_single_fide_ratings_dataset",
        start_time,
        year,
        month,
        fide_game_format,
        gcp_credentials_block,
        gcs_bucket_block,
        store_local,
        overwrite_existing,
    )
    logger.info(start_message)

    # Validate year and month flow parameters
    check_valid_year(year)
    check_valid_month(month)

    # Extract FIDE ratings dataset from web and convert from compressed XML to DataFrame
    fide_ratings: pl.DataFrame = extract_fide_ratings(
        year, month, fide_game_format.value
    )

    # Apply initial cleaning/pre-processing to FIDE ratings DataFrame
    fide_ratings_clean: pl.DataFrame = clean_fide_ratings(
        fide_ratings, year, month, fide_game_format
    )

    # Write FIDE ratings DataFrame to a parquet file in a GCS bucket and/or locally
    destination: Path = generate_file_path(year, month, fide_game_format)
    write_dataframe_to_gcs(
        fide_ratings_clean, destination, gcs_bucket_block, overwrite_existing
    )
    if store_local:
        write_dataframe_to_local(fide_ratings_clean, destination, overwrite_existing)

    # Log flow end message
    end_time = datetime.now()
    end_message: str = generate_flow_end_log_message(
        "extract_single_fide_ratings_dataset", end_time, start_time
    )
    logger.info(end_message)


@flow(log_prints=True)
def load_fide_ratings_to_bq_external_table(
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    project: str = "fide-chess-ratings",
    bq_dataset_name: str = "chess_ratings",
    bq_table_name: str = "landing_fide__ratings",
) -> str:
    """
    Loads FIDE ratings data from Parquet files in a GCS bucket into an external BigQuery
    table.

    Args:
        gcp_credentials_block (GcpCredentials): The GCP credentials block.
        gcs_bucket_block (GcsBucket): The GCS bucket block.
        project (str, optional): The GCP project name. Defaults to "fide-chess-ratings".
        bq_dataset_name (str, optional): The BigQuery dataset name. Defaults to "chess_ratings".
        bq_table_name (str, optional): The BigQuery table name. Defaults to "landing_fide__ratings".

    Returns:
        str: The state of the external BigQuery table creation.
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "load_fide_ratings_to_bq_external_table",
        start_time,
        gcp_credentials_block,
        gcs_bucket_block,
        project,
        bq_dataset_name,
        bq_table_name,
    )
    logger.info(start_message)

    # Get list of parent directories containing FIDE ratings Parquet files in GCS bucket
    example_year, example_month = 2000, 1
    example_fide_game_format = FideGameFormat.STANDARD
    dirs: List[str] = gcs_bucket_block.list_folders(
        str(
            generate_file_path(
                example_year, example_month, example_fide_game_format
            ).parent.parent
        )
    )

    # Define URI patterns for FIDE ratings Parquet files in GCS bucket
    source_uris: List[str] = [
        f"gs://{gcs_bucket_block.bucket}/{dir}/*.parquet" for dir in dirs
    ]

    # Define BigQuery table schema
    bq_schema = [
        bigquery.SchemaField("fide_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("game_format", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("player_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fide_federation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sex", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("w_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("o_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rating", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("game_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("k", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("birth_year", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("foa_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("period_year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("period_month", "INTEGER", mode="REQUIRED"),
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
        "load_fide_ratings_to_bq_external_table", end_time, start_time
    )
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
    Prefect flow which extracts FIDE ratings data for specified years, months, and game 
    formats, applies initial cleaning/preprocessing, writes the cleaned datasets to 
    Parquet files in a GCS bucket and/or locally, loads the data into external BigQuery
    landing tables, and runs all downstream dbt models in the data warehouse.

    Args:
        years (List[int] | int): The years for which to extract FIDE ratings data.
            Defaults to the current year.
        months (List[int] | int): The months for which to extract FIDE ratings data.
            Defaults to the current month.
        fide_game_format (str): The FIDE game format(s) to extract ratings for.
            Valid options are "all", "standard", "rapid", and "blitz".
            Defaults to "all".
        gcp_credentials_block_name (str): The name of the GCP credentials block to use.
            Defaults to "gcp-creds-chess-ratings".
        gcs_bucket_block_name (str): The name of the GCS bucket block to use.
            Defaults to "chess-ratings-dev".
        store_local (bool): Whether to store the extracted data locally.
            Defaults to False.
        overwrite_existing (bool): Whether to overwrite existing data.
            Defaults to True.
        bq_dataset_name (str): The name of the BigQuery dataset to load the data into.
            Defaults to "chess_ratings".
        bq_table_name (str): The name of the BigQuery table to load the data into.
            Defaults to "landing_fide__ratings".

    Returns:
        None
    """
    # Log flow start message
    logger = get_run_logger()
    start_time = datetime.now()
    start_message: str = generate_flow_start_log_message(
        "elt_fide_ratings",
        start_time,
        years,
        months,
        fide_game_format,
        gcp_credentials_block_name,
        gcs_bucket_block_name,
        store_local,
        overwrite_existing,
        bq_dataset_name,
        bq_table_name,
    )
    logger.info(start_message)

    # Convert int year/month values to lists
    if isinstance(years, int):
        years: List[int] = [years]
    if isinstance(months, int):
        months: List[int] = [months]

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

    # Load GCP and GCS blocks
    gcp_credentials_block: GcpCredentials = GcpCredentials.load(
        gcp_credentials_block_name
    )
    gcs_bucket_block: GcsBucket = GcsBucket.load(gcs_bucket_block_name)

    # Extract FIDE ratings data for each year/month and game format to GCS
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
    load_fide_ratings_to_bq_external_table(
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
        "elt_fide_ratings", end_time, start_time
    )
    logger.info(end_message)


if __name__ == "__main__":
    elt_fide_ratings()
