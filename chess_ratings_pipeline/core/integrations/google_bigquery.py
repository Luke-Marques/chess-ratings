from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import polars as pl
from google.cloud.bigquery import (
    CreateDisposition,
    SchemaField,
    SourceFormat,
    WriteDisposition,
)
from prefect import flow, get_run_logger
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials


class BigQueryDataType(StrEnum):
    """
    Enum class representing the data types supported by BigQuery.
    """

    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    STRING = "STRING"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    RECORD = "RECORD"


def convert_polars_dtype_to_bigquery_field_type(
    polars_dtype: pl.DataType,
) -> BigQueryDataType | None:
    """
    Maps a Polars data type to the corresponding BigQuery data type.

    Args:
        polars_dtype (pl.DataType): The Polars data type to be mapped.

    Returns:
        BigQueryDataType | None: The corresponding BigQuery data type, or None if no mapping is found.
    """
    TYPE_MAPPING = {
        (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ): BigQueryDataType.INTEGER,
        (pl.Boolean, pl.Binary): BigQueryDataType.BOOLEAN,
        (pl.Float32, pl.Float64, pl.Decimal): BigQueryDataType.FLOAT,
        (pl.String, pl.Utf8, pl.Categorical, pl.Enum): BigQueryDataType.STRING,
        (pl.Date): BigQueryDataType.DATE,
        (pl.Time): BigQueryDataType.TIME,
        (pl.Datetime): BigQueryDataType.DATETIME,
    }
    for polars_dtypes, bq_dtype in TYPE_MAPPING.items():
        if isinstance(polars_dtypes, Iterable) and polars_dtype in polars_dtypes:
            return bq_dtype
        elif polars_dtype == polars_dtypes:
            return bq_dtype
    return None


def generate_bigquery_schema(df: pl.DataFrame) -> List[SchemaField]:
    """
    Generates a BigQuery schema from a Polars DataFrame.

    Args:
        df (pl.DataFrame): The Polars DataFrame to generate the schema from.

    Returns:
        List[SchemaField]: The BigQuery schema as a list of SchemaField objects.
    """
    schema = []
    for column, dtype in df.schema.items():
        val = df.select(column).row(0)[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"
        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_bigquery_schema(pl.from_pandas(pd.json_normalize(val)))
        else:
            fields = ()
        type = (
            "RECORD" if fields else convert_polars_dtype_to_bigquery_field_type(dtype)
        )
        schema.append(
            SchemaField(
                name=column,
                field_type=type,
                mode=mode,
                fields=fields,
            )
        )
    return schema


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


@flow(log_prints=True, retries=3, timeout_seconds=500)
def load_file_gcs_to_bq(
    gcs_file: Path,
    gcp_credentials_block: GcpCredentials,
    gcs_bucket_block: GcsBucket,
    dataset: str,
    table_name: str,
    location: Optional[str] = "europe-west1",
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

    # Get BQ schema for data in GCS file
    bq_schema: List[SchemaField] = generate_bigquery_schema(pl.read_parquet(gcs_file))

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
        schema=bq_schema,
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
    location: Optional[str] = "europe-west1",
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
