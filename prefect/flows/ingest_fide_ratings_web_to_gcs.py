import io
import zipfile
from datetime import date
from itertools import product
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import polars as pl
import requests
from prefect_gcp.cloud_storage import GcsBucket
from utils.chess_ratings_data_model import ChessRating
from utils.dates import (
    check_valid_month,
    check_valid_year,
    convert_numeric_month_to_string,
)
from utils.game_format import GameFormat
from utils.write_data import check_if_file_exists_in_gcs, write_to_gcs, write_to_local

from prefect import flow, task


def add_missing_columns(
    df: pl.DataFrame | pl.LazyFrame, required_columns: Iterable[str]
) -> pl.DataFrame | pl.LazyFrame:
    """Creates empty required columns if they don't already exist in the dataframe."""
    df = df.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias(col)
            for col in required_columns
            if col not in df.columns
        ]
    )
    return df


def generate_fide_download_url(year: int, month: int, game_format: GameFormat) -> str:
    """
    Generate a download url for FIDE chess ratings data for a given year, month, and
    game format.
    """
    check_valid_month(month)
    check_valid_year(year)
    month: str = convert_numeric_month_to_string(month)
    url = (
        "http://ratings.fide.com/download/"
        f"{game_format.value}_{month}{str(year)[-2:]}frl_xml.zip"
    )
    return url


def generate_file_name(year: int, month: int, game_format: GameFormat) -> Path:
    """Generate a file name for a month's FIDE chess ratings data, as a Path object."""
    return Path(f"fide_chess_ratings_{year}_{month}_{game_format.value}")


@task()
def parse_xml_file(xml_file: str | Path | bytes) -> pl.DataFrame:
    """Parse an XML format data file to a Polars DataFrame, via Pandas."""
    df: pl.DataFrame = pl.from_pandas(pd.read_xml(xml_file))
    return df


@task(retries=3)
def stream_zip_file(url: str) -> Tuple[zipfile.ZipFile, str]:
    """Stream a FIDE chess ratings compressed file, without downloading it locally."""
    response = requests.get(url)
    byte_data = io.BytesIO(response.content)
    zip_file = zipfile.ZipFile(byte_data)
    xml_file_name = zip_file.namelist()[0]
    return zip_file, xml_file_name


@flow(log_prints=True)
def extract_ratings_data(
    year: int, month: int, game_format: GameFormat
) -> pl.DataFrame:
    """
    Extract FIDE ratings data for given year, month, and game format, from
    compressed XML file to Polars DataFrame.
    """
    # create download url
    url = generate_fide_download_url(year, month, game_format)

    # stream xml file
    zip_file, xml_file_name = stream_zip_file(url)

    # read xml to polars dataframe (using pandas as intermediary)
    with zip_file.open(xml_file_name) as f:
        df = parse_xml_file(f)

    return df


@task()
def preprocess_ratings_data(df: pl.DataFrame, year: int, month: int) -> pl.DataFrame:
    """Initial preprocessing of raw FIDE chess ratings dataset."""
    # add missing columns to dataframe
    required_columns = ["foa_title"]
    df: pl.DataFrame = add_missing_columns(df, required_columns)

    df: pl.LazyFrame = (
        df.lazy()
        .rename(
            {
                "fideid": "fide_id",
                "name": "player_name",
                "country": "fide_federation",
                "games": "game_count",
                "birthday": "birth_year",
            }
        )
        .with_columns(
            # convert birth year column to date
            pl.col("birth_year").replace(0, None).cast(pl.Datetime).dt.year(),
            # convert sex indicator column to binary
            pl.col("sex").replace({"F": 0, "M": 1}).cast(pl.Int8),
            # add ratings period column
            pl.lit(year).alias("period_year"),
            pl.lit(month).alias("period_month"),
        )
    )

    return df.collect()


@task()
def validate_ratings_data(df: pl.DataFrame) -> None:
    """
    Validate ratings records in FIDE chess ratings dataset using Patito data model.
    """
    ChessRating.validate(df)


@flow()
def ingest_single_month_web_to_gcs(
    year: int,
    month: int,
    game_format: GameFormat,
    gcs_bucket_block_name: str = "chess-ratings-dev",
    store_local: bool = False,
    overwrite_existing: bool = False,
) -> Tuple[pl.DataFrame, Path] | None:
    """
    Sub-flow for the extraction, pre-processing, and writing of FIDE chess ratings
    data to GCS, for a given of date (year and month) and game format.
    """
    # generate file path for cleaned ratings dataset
    out_file_name: Path = generate_file_name(year, month, game_format)
    out_path = Path("data" / out_file_name)

    # check if ratings dataset file already exists in gcs
    file_exists_in_gcs: bool = check_if_file_exists_in_gcs(out_path)
    if file_exists_in_gcs and not overwrite_existing:
        print(f"Data file {out_path} exists in GCS already. Skipping.")
        return None

    # extract ratings dataset from web
    df = extract_ratings_data(year, month, game_format)

    # clean ratings dataset
    df_clean = preprocess_ratings_data(df, year, month)

    # validate ratings dataset using patito data model
    validate_ratings_data(df_clean)

    # write cleaned ratings dataset to local parquet file
    if store_local:
        write_to_local(df_clean, out_path)

    # write cleaned ratings dataset to gcs bucket
    write_to_gcs(df, out_path, gcs_bucket_block_name)

    return df, out_path


@flow()
def ingest_web_to_gcs(
    year: int | Iterable[int] = date.today().year,
    month: int | Iterable[int] = date.today().month,
    game_format: str | Iterable[str] = "all",
    gcs_bucket_block_name: str = "chess-ratings-dev",
    store_local: bool = False,
    overwrite_existing: bool = True,
) -> None:
    """
    Parent-flow for the extraction, pre-processing, and writing of FIDE chess ratings
    data to GCS, across a range of dates (years and months) and game formats.
    """
    # convert int year/month values to lists
    if isinstance(year, int):
        year = [year]
    if isinstance(month, int):
        month = [month]

    # convert game_format variable to GameFormat object(s)
    match game_format:
        case "all":
            game_format = [GameFormat.STANDARD, GameFormat.RAPID, GameFormat.BLITZ]
        case "standard":
            game_format = [GameFormat.STANDARD]
        case "rapid":
            game_format = [GameFormat.RAPID]
        case "blitz":
            game_format = [GameFormat.BLITZ]

    for game_format, year, month in product(game_format, year, month):
        ingest_single_month_web_to_gcs(
            year,
            month,
            game_format,
            gcs_bucket_block_name,
            store_local,
            overwrite_existing,
        )


if __name__ == "__main__":
    ingest_web_to_gcs()
