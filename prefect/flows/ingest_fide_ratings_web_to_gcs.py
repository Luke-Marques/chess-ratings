from datetime import date
from itertools import product
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd
import polars as pl
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


@task
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


def generate_file_path(year: int, month: int, game_format: GameFormat) -> Path:
    """Generate a file path for a month's FIDE chess ratings data, as a Path object."""
    # Generate file name
    file_name = Path(f"fide_chess_ratings_{year}_{month:02d}_{game_format.value}")

    # Generate file path
    file_path = Path("data") / "fide_ratings" / game_format / file_name

    return file_path


@task(log_prints=True, retries=3, cache_result_in_memory=False)
def parse_xml_url_to_dataframe(url: str) -> pl.DataFrame:
    """Read an ZIP compressed XML file to a Polars DataFrame from a URL, via Pandas."""
    df: pl.DataFrame = pl.from_pandas(pd.read_xml(url))
    return df


@flow(log_prints=True)
def extract_ratings_data(
    year: int, month: int, game_format: GameFormat
) -> pl.DataFrame:
    """
    Extract FIDE ratings data for given year, month, and game format, from
    compressed XML file to Polars DataFrame.
    """
    # create download url
    print(
        f"Generating download URL for year {year}, month {month}, and game format {game_format.value}..."
    )
    url: str = generate_fide_download_url(year, month, game_format)
    print(f"URL: {url}")

    # read zip compressed xml file from url to polars dataframe via pandas
    print("Reading compressed XML file to Polars DataFrame...")
    df: pl.DataFrame = parse_xml_url_to_dataframe(url)
    print("Done.")

    return df


@task
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


@task
def validate_ratings_data(df: pl.DataFrame) -> None:
    """
    Validate ratings records in FIDE chess ratings dataset using Patito data model.
    """
    ChessRating.validate(df)


@flow(log_prints=True, cache_result_in_memory=False, persist_result=False)
def ingest_single_month_fide_ratings_web_to_gcs(
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
    out_path: Path = generate_file_path(year, month, game_format)

    # check if ratings dataset file already exists in gcs
    file_exists_in_gcs: bool = check_if_file_exists_in_gcs(out_path)
    if file_exists_in_gcs and not overwrite_existing:
        print(f"Data file {out_path} exists in GCS already. Skipping.")
        return None

    # extract ratings dataset from web
    df = extract_ratings_data(year, month, game_format)
    print(
        f"Raw data extracted for year {year}, month {month}, game format {game_format}:"
    )
    print(df.head())

    # clean ratings dataset
    df = preprocess_ratings_data(df, year, month)
    print("Cleaned data:")
    print(df.head())

    # validate ratings dataset using patito data model
    print("Validating cleaned data...")
    validate_ratings_data(df)
    print("Done.")

    # write cleaned ratings dataset to local parquet file
    if store_local:
        write_to_local(df, out_path, return_state=True)

    # write cleaned ratings dataset to gcs bucket
    write_to_gcs(df, out_path, gcs_bucket_block_name, return_state=True)

    return df


@flow
def ingest_fide_ratings_web_to_gcs(
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
        ingest_single_month_fide_ratings_web_to_gcs(
            year,
            month,
            game_format,
            gcs_bucket_block_name,
            store_local,
            overwrite_existing,
            return_state=True,  # allows sub-flow to not store result
        )


if __name__ == "__main__":
    ingest_fide_ratings_web_to_gcs()
