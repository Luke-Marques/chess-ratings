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
    """
    Adds missing columns to a Polars DataFrame or LazyFrame.

    Args:
        df (pl.DataFrame | pl.LazyFrame): The input Polars DataFrame or LazyFrame.
        required_columns (Iterable[str]): The list of required column names.

    Returns:
        pl.DataFrame | pl.LazyFrame: 
            The Polars DataFrame or LazyFrame with missing columns added.
    """
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
    Generate the download URL for FIDE chesss ratings based on the given year, month, 
    and FIDE game-format.

    Args:
        year (int): The year of the ratings.
        month (int): The month of the ratings.
        game_format (GameFormat): The format of the game (e.g., standard, rapid, blitz).

    Returns:
        str: The download URL for the FIDE ratings.

    Raises:
        ValueError: If the month or year is invalid.
    """
    check_valid_month(month)
    check_valid_year(year)
    month: str = convert_numeric_month_to_string(month)
    url = (
        "http://ratings.fide.com/download/"
        f"{game_format.value}_{month}{str(year)[-2:]}frl_xml.zip"
    )
    return url


@task
def generate_file_path(year: int, month: int, game_format: GameFormat) -> Path:
    """
    Generate a file path for FIDE chess ratings based on the given year, month, and FIDE
    game-format.

    Args:
        year (int): The year of the ratings.
        month (int): The month of the ratings.
        game_format (GameFormat): The FIDE game-format.

    Returns:
        Path: The file path for the FIDE chess ratings.
    """
    # Generate file name
    file_name = Path(f"fide_chess_ratings_{year}_{month:02d}_{game_format.value}")

    # Generate file path
    file_path = Path("data") / "fide_ratings" / game_format / file_name

    return file_path


@task(log_prints=True, retries=3, cache_result_in_memory=False)
def parse_xml_url_to_dataframe(url: str) -> pl.DataFrame:
    """
    Parses an XML file from the given URL, via Pandas, and returns a Polars DataFrame.

    Args:
        url (str): The URL of the XML file.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the parsed data from the XML file.
    """
    df: pl.DataFrame = pl.from_pandas(pd.read_xml(url))
    return df


@flow(log_prints=True)
def extract_ratings_data(
    year: int, month: int, game_format: GameFormat
) -> pl.DataFrame:
    """
    Prefect sub-flow extracts FIDE chess ratings data for a given year, month, and FIDE 
    game-format.

    Args:
        year (int): The year of the ratings data.
        month (int): The month of the ratings data.
        game_format (GameFormat): The FIDE game-format of the ratings data.

    Returns:
        pl.DataFrame: The extracted FIDE chess ratings data as a Polars DataFrame.
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
def clean_ratings_data(df: pl.DataFrame, year: int, month: int) -> pl.DataFrame:
    """
    Clean a Polars DataFrame containing FIDE chess ratings data by adding missing 
    columns, renaming columns, converting data types, and adding columns corresponding 
    to the year/month that the ratings relate to.

    Args:
        df (pl.DataFrame): The input Polars DataFrame containing the ratings data.
        year (int): The year of the ratings period.
        month (int): The month of the ratings period.

    Returns:
        pl.DataFrame: The cleaned Polars DataFrame of FIDE chess ratings.

    """
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
    Validates the FIDE chess ratings data in the given Polars DataFrame using Patito 
    data model.

    Args:
        df (pl.DataFrame): The Polars DataFrame containing the FIDE chess ratings data.

    Returns:
        None

    Raises:
        patito.exceptions.DataFrameValidationError: 
            If the given Polars DataFrame does not match the Patito data model/schema.
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
    Prefect sub-flow which ingests FIDE chess ratings data for a given year and month 
    from the web, converts the downloaded XML file to a Polars DataFrame, cleans the 
    DataFrame, and writes the cleaned DataFrame as a parquet file to a GCS bucket and/or 
    locally.

    Args:
        year (int): The year of the ratings data.
        month (int): The month of the ratings data.
        game_format (GameFormat): 
            The format of the chess games as a GameFormat object (e.g., 
            GameFormat.STANDARD, GameFormat.RAPID, GameFormat.BLITZ).
        gcs_bucket_block_name (str, optional): 
            The name of the GCS bucket Prefect block. Defaults to "chess-ratings-dev".
        store_local (bool, optional): 
            Whether to store the cleaned ratings dataset locally. Defaults to False.
        overwrite_existing (bool, optional): 
            Whether to overwrite the existing ratings dataset in GCS if it already 
            exists. Defaults to False.

    Returns:
        Tuple[pl.DataFrame, Path] | None: A tuple containing the cleaned ratings dataset 
        as a Polars DataFrame and the file path where it is stored in GCS/locally. 
        Returns None if the ratings dataset already exists in GCS and overwrite_existing 
        is False.
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
    df = clean_ratings_data(df, year, month)
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
    Prefect parent-flow which ingests FIDE chess ratings data from the web across a 
    range of years/months/game-format combinations, converts the downloaded XML files to 
    Polars DataFrames, cleans the DataFrames, and writes the cleaned DataFrames to 
    parquet files in a GCS bucket and/or locally.

    Args:
        year (int | Iterable[int]): 
            The year(s) for which to ingest the ratings. Defaults to the current year.
        month (int | Iterable[int]): 
            The month(s) for which to ingest the ratings. Defaults to the current month.
        game_format (str | Iterable[str]): 
            The FIDE game-format(s) for which to ingest the ratings. Defaults to "all", 
            representing the standard, rapid, and blitz game-formats.
        gcs_bucket_block_name (str): 
            The name of the GCS bucket Prefect block where the ratings data will be 
            written to.
        store_local (bool): 
            Whether to store the ratings locally in addition to GCS. Defaults to False.
        overwrite_existing (bool): 
            Whether to overwrite existing ratings in GCS. Defaults to True.

    Returns:
        None
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
