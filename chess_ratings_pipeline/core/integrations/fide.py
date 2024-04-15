from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Optional

import pandas as pd
import patito as pt
import polars as pl
from prefect import flow, task
from prefect.logging import get_run_logger
from pydantic import validator


class FideGameFormat(StrEnum):
    """
    Enumeration class representing different FIDE chess game-formats.

    Attributes:
        STANDARD (str): Represents the standard FIDE chess game format.
        RAPID (str): Represents the rapid FIDE chess game format.
        BLITZ (str): Represents the blitz FIDE chess game format.
    """

    STANDARD = "standard"
    RAPID = "rapid"
    BLITZ = "blitz"


class ChessRating(pt.Model):
    """
    Patito data model/schema which represents a FIDE chess rating entry.

    Attributes:
        fide_id (int): The FIDE ID of the player.
        player_name (str, optional): The name of the player.
        fide_federation (str, optional): The FIDE federation of the player.
        sex (int, optional): The sex of the player.
        title (str, optional): The title of the player.
        w_title (str, optional): The women's title of the player.
        o_title (str, optional): The open title of the player.
        foa_title (str, optional): The FIDE Online Arena title of the player.
        rating (int): The rating of the player.
        game_count (int): The number of games played by the player.
        k (int): The K-factor used for rating calculation.
        birth_year (int, optional): The birth year of the player.
        flag (str, optional): The flag of the player.
        period_year (int): The year of the rating period.
        period_month (int): The month of the rating period.

    Methods:
        within_date_range(cls, value):
            Validates that the birth year is within a valid range.

    """

    fide_id: int = pt.Field(unique=True)
    player_name: Optional[str]
    fide_federation: Optional[str] = pt.Field(pattern=r"(?i)[A-Z]{3}")
    sex: Optional[str]
    title: Optional[str]
    w_title: Optional[str]
    o_title: Optional[str]
    foa_title: Optional[str]
    rating: int
    game_count: int
    k: int
    birth_year: Optional[int] = None
    flag: Optional[str]
    period_year: int = pt.Field(ge=2000, le=date.today().year)
    period_month: int = pt.Field(ge=1, le=12)

    @validator("birth_year")
    def within_date_range(cls, value):
        assert 1900 <= value <= date.today().year
        return value


def convert_numeric_month_to_string(month: int) -> str:
    """
    Converts a numeric month to its corresponding 3-character string representation.

    Args:
        month (int): The numeric representation of the month (1-12).

    Returns:
        str: The string representation of the month.

    Raises:
        ValueError: If month integer value is not valid.
    """
    months = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    ]
    return months[month - 1]


@task(log_prints=True)
def check_valid_year(year: int) -> None:
    """
    Checks if the given year is valid for the purposes of FIDE chess ratings data
    extraction.

    Args:
        year (int): The year to be checked.

    Raises:
        ValueError: If the year is not a valid integer year value between 2015 and the
        current year inclusive.

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Validate that year value is an integer between 2015 and current year
    MIN_YEAR, MAX_YEAR = 2015, date.today().year
    logger.info(
        f"Validating that year value {year} is a valid integer between "
        f"{MIN_YEAR} and {MAX_YEAR}..."
    )
    error_message = (
        f"Year value is not valid ({year = }). "
        "Please enter an integer year value between "
        f"{MIN_YEAR} and {MAX_YEAR}, inclusive."
    )
    if (not isinstance(year, int)) or (year < MIN_YEAR) or (year > MAX_YEAR):
        raise ValueError(error_message)
    logger.info(f"Year value {year} is valid.")


@task(log_prints=True)
def check_valid_month(month: int) -> None:
    """
    Check if the given month value is valid.

    Args:
        month (int): The month value to be checked.

    Raises:
        ValueError: If the month value is not a valid integer between 1 and 12 inclusive.

    Returns:
        None
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Validate that month value is an integer between 1 and 12
    MIN_MONTH, MAX_MONTH = 1, 12
    logger.info(
        f"Validating that month value {month} is a valid integer between "
        f"{MIN_MONTH} and {MAX_MONTH}, inclusive..."
    )
    error_message = (
        "Month value is not valid. Please enter an integer month value between "
        f"{MIN_MONTH} and {MAX_MONTH}, inclusive. You entered {month = }."
    )
    if (not isinstance(month, int)) or (month < MIN_MONTH) or (month > MAX_MONTH):
        raise ValueError(error_message)
    logger.info(f"Month value {month} is valid.")


@task(log_prints=True)
def generate_fide_download_url(
    year: int, month: int, game_format: FideGameFormat
) -> str:
    """
    Generate the download URL for FIDE chesss ratings based on the given year, month,
    and FIDE game-format.

    Args:
        year (int): The year of the ratings.
        month (int): The month of the ratings.
        game_format (FideGameFormat): The format of the game (e.g., standard, rapid, blitz).

    Returns:
        str: The download URL for the FIDE ratings.

    Raises:
        ValueError: If the month or year is invalid.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Convert month integer value to 3-character string representation
    logger.info(
        f"Converting month integer value {month} to "
        "3-character string representation..."
    )
    month: str = convert_numeric_month_to_string(month)
    logger.info(f"Month string representation: {month = }.")

    # Generate FIDE chess ratings download URL
    logger.info(
        f"Generating download URL for year {year}, month {month}, and game format "
        f"{game_format.value}..."
    )
    url = (
        "http://ratings.fide.com/download/"
        f"{game_format.value}_{month}{str(year)[-2:]}frl_xml.zip"
    )
    logger.info(f"URL: {url}")

    return url


@task(log_prints=True)
def generate_file_path(
    year: int, month: int, game_format: FideGameFormat, extension: str = "parquet"
) -> Path:
    """
    Generate a file path for FIDE chess ratings based on the given year, month, and FIDE
    game-format.

    Args:
        year (int): The year of the ratings.
        month (int): The month of the ratings.
        game_format (FideGameFormat): The FIDE game-format.
        extension (str, optional): The file extension. Defaults to "parquet".

    Returns:
        Path: The file path for the FIDE chess ratings.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Generate file path
    logger.info(
        f"Generating file path for year {year}, month {month}, and game format "
        f"{game_format.value}..."
    )
    file_name = Path(
        f"fide_chess_ratings_{year}_{month:02d}_{game_format.value}.{extension}"
    )
    file_path = Path("data") / "fide_ratings" / game_format / file_name
    logger.info(f"File path: {file_path}")

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
    # Create Prefect info logger
    logger = get_run_logger()

    # Read XML file from URL to Polars DataFrame via Pandas
    logger.info(f"Reading compressed XML file at {url} to Polars DataFrame...")
    df: pl.DataFrame = pl.from_pandas(pd.read_xml(url))
    logger.info(f"DataFrame: \n{df}")

    return df


@flow(log_prints=True, cache_result_in_memory=False)
def extract_fide_ratings(
    year: int, month: int, game_format: FideGameFormat
) -> pl.DataFrame:
    """
    Prefect sub-flow extracts FIDE chess ratings data for a given year, month, and FIDE
    game-format.

    Args:
        year (int): The year of the ratings data.
        month (int): The month of the ratings data.
        game_format (FideGameFormat): The FIDE game-format of the ratings data.

    Returns:
        pl.DataFrame: The extracted FIDE chess ratings data as a Polars DataFrame.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `extract_fide_ratings` flow at {start_time} (local time).
    Inputs:
        year (int): {year}
        month (int): {month}
        game_format (FideGameFormat): {game_format}"""
    logger.info(start_message)

    # Create Download URL
    logger.info(
        f"Generating download URL for year {year}, month {month}, and "
        f"game-format {game_format.value}..."
    )
    url: str = generate_fide_download_url(year, month, game_format)
    logger.info(f"URL: {url}")

    # Read zip compressed XML file from URL to Polars DataFrame via Pandas
    logger.info(f"Reading compressed XML file at {url} to Polars DataFrame...")
    df: pl.DataFrame = parse_xml_url_to_dataframe(url)
    logger.info(f"DataFrame: \n{df}")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `extract_fide_ratings` flow at {end_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)

    return df


@task(log_prints=True, cache_result_in_memory=False)
def clean_fide_ratings(ratings: pl.DataFrame, year: int, month: int) -> pl.DataFrame:
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
    # Create Prefect info logger
    logger = get_run_logger()
    logger.info(f"Cleaning FIDE ratings DataFrame for {year}-{month}...")

    # Define schema of Polars data types for FIDE ratings DataFrame
    schema = {
        "fideid": pl.Int64,
        "name": pl.Utf8,
        "country": pl.Utf8,
        "sex": pl.Utf8,
        "title": pl.Utf8,
        "w_title": pl.Utf8,
        "o_title": pl.Utf8,
        "rating": pl.Int16,
        "games": pl.Int16,
        "k": pl.Int16,
        "birthday": pl.Int16,
        "flag": pl.Utf8,
        "foa_title": pl.Utf8,
    }

    # Convert ratings DataFrame to LazyFrame
    ratings = ratings.lazy()

    # Ensure all columns are of the correct data type
    ratings = ratings.with_columns(
        [
            pl.col(col).cast(dtype)
            for col, dtype in schema.items()
            if col in ratings.columns
        ]
    )

    # Rename columns
    ratings = ratings.rename(
        {
            "fideid": "fide_id",
            "name": "player_name",
            "country": "fide_federation",
            "games": "game_count",
            "birthday": "birth_year",
        }
    )

    # Convert birth year column to date
    ratings = ratings.with_columns(
        pl.col("birth_year").replace(0, None).cast(pl.Datetime).dt.year()
    )

    # Add columns for ratings period (year and month)
    ratings = ratings.with_columns(
        [
            pl.lit(year).alias("period_year"),
            pl.lit(month).alias("period_month"),
        ]
    )

    # Drop duplicate rows and gather DataFrame
    ratings = ratings.unique().collect()

    # Display cleaned DataFrame
    logger.info("Finished cleaning FIDE ratings DataFrame.")
    logger.info(f"DataFrame: {ratings}")
    logger.info(f"Schema: {ratings.schema}")

    return ratings


@task(log_prints=True, cache_result_in_memory=False)
def validate_fide_ratings(df: pl.DataFrame) -> None:
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
    # Create Prefect info logger
    logger = get_run_logger()

    # Validate FIDE ratings DataFrame using Patito data-model/schema
    logger.info("Validating FIDE ratings DataFrame using Patito data-model/schema...")
    ChessRating.validate(df)
    logger.info("FIDE ratings DataFrame is valid.")
