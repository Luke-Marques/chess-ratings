from datetime import datetime
from pathlib import Path
from typing import Dict, List

import polars as pl
import requests
from prefect import flow, task
from prefect.logging import get_run_logger

from chess_ratings_pipeline.core.integrations.cdc.api import ChessDotComAPI
from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.usernames import (
    fetch_titled_cdc_usernames,
)


@task(retries=3, log_prints=True)
def fetch_cdc_profiles(usernames: List[str]) -> dict:
    """
    Fetches Chess.com player profiles for given usernames using Chess.com API.

    Args:
        usernames (List[str]):
            A list of Chess.com usernames for which player profiles need to be fetched.

    Returns:
        Dict: A dictionary containing player profiles, where the keys are the usernames
        and the values are the profiles.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Retrieve player profiles from Chess.com API
    logger.info("Fetching Chess.com player profiles for each username specified...")
    profiles = []
    for index, username in enumerate(usernames):
        logger.info(f"Player {index+1:_} of {len(usernames):_} ({username})")
        try:
            profiles.append(ChessDotComAPI().fetch_player_profile(username))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"HttpError 404 for {username = }.")
            else:
                raise
    return profiles


@task(log_prints=True)
def convert_cdc_profiles_from_dictionaries_to_dataframe(
    profiles: List[dict],
) -> pl.DataFrame:
    """
    Convert a list of Chess.com player profiles dictionaries to a single Polars
    DataFrame.

    Args:
        profiles (List[Dict]): A list of player profiles, where each profile is a
        dictionary.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the converted player profiles.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Convert player profiles from dictionaries to Polars DataFrame
    logger.info("Converting player profiles from dictionaries to Polars DataFrame...")
    profiles: pl.DataFrame = pl.DataFrame(profiles)
    logger.info(f"Chess.com player profiles DataFrame: \n\t{profiles}")

    return profiles


@task(log_prints=True)
def clean_cdc_profiles(profiles: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans a Polars DataFrame containing Chess.com player profiles by performing the
    following operations:
    - Renames specific columns
    - Adds a column with the current date and time
    - Removes duplicate rows
    - Collects the cleaned dataframe

    Args:
        profiles (pl.DataFrame): The input DataFrame of Chess.com player profiles.

    Returns:
        pl.DataFrame: The cleaned DataFrame of Chess.com player profiles.
    """
    # Create Prefect info logger
    logger = get_run_logger()
    logger.info("Cleaning Chess.com player profiles DataFrame...")

    # Convert DataFrame to LazyFrame
    profiles = profiles.lazy()

    # Define schema of columns Polars data types for DataFrame
    schema = {
        "avatar_url": pl.Utf8,
        "api_url": pl.Utf8,
        "profile_url": pl.Utf8,
        "username": pl.Utf8,
        "player_id": pl.Int64,
        "title": pl.Utf8,
        "status": pl.Utf8,
        "name": pl.Utf8,
        "location": pl.Utf8,
        "country": pl.Utf8,
        "joined": pl.Int64,
        "last_online": pl.Int64,
        "followers": pl.Int64,
        "is_streamer": pl.Boolean,
        "twitch_url": pl.Utf8,
        "fide": pl.Int16,
    }

    # Rename columns
    profiles = profiles.rename(
        {
            "avatar": "avatar_url",
            "@id": "api_url",
            "url": "profile_url",
        }
    )

    # Convert columns to data types specified in schema
    profiles = profiles.with_columns(
        [
            pl.from_epoch(col)
            if col in ["joined", "last_online"]
            else pl.col(col).cast(dtype)
            for col, dtype in schema.items()
            if col in profiles.columns
        ]
    )

    # Add column of todays date/time
    profiles = profiles.with_columns(pl.lit(datetime.now()).alias("scrape_datetime"))

    # Select only those columns which appear in schema, and scrape_datetime
    profiles = profiles.select(
        [col for col in schema.keys() if col in profiles.columns], "scrape_datetime"
    )

    # Drop duplicate rows and gather DataFrame
    profiles = profiles.unique().collect()

    # Display cleaned DataFrame and Schema
    logger.info("Finished cleaning Chess.com player profiles DataFrame.")
    logger.info(f"DataFrame: {profiles}")
    logger.info(f"Schema: {profiles.schema}")

    return profiles


@flow(log_prints=True)
def extract_titled_cdc_profiles(chess_title: ChessTitle) -> pl.DataFrame:
    """
    Retrieve Chess.com player profile details of titled players of a given title.

    Args:
        chess_title (ChessTitle):
            The title of the titled players to retrieve profile details for.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the profile details of titled
        players.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Get list of titled players usernames
    logger.info(f"Fetching {chess_title.value} titled Chess.com players' usernames...")
    usernames: List[str] = fetch_titled_cdc_usernames(chess_title)
    logger.info(f"Retrieved {len(usernames):_} usernames.")

    # Get profile details for each titled player
    logger.info(
        f"Fetching Chess.com player profiles for each {chess_title.value} titled "
        "Chess.com player..."
    )
    profiles: List[Dict] = fetch_cdc_profiles(usernames)
    logger.info(f"Retrieved {len(profiles):_} player profiles.")

    # Convert list of profile dictionaries to Polars DataFrame
    logger.info("Converting player profiles from dictionaries to Polars DataFrame...")
    profiles: pl.DataFrame = convert_cdc_profiles_from_dictionaries_to_dataframe(
        profiles
    )
    logger.info(
        f"{chess_title.value} titled Chess.com player profiles DataFrame: "
        f"\n\t{profiles}"
    )

    return profiles


@task
def generate_cdc_profiles_file_path(
    chess_title: ChessTitle,
    scrape_date: datetime = datetime.today(),
    extension: str = "parquet",
) -> Path:
    """
    Generate a full file path for the storage of player profiles data locally or in GCS.

    Args:
        chess_title (ChessTitle):
            The title of the titled players for which to generate the file path.
        scrape_date (datetime, optional):
            The date of the scrape. Defaults to today's date.
        extension (str, optional):
            The file extension. Defaults to "parquet".

    Returns:
        Path: The full file path for storing the player profiles data.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Generate file path
    logger.info(
        f"Generating destination parquet file path for writing of {chess_title.value} "
        "titled Chess.com player profiles DataFrame..."
    )
    file_name = Path(
        f"{chess_title.name.lower()}_player_profiles_"
        f"{str(scrape_date.date()).replace('-', '_')}.{extension}"
    )
    file_path = (
        Path("data")
        / "chess_dot_com"
        / "player_profiles"
        / chess_title.name.lower()
        / file_name
    )
    logger.info(f"Generated file path: {file_path}")

    return file_path
