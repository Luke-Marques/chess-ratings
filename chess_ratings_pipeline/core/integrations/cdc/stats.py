from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
import polars as pl
from prefect import flow, task
from prefect.logging import get_run_logger

from chess_ratings_pipeline.core.integrations.cdc.api import ChessDotComAPI
from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from chess_ratings_pipeline.core.integrations.cdc.usernames import (
    fetch_titled_cdc_usernames,
)


@task(retries=3, log_prints=True)
def fetch_all_game_formats_stats(
    usernames: List[str], chess_title: ChessTitle
) -> List[Dict]:
    """
    Prefect task which fetches Chess.com player game statistics for each player of a given chess title.

    Args:
        usernames (List[str]): A list of usernames for the titled players.
        title_abbrv (Literal):
            The abbreviation of the chess title to filter the players by.

    Returns:
        List[Dict]:
            A list of dictionaries containing the game statistics for each player.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Retrieve player game statistics from Chess.com API
    logger.info(
        f"Fetching player all Chess.com game-format game statistics for each "
        f"{chess_title.value} titled player..."
    )
    all_game_formats_stats = []
    for index, username in enumerate(usernames[:10]):
        logger.info(
            f"Fetching game statistics for player {index+1:_} of {len(usernames):_} "
            f"({username})"
        )
        player_id: int = ChessDotComAPI().fetch_player_id(username)
        player_stats: Dict = ChessDotComAPI().fetch_player_stats(username)
        player_stats["player_id"] = player_id
        all_game_formats_stats.append(player_stats)
    logger.info(
        f"Finished fetching Chess.com game statistics for {len(usernames):_} "
        f"{chess_title.value} titled players."
    )

    return all_game_formats_stats


@task(log_prints=True)
def convert_json_stats_to_dataframes(
    all_game_formats_stats: List[Dict],
) -> pl.DataFrame:
    """
    Prefect task which converts a list of dictionaries containing JSON Chess.com player game statistics
    into a single Polars DataFrame, via Pandas (for it's ability to normalise JSON
    structures).

    Args:
        all_game_formats_stats (List[Dict]):
            A list of dictionaries containing JSON Chess.com player game statistics.

    Returns:
        pl.DataFrame:
            A Polars DataFrame containing the converted JSON Chess.com player game
            statistics.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Convert list of game stats dictionaries to Polars DataFrame
    logger.info(
        "Converting JSON-style Chess.com game statistics dictionaries for all "
        "game-formats to single Polars DataFrame..."
    )
    all_game_formats_stats: pl.DataFrame = pl.from_pandas(
        pd.json_normalize(all_game_formats_stats)
    )
    logger.info(f"All game-formats statistics DataFrame: {all_game_formats_stats}")

    return all_game_formats_stats


@task
def seperate_game_formats(
    all_game_formats_stats: pl.DataFrame,
) -> Dict[str, pl.DataFrame]:
    """
    Prefect task which separates the input DataFrame containing Chess.com player game statistics for all
    game-formats into per-game-format DataFrames and stores them in a dictionary.

    Args:
        all_game_formats_stats (pl.DataFrame):
            The input DataFrame containing Chess.com player game statistics for all
            game-formats.

    Returns:
        Dict[str, pl.DataFrame]:
            A dictionary where the keys are Chess.com game-formats and the values are
            the corresponding DataFrames which contain Chess.com players' game
            statistics for that game-format.
    """
    # Get list of game formats present in DataFrame
    game_formats: List[str] = set(
        [
            col.split(".")[0]
            for col in all_game_formats_stats.columns
            if col not in ["fide", "player_id"]
        ]
    )

    # Seperate DataFrame to per game-format DataFrames and store in dictionary
    stats = {}
    for game_format in game_formats:
        stats[game_format] = all_game_formats_stats.select(
            "player_id", pl.col(rf"^{game_format}.*$")
        )

    return stats


@task
def clean_cdc_stats(stats: pl.DataFrame, cdc_game_format: str) -> pl.DataFrame:
    """
    Prefect task which cleans the input DataFrame by renaming columns and converting date columns to the
    Polars date data type. Also adds a column containing the todays date to show the
    date the data was scraped.

    Args:
        stats (pl.DataFrame):
            The input DataFrame containing Chess.com players' game statistics.

    Returns:
        pl.DataFrame:
            The cleaned DataFrame with renamed columns and converted date columns.
    """
    # Create Prefect info logger
    logger = get_run_logger()
    logger.info(f"Cleaning Chess.com player {cdc_game_format} statistics DataFrame...")

    # Convert DataFrame to LazyFrame
    stats = stats.lazy()

    # Standardise column names
    stats = stats.rename(
        lambda col: col
        if len(col.split(".")) == 1
        else col.split(".", maxsplit=1)[1].replace(".", "_")
    )

    # Define schema of columns Polars data types for DataFrame, dependent on game format
    if cdc_game_format in ["tactics", "lessons"]:
        schema = {
            "highest_rating": pl.Int16,
            "highest_date": pl.Int64,
            "lowest_rating": pl.Int16,
            "lowest_date": pl.Int64,
        }
    elif cdc_game_format == "puzzle_rush":
        schema = {
            "daily_total_attempts": pl.Int16,
            "daily_score": pl.Int16,
            "best_total_attempts": pl.Int16,
            "best_score": pl.Int16,
        }
    else:
        schema = {
            "last_date": pl.Int64,
            "last_rating": pl.Int16,
            "last_rd": pl.Int16,
            "best_date": pl.Int64,
            "best_rating": pl.Int16,
            "best_game": pl.Utf8,
            "record_win": pl.Int16,
            "record_loss": pl.Int16,
            "record_draw": pl.Int16,
            "record_time_per_move": pl.Int16,
            "record_timeout_percent": pl.Float64,
            "tournament_count": pl.Int16,
            "tournament_withdraw": pl.Int16,
            "tournament_points": pl.Int16,
            "tournament_highest_finish": pl.Int16,
        }

    # Ensure all data fields for game format exist as columns (empty if not present)
    stats = stats.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias(col)
            for col in schema.keys()
            if col not in stats.columns
        ]
    )

    # Ensure columns have correct data types
    stats = stats.with_columns(
        [
            pl.from_epoch(pl.col(col)) if "date" in col else pl.col(col).cast(dtype)
            for col, dtype in schema.items()
        ]
    )

    # Add column containing todays date, to show date data was scraped
    stats = stats.with_columns(pl.lit(datetime.today()).alias("scrape_date"))

    # Remove columns with `tournament` in the name as they are not relevant
    stats = stats.drop([col for col in stats.columns if "tournament" in col.lower()])

    # Display cleaned DataFrame
    logger.info(f"Finished cleaning {cdc_game_format} statistics DataFrame.")
    logger.info(f"DataFrame: {stats}")
    logger.info(f"Schema: {stats.schema}")

    return stats.collect()


@flow(log_prints=True)
def extract_titled_cdc_stats(chess_title: ChessTitle) -> Dict[str, pl.DataFrame]:
    """
    Prefect sub-flow which retrieves Chess.com players' game statistics for titled
    players based on the given title abbreviation.

    Args:
        chess_title (ChessTitle):
            The title abbreviation to filter the players by.

    Returns:
        Dict[str, pl.DataFrame]:
            A dictionary containing the Chess.com players' game statistics for each
            game-format, where the keys are the game-format names and the values are the
            corresponding Polars DataFrames of game statistics.

    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Log flow start message
    start_time = datetime.now()
    start_message = f"""Starting `extract_titled_cdc_stats` sub-flow at {start_time} (local time).
    Inputs:
        chess_title (ChessTitle): {chess_title}"""
    logger.info(start_message)

    # Get usernames of titled players for title abbreviation
    logger.info(f"Fetching {chess_title.value} titled Chess.com players' usernames...")
    usernames: List[str] = fetch_titled_cdc_usernames(chess_title)
    logger.info(f"Retrieved {len(usernames):_} usernames.")

    # Get player statistics for each username
    logger.info(
        f"Fetching Chess.com player game statistics for each {chess_title.value} "
        "titled player..."
    )
    all_game_formats_stats: List[Dict] = fetch_all_game_formats_stats(
        usernames, chess_title
    )
    logger.info(f"Retrieved {len(all_game_formats_stats):_} player game statistics.")

    # Convert list of game stats dictionaries to Polars DataFrame
    logger.info("Converting JSON-style game statistics to Polars DataFrame...")
    all_game_formats_stats: pl.DataFrame = convert_json_stats_to_dataframes(
        all_game_formats_stats
    )
    logger.info(f"Converted game statistics DataFrame: {all_game_formats_stats}")

    # Seperate game format columns to seperate DataFrames
    logger.info("Seperating game-format statistics to individual DataFrames...")
    stats: Dict[str, pl.DataFrame] = seperate_game_formats(all_game_formats_stats)
    logger.info("Seperated game-format statistics DataFrames.")
    for game_format, stats_df in stats.items():
        logger.info(f"{game_format} statistics DataFrame: {stats_df}")

    # Log flow end message
    end_time = datetime.now()
    time_taken: timedelta = end_time - start_time
    end_message = f"""Finished `extract_titled_cdc_stats` sub-flow at {start_time} (local time).
        Time taken: {time_taken}"""
    logger.info(end_message)

    return stats


@task(log_prints=True)
def generate_cdc_stats_file_path(
    chess_title: ChessTitle,
    cdc_game_format: str,
    scrape_date: datetime = datetime.today(),
    extension: str = "parquet",
) -> Path:
    """
    Prefect task which generates a file path for Chess.com players' game statistics data.

    Args:
        chess_title (ChessTitle):
            The title abbreviation to filter the players by.
        cdc_game_format (str):
        scrape_date (datetime, optional):
            The date the data was scraped. Defaults to today's date.
        extension (str, optional): The file extension. Defaults to "parquet".

    Returns:
        Path: The file path for the player game stats file.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Generate file path
    logger.info(
        f"Generating file path for Chess.com player {cdc_game_format} statistics for titled players "
        f"of {chess_title.value}..."
    )
    file_name: Path = Path(
        f"{chess_title.name.lower()}_{cdc_game_format}_stats_"
        f"{str(scrape_date.date()).replace('-', '_')}.{extension}"
    )
    file_path = (
        Path("data")
        / "chess_dot_com"
        / "player_game_stats"
        / cdc_game_format
        / chess_title.name.lower()
        / file_name
    )

    return file_path
