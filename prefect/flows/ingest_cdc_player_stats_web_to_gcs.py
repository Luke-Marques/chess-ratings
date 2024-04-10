from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal

import pandas as pd
import polars as pl
from utils.chess_dot_com_api import ChessAPI
from utils.write_data import check_if_file_exists_in_gcs, write_to_gcs, write_to_local

from prefect import flow, task


@task(retries=3, log_prints=True)
def get_titled_usernames(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> List[str]:
    """
    Retrieves usernames of titled players from Chess.com API.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the chess title to filter the players by.

    Returns:
        List[str]: A list of usernames of titled players.

    """
    # Retrieve usernames from Chess.com API
    print(f"Fetching {title_abbrv} titled players' usernames...")
    usernames: List[str] = ChessAPI().get_titled_players_usernames(title_abbrv)[
        title_abbrv
    ]

    # Display username count
    print(f"Done. {len(usernames):_} usernames retrieved.")

    return usernames


@task(retries=3, log_prints=True)
def get_all_game_formats_stats(
    usernames: List[str],
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> List[Dict]:
    """
    Fetches Chess.com player game statistics for each player of a given chess title.

    Args:
        usernames (List[str]): A list of usernames for the titled players.
        title_abbrv (Literal):
            The abbreviation of the chess title to filter the players by.

    Returns:
        List[Dict]:
            A list of dictionaries containing the game statistics for each player.
    """
    print(f"Fetching player game statistics for each {title_abbrv} titled player...")
    all_game_formats_stats = []
    for index, username in enumerate(usernames):
        print(f"Player {index+1:_} of {len(usernames):_}.")
        player_id: int = ChessAPI().get_player_id(username)
        player_stats: Dict = ChessAPI().get_player_stats(username)
        player_stats["player_id"] = player_id
        all_game_formats_stats.append(player_stats)
    print("Done.")
    return all_game_formats_stats


@task
def convert_json_stats_to_dataframes(
    all_game_formats_stats: List[Dict],
) -> pl.DataFrame:
    """
    Converts a list of dictionaries containing JSON Chess.com player game statistics
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
    all_game_formats_stats: pl.DataFrame = pl.from_pandas(
        pd.json_normalize(all_game_formats_stats)
    )
    return all_game_formats_stats


@task
def seperate_game_formats(
    all_game_formats_stats: pl.DataFrame,
) -> Dict[str, pl.DataFrame]:
    """
    Separates the input DataFrame containing Chess.com player game statistics for all
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

    # Seperate DataFrame to per game format DataFrames and store in dictionary
    stats = {}
    for game_format in game_formats:
        stats[game_format] = all_game_formats_stats.select(
            "player_id", pl.col(rf"^{game_format}.*$")
        )

    return stats


@task
def clean_stats_dataframe(stats: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans the input DataFrame by renaming columns and converting date columns to the
    Polars date data type. Also adds a column containing the todays date to show the
    date the data was scraped.

    Args:
        stats (pl.DataFrame):
            The input DataFrame containing Chess.com players' game statistics.

    Returns:
        pl.DataFrame:
            The cleaned DataFrame with renamed columns and converted date columns.
    """
    stats = stats.rename(
        lambda col: col
        if len(col.split(".")) == 1
        else col.split(".", maxsplit=1)[1].replace(".", "_")
    ).with_columns(
        pl.from_epoch(pl.col(r"^.*date.*$")),  # convert all date columns date dtype
        pl.lit(datetime.today()).alias("scrape_date"),  # add date scraped as column
    )
    return stats


@flow
def get_titled_player_stats(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> Dict[str, pl.DataFrame]:
    """
    Prefect sub-flow which retrieves Chess.com players' game statistics for titled players based on the given
    title abbreviation.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the chess title for which to retrieve Chess.com players'
            game statistics.

    Returns:
        Dict[str, pl.DataFrame]:
            A dictionary containing the Chess.com players' game statistics for each
            game-format, where the keys are the game-format names and the values are the
            corresponding Polars DataFrames of game statistics.

    """
    # Get usernames of titled players for title abbreviation
    usernames: List[str] = get_titled_usernames(title_abbrv)

    # Get player statistics for each username
    all_game_formats_stats: List[Dict] = get_all_game_formats_stats(
        usernames, title_abbrv
    )

    # Convert list of game stats dictionaries to Polars DataFrame
    all_game_formats_stats: pl.DataFrame = convert_json_stats_to_dataframes(
        all_game_formats_stats
    )

    # Seperate game format columns to seperate DataFrames
    stats: Dict[str, pl.DataFrame] = seperate_game_formats(all_game_formats_stats)

    # Clean stats DataFrames
    for game_format, stats_df in stats.items():
        stats[game_format] = clean_stats_dataframe(stats_df)

    return stats


@task
def generate_file_path(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
    game_format: str,
    scrape_date: datetime = datetime.today(),
    extension: str = "parquet",
) -> Path:
    """
    Generate a file path for Chess.com players' game statistics data.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the player's title.
        game_format (str): The Chess.com game-format name.
        scrape_date (datetime, optional):
            The date the data was scraped. Defaults to today's date.
        extension (str, optional): The file extension. Defaults to "parquet".

    Returns:
        Path: The file path for the player game stats file.
    """

    # Generate filename
    file_name: Path = Path(
        f"{title_abbrv.lower()}_{game_format}_stats_"
        f"{str(scrape_date.date()).replace('-', '_')}.{extension}"
    )

    # Generate filepath
    file_path = (
        Path("data") / "chess_dot_com" / "player_game_stats" / game_format / file_name
    )

    return file_path


@flow(log_prints=True, cache_result_in_memory=False, persist_result=False)
def ingest_titled_players_stats(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> Dict[str, pl.DataFrame]:
    """
    Prefect sub-flow which ingests Chess.com players' game statistics for titled players
    from the Chess.com API, and cleans and writes the data, in a columnar format, to
    parquet files in a GCS bucket and/or locally.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the title of the players whose statistics will be
            ingested.
        gcs_bucket_block_name (str, optional):
            The name of the GCS bucket where the data will be written. Defaults to
            "chess-ratings-dev".
        write_local (bool, optional):
            Whether to write the data to a local file. Defaults to False.
        overwrite_existing (bool, optional):
            Whether to overwrite existing files in the GCS bucket. Defaults to True.

    Returns:
        Dict[str, pl.DataFrame]:
            A dictionary containing cleaned DataFrames of titled players' Chess.com game
            statistics. The keys of the dictionary are the game formats, and the values
            are the corresponding DataFrames containing Chess.com players' game
            statistics.
    """
    # Get cleaned DataFrames of titled players Chess.com game statistics
    stats: Dict[str, pl.DataFrame] = get_titled_player_stats(title_abbrv)

    for game_format, game_stats in stats.items():
        # Generate out file path
        out_file_path: Path = generate_file_path(title_abbrv, game_format)

        # Write to local file
        if write_local:
            print("Writing game statistics data to local file...")
            write_to_local(game_stats, out_file_path)
            print("Done.")

        # Write to file in GCS bucket
        if overwrite_existing or not check_if_file_exists_in_gcs(out_file_path):
            print("Writing game statistics data to GCS bucket...")
            write_to_gcs(game_stats, out_file_path, gcs_bucket_block_name)
            print("Done.")

    return stats


@flow(log_prints=True)
def ingest_cdc_player_stats_web_to_gcs(
    title_abbrvs: List[
        Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]
    ]
    | Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"] = [
        "GM",
        "WGM",
        "IM",
        "WIM",
        "FM",
        "WFM",
        "NM",
        "WNM",
        "CM",
        "WCM",
    ],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> None:
    """
    Ingests Chess.com players' game statistics for titled players across a range of
    chess titles from the Chess.com API, converts the data from JSON to Polars
    DataFrames, cleans the DataFrames, and writes the data in columnar format to parqeut
    files in a GCS bucket and/or locally.

    Args:
        title_abbrvs (List[Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]] or Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"], optional):
            A list of title abbreviations or a single title abbreviation to ingest
            Chess.com player game statistics for. Defaults to all Chess.com titles
            ["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"].
        gcs_bucket_block_name (str, optional):
            The name of the GCS bucket Prefect block. Defaults to "chess-ratings-dev".
        write_local (bool, optional):
            Whether to write the player statistics locally. Defaults to False.
        overwrite_existing (bool, optional):
            Whether to overwrite existing player statistics in the GCS bucket. Defaults
            to True.

    Returns:
        None
    """
    if not isinstance(title_abbrvs, list):
        title_abbrvs = [title_abbrvs]
    for title_abbrv in title_abbrvs:
        ingest_titled_players_stats(
            title_abbrv,
            gcs_bucket_block_name,
            write_local,
            overwrite_existing,
            return_state=True,
        )


if __name__ == "__main__":
    ingest_cdc_player_stats_web_to_gcs()
