from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal

import pandas as pd
import polars as pl
from utils.chess_dot_com_api import (
    check_title_abbrv,
    get_titled_players_usernames,
    request_from_chess_dot_com_public_api,
)
from utils.write_data import check_if_file_exists_in_gcs, write_to_gcs, write_to_local

from prefect import flow, task


@task(retries=3)
def get_player_id(username: str) -> int:
    """
    Function which uses the public Chess.com API to retrieve the non-changing, Chess.com
    player ID for a given player username.
    """
    # Define the API endpoint suffix
    api_endpoint_suffix = f"player/{username}"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    # Extract player id
    player_id: int = response["player_id"]

    return player_id


@flow(retries=3)
def get_player_stats(username: str) -> Dict[str, pl.DataFrame]:
    """
    Function which uses the public Chess.com API to return the game statistics of a
    given player.
    """
    # Get player id
    player_id: int = get_player_id(username)

    # Get player game stats
    game_stats_api_endpoint_suffix = f"player/{username}/stats"
    game_stats: Dict = request_from_chess_dot_com_public_api(
        game_stats_api_endpoint_suffix
    )

    # Convert each game format's stats to Polars DataFrame and store in dictionary
    game_stats_dfs = {}
    for game_format in game_stats:
        if game_format != "fide":
            game_stats_dfs[game_format] = pl.from_pandas(
                pd.json_normalize(game_stats[game_format], sep="_")
            ).with_columns(pl.lit(player_id).alias("player_id"))

    return game_stats_dfs


@task
def concatenate_dataframes(
    dict_list: List[Dict[str, pl.DataFrame]],
) -> Dict[str, pl.DataFrame]:
    """
    Given a list of dictionaries containing string keys and Polars DataFrame values,
    concatenate all DataFrames belonging to the same key.
    """
    grouped_dataframes = defaultdict(list)
    for d in dict_list:
        for key, df in d.items():
            grouped_dataframes[key].append(df)
    result = {
        key: pl.concat(dfs, how="diagonal_relaxed")
        for key, dfs in grouped_dataframes.items()
    }
    return result


@task
def clean_game_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Clean Chess.com game statistics DataFrames."""
    # Convert datetime columns to proper datatype
    df = df.with_columns(pl.from_epoch(pl.col(r"^*date*$")))
    return df


@flow
def get_titled_player_stats(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> Dict[str, pl.DataFrame]:
    """
    Function which uses the public Chess.com API to return the game statistics of all
    titled players of a given title.
    """
    # Get usernames of titled players for title abbreviation
    usernames: List[str] = get_titled_players_usernames(title_abbrv)

    # Get player statistics for each username
    game_stats_dfs: List[Dict[str, pl.DataFrame]] = [
        get_player_stats(username) for username in usernames
    ]

    # Concatenate game stats dataframes
    stats = concatenate_dataframes(game_stats_dfs)

    # Clean game stats dataframes
    for game_format, game_stats in stats.items():
        stats[game_format] = clean_game_stats(game_stats)

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
    Generate a full file path for the storage of player game statistics data locally or
    in GCS.
    """
    # Generate filename
    file_name: Path = Path(
        f"{title_abbrv.lower()}_titled_player_{game_format}_stats_"
        f"{str(scrape_date.date()).replace("-", "_")}.{extension}"
    )

    # Generate filepath
    file_path = (
        Path("data")
        / "chess_dot_com"
        / "player_game_stats"
        / game_format
        / title_abbrv.lower()
        / file_name
    )

    return file_path


@flow
def ingest_titled_players_stats(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> pl.DataFrame:
    """
    Sub-flow that retrieves titled player game statistics using the public Chess.com API
    and writes these profiles to files in GCS bucket and optionally locally.
    """
    # Check that title abbreviation is valid
    check_title_abbrv(title_abbrv)

    # Get cleaned DataFrame of titled players Chess.com game statistics
    stats: Dict[str, pl.DataFrame] = get_titled_player_stats(title_abbrv)

    for game_format, game_stats in stats.items():
        # Generate out file path
        out_file_path: Path = generate_file_path(title_abbrv, game_format)

        # Write to local file
        if write_local:
            write_to_local(game_stats, out_file_path)

        # Write to file in GCS bucket
        if overwrite_existing or not check_if_file_exists_in_gcs(out_file_path):
            write_to_gcs(game_stats, out_file_path, gcs_bucket_block_name)

    return stats


@flow
def ingest_cdc_player_stats_web_to_gcs(
    title_abbrvs: List[
        Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]
    ] = ["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> None:
    """
    Parent-flow that retrieves titled player game statistics across a range of titles
    using the public Chess.com API and writes these statistics to files in GCS bucket and
    optionally locally.
    """
    # Ingest titled player profiles for each title specified
    for title_abbrv in title_abbrvs:
        ingest_titled_players_stats(
            title_abbrv, gcs_bucket_block_name, write_local, overwrite_existing
        )


if __name__ == "__main__":
    ingest_cdc_player_stats_web_to_gcs()
