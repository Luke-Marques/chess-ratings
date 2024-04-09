from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import polars as pl
from utils.chess_dot_com_api import ChessTitle, ChessAPI
from utils.write_data import check_if_file_exists_in_gcs, write_to_gcs, write_to_local

from prefect import flow, task


@task(retries=3, log_prints=True)
def get_titled_usernames(
    title_abbrv: ChessTitle,
) -> List[str]:
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
    usernames: List[str], title_abbrv: ChessTitle
) -> List[Dict]:
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
    all_game_formats_stats: pl.DataFrame = pl.from_pandas(
        pd.json_normalize(all_game_formats_stats)
    )
    return all_game_formats_stats


@task
def seperate_game_formats(
    all_game_formats_stats: pl.DataFrame,
) -> Dict[str, pl.DataFrame]:
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
    title_abbrv: ChessTitle,
) -> Dict[str, pl.DataFrame]:
    """
    Function which uses the public Chess.com API to return the game statistics of all
    titled players of a given title.
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
    title_abbrv: ChessTitle,
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
        f"{title_abbrv.lower()}_{game_format}_stats_"
        f"{str(scrape_date.date()).replace("-", "_")}.{extension}"
    )

    # Generate filepath
    file_path = (
        Path("data") / "chess_dot_com" / "player_game_stats" / game_format / file_name
    )

    return file_path


@flow(log_prints=True, cache_result_in_memory=False, persist_result=False)
def ingest_titled_players_stats(
    title_abbrv: ChessTitle,
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> pl.DataFrame:
    """
    Sub-flow that retrieves titled player game statistics using the public Chess.com API
    and writes these profiles to files in GCS bucket and optionally locally.
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
    title_abbrvs: List[ChessTitle] | ChessTitle = [
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
    Parent-flow that retrieves titled player game statistics across a range of titles
    using the public Chess.com API and writes these statistics to files in GCS bucket and
    optionally locally.
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
    ingest_cdc_player_stats_web_to_gcs("WGM", write_local=True)
