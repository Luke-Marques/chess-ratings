from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal

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
    # Retrieve usernames from Chess.com API
    print(f"Fetching {title_abbrv} titled players' usernames...")
    usernames: List[str] = ChessAPI().get_titled_players_usernames(title_abbrv)[
        title_abbrv
    ]

    # Display username count
    print(f"Done. {len(usernames):_} usernames retrieved.")

    return usernames


@task(retries=3, log_prints=True)
def get_player_profiles(usernames: List[str]) -> Dict:
    """
    Wrapper function for ChessAPI class method get_player_profile to use Prefect task
    API.
    """
    print("Fetching player profiles for each username...")
    profiles = []
    for index, username in enumerate(usernames):
        print(f"Player {index+1:_} of {len(usernames):_}")
        profiles.append(ChessAPI().get_player_profile(username))
    return profiles


@task(log_prints=True)
def convert_player_profiles_from_dictionaries_to_dataframe(
    profiles: List[Dict],
) -> pl.DataFrame:
    print("Converting player profile from dictionary to Polars DataFrame...")
    profiles: pl.DataFrame = pl.DataFrame(profiles)
    print("Done.")
    return profiles


@task(log_prints=True)
def clean_player_profiles(profiles: pl.DataFrame) -> pl.DataFrame:
    """
    Clean a Chess.com player profiles Polars DataFrame by renaming columns, and adding a
    column indicating the date of scraping.
    """
    print("Cleaning player profile dataframe...")
    profiles_clean = (
        profiles.lazy()
        .rename(
            {
                "avatar": "avatar_url",
                "@id": "api_url",
                "url": "profile_url",
                "followers": "follower_count",
            }
        )
        # Add column of todays date/time
        .with_columns(pl.lit(datetime.now()).alias("scrape_datetime"))
        .unique()
        .select(
            "player_id",
            "username",
            "name",
            "title",
            "profile_url",
            "avatar_url",
            "api_url",
            "follower_count",
            "country",
            "location",
            "last_online",
            "joined",
            "status",
            "is_streamer",
            "twitch_url",
            "verified",
            "league",
            "scrape_datetime",
        )
        .collect()
    )
    print("Done.")
    return profiles_clean


@flow(log_prints=True)
def get_titled_players_profiles(
    title_abbrv: Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
],
) -> pl.DataFrame:
    """
    Function which uses the public Chess.com API to retrieve profile details of titled
    players of a given title, as a Polars DataFrame.
    """
    # Get list of titled players usernames
    usernames: List[str] = get_titled_usernames(title_abbrv)

    # Get profile details for each titled player
    profiles: List[Dict] = get_player_profiles(usernames)

    # Convert list of profile dictionaries to Polars DataFrame
    profiles: pl.DataFrame = convert_player_profiles_from_dictionaries_to_dataframe(
        profiles
    )

    # Clean player profiles DataFrame
    profiles: pl.DataFrame = clean_player_profiles(profiles)

    return profiles


@task
def generate_file_path(
    title_abbrv: Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
],
    scrape_date: datetime = datetime.today(),
    extension: str = "parquet",
) -> Path:
    """
    Generate a full file path for the storage of player profiles data locally or in GCS.
    """
    # Generate filename
    file_name = Path(
        f"{title_abbrv.lower()}_player_profiles_"
        f"{str(scrape_date.date()).replace("-", "_")}.{extension}"
    )

    # Generate filepath
    file_path = (
        Path("data")
        / "chess_dot_com"
        / "player_profiles"
        / title_abbrv.lower()
        / file_name
    )

    return file_path


@flow(log_prints=True, cache_result_in_memory=False, persist_result=False)
def ingest_titled_players_profiles(
    title_abbrv: Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> pl.DataFrame:
    """
    Sub-flow that retrieves titled player profile details using the public Chess.com API
    and writes these profiles to files in GCS bucket and optionally locally.
    """
    # Get cleaned DataFrame of titled players profile details
    profiles: pl.DataFrame = get_titled_players_profiles(title_abbrv)

    # Generate out file path
    out_file_path: Path = generate_file_path(title_abbrv)

    # Write to local file
    if write_local:
        print("Writing profile data to local file...")
        write_to_local(profiles, out_file_path)
        print("Done.")

    # Write to file in GCS bucket
    if overwrite_existing or not check_if_file_exists_in_gcs(out_file_path):
        print("Writing profile data to GCS bucket...")
        write_to_gcs(profiles, out_file_path, gcs_bucket_block_name)
        print("Done.")

    return profiles


@flow
def ingest_cdc_profiles_web_to_gcs(
    title_abbrvs: List[Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
]] | Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
] = [
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
    Parent-flow that retrieves titled player profile details across a range of titles
    using the public Chess.com API and writes these profiles to files in GCS bucket and
    optionally locally.
    """
    if not isinstance(title_abbrvs, list):
        title_abbrvs = [title_abbrvs]
    for title_abbrv in title_abbrvs:
        ingest_titled_players_profiles(
            title_abbrv,
            gcs_bucket_block_name,
            write_local,
            overwrite_existing,
            return_state=True,
        )


if __name__ == "__main__":
    ingest_cdc_profiles_web_to_gcs()
