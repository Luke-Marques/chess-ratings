from datetime import datetime
from typing import Dict, List, Literal

import polars as pl
from utils.chess_dot_com_api import (
    check_title_abbrv,
    request_from_chess_dot_com_public_api,
    get_titled_players_usernames,
)
from utils.write_data import write_to_local, check_if_file_exists_in_gcs, write_to_gcs

from pathlib import Path

from prefect import task, flow


@task(retries=3)
def get_player_profile(username: str) -> Dict:
    """
    Function which uses the public Chess.com API to return the profile details of a
    given player.
    """
    # Define the API endpoint suffix
    api_endpoint_suffix = f"player/{username}"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    return response


@task
def clean_player_profiles(profiles: pl.DataFrame) -> pl.DataFrame:
    """
    Clean a Chess.com player profiles Polars DataFrame by renaming columns, and adding a
    column indicating the date of scraping.
    """
    profiles_clean = (
        # Convert DataFrame to LazyFrame
        profiles.lazy()
        # Rename columns
        .rename(
            {
                "avatar": "avatar_url",
                "@id": "api_url",
                "url": "player_profile_url",
                "followers": "follower_count",
            }
        )
        # Add column of todays date/time
        .with_columns(pl.lit(datetime.now()).alias("scrape_datetime"))
        # Drop any duplicate rows
        .unique()
        # Convert LazyFrame back to DataFrame
        .collect()
    )
    return profiles_clean


@flow
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
    usernames: List[str] = get_titled_players_usernames(title_abbrv)

    # Get profile details for each titled player
    profiles: List[Dict] = [get_player_profile(username) for username in usernames]

    # Convert list of profile dictionaries to Polars DataFrame and clean
    profiles: pl.DataFrame = clean_player_profiles(pl.DataFrame(profiles))

    return profiles


def generate_file_name(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
    scrape_date: datetime = datetime.today(),
    extension: str = "parquet",
) -> Path:
    """Generate a filename for the storage of player profiles data locally or in GCS."""
    # Generate filename
    file_name = Path(
        f"{title_abbrv.lower()}_titled_player_profiles_"
        f"{str(scrape_date.date()).replace("-", "_")}.{extension}"
    )

    return file_name


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
    file_name: Path = generate_file_name(title_abbrv, scrape_date, extension)

    # Generate filepath
    file_path = (
        Path("data")
        / "chess_dot_com"
        / "player_profiles"
        / title_abbrv.lower()
        / file_name
    )

    return file_path


@flow()
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
    # Check that title abbreviation is valid
    check_title_abbrv(title_abbrv)

    # Get cleaned DataFrame of titled players profile details
    profiles: pl.DataFrame = get_titled_players_profiles(title_abbrv)

    # Generate out file path
    out_file_path: Path = generate_file_path(title_abbrv)

    # Write to local file
    if write_local:
        write_to_local(profiles, out_file_path)

    # Write to file in GCS bucket
    if overwrite_existing or not check_if_file_exists_in_gcs(out_file_path):
        write_to_gcs(profiles, out_file_path, gcs_bucket_block_name)

    return profiles


@flow
def ingest_cdc_profiles_web_to_gcs(
    title_abbrvs: List[
        Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]
    ] = ["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"],
    gcs_bucket_block_name: str = "chess-ratings-dev",
    write_local: bool = False,
    overwrite_existing: bool = True,
) -> None:
    """
    Parent-flow that retrieves titled player profile details across a range of titles
    using the public Chess.com API and writes these profiles to files in GCS bucket and
    optionally locally.
    """
    # Ingest titled player profiles for each title specified
    for title_abbrv in title_abbrvs:
        ingest_titled_players_profiles(
            title_abbrv, gcs_bucket_block_name, write_local, overwrite_existing
        )


if __name__ == "__main__":
    ingest_cdc_profiles_web_to_gcs()
