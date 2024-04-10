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
    """
    Retrieves usernames of titled players from Chess.com API.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the title to retrieve usernames for.

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
def get_player_profiles(usernames: List[str]) -> Dict:
    """
    Fetches Chess.com player profiles for given usernames using Chess.com API.

    Args:
        usernames (List[str]): A list of usernames for which player profiles need to be
        fetched.

    Returns:
        Dict: A dictionary containing player profiles, where the keys are the usernames
        and the values are the profiles.
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
    """
    Convert a list of Chess.com player profiles dictionaries to a single Polars
    DataFrame.

    Args:
        profiles (List[Dict]): A list of player profiles, where each profile is a
        dictionary.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the converted player profiles.
    """
    print("Converting player profile from dictionary to Polars DataFrame...")
    profiles: pl.DataFrame = pl.DataFrame(profiles)
    print("Done.")
    return profiles


@task(log_prints=True)
def clean_player_profiles(profiles: pl.DataFrame) -> pl.DataFrame:
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
    Retrieve Chess.com player profile details of titled players of a given title.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the chess title for which to retrieve the profiles.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the profile details of titled
        players.
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

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the player's title.
        scrape_date (datetime, optional):
            The date of the scrape. Defaults to today's date.
        extension (str, optional):
            The file extension. Defaults to "parquet".

    Returns:
        Path: The full file path for storing the player profiles data.
    """
    # Generate filename
    file_name = Path(
        f"{title_abbrv.lower()}_player_profiles_"
        f"{str(scrape_date.date()).replace('-', '_')}.{extension}"
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
    Prefect sub-flow which retrieves Chess.com player profile details of titled players
    using the Chess.com API and writes these profiles to files in GCS bucket and
    optionally locally.

    Args:
        title_abbrv (Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]):
            The abbreviation of the player's title.
        gcs_bucket_block_name (str):
            The name of the GCS bucket Prefect block. Defaults to "chess-ratings-dev".
        write_local (bool):
            Whether to write the profiles to a local file as well as to
            GCS bucket. Defaults to False.
        overwrite_existing (bool):
            Whether to overwrite existing files in the GCS bucket. Defaults to True.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the profile details of titled players.
    """
    # Get cleaned DataFrame of titled players profile details
    profiles: pl.DataFrame = get_titled_players_profiles(title_abbrv)

    # Generate out file path
    out_file_path: Path = generate_file_path(title_abbrv)

    # Write to local file
    if write_local:
        print("Writing profile data to local file...")
        write_to_local(profiles, out_file_path, return_state=False)
        print("Done.")

    # Write to file in GCS bucket
    if overwrite_existing or not check_if_file_exists_in_gcs(out_file_path):
        print("Writing profile data to GCS bucket...")
        write_to_gcs(profiles, out_file_path, gcs_bucket_block_name, return_state=False)
        print("Done.")

    return profiles


@flow
def ingest_cdc_profiles_web_to_gcs(
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
    Prefect parent-flow that retrieves Chess.com player profile details for titled
    players across a range of titles using the public Chess.com API and writes these
    profiles to files in GCS bucket and optionally locally.

    Args:
        title_abbrvs (List[Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"]] | Literal["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"], optional):
            The list of player titles or a single player title to retrieve profiles for. Defaults to ["GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"].
        gcs_bucket_block_name (str, optional):
            The name of the GCS bucket block. Defaults to "chess-ratings-dev".
        write_local (bool, optional):
            Whether to write the profiles to a local file as well as to GCS bucket.
            Defaults to False.
        overwrite_existing (bool, optional):
            Whether to overwrite existing files in the GCS bucket. Defaults to True.

    Returns:
        None
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
