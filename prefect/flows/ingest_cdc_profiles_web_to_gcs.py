from datetime import datetime
from typing import Dict, List, Literal

import polars as pl
from utils.chess_dot_com_api import request_from_chess_dot_com_public_api


def check_title_abbrv(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> None:
    allowed_title_abbrvs = [
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
    ]
    if title_abbrv not in allowed_title_abbrvs:
        error_message = f"Title abbreviation is not valid: {title_abbrv}"
        raise ValueError(error_message)


def get_titled_players_usernames(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> List[str]:
    """
    Function which uses the public Chess.com API to return a list of Chess.com usernames
    of players with a given title.
    """
    # Check title abbreviation string is valid
    check_title_abbrv(title_abbrv)

    # Define the API endpoint suffix
    api_endpoint_suffix = f"titled/{title_abbrv}"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    return response["players"]


def get_player_profile_details(username: str) -> Dict:
    """
    Function which uses the public Chess.com API to return the profile details of a
    given player.
    """
    # Define the API endpoint suffix
    api_endpoint_suffix = f"player/{username}"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    return response


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
    profiles: List[Dict] = [
        get_player_profile_details(username) for username in usernames
    ]

    # Convert list of profile dictionaries to Polars DataFrame and clean
    profiles: pl.DataFrame = clean_player_profiles(pl.DataFrame(profiles))

    return profiles


def main() -> None:
    print(get_titled_players_profiles("GM"))


if __name__ == "__main__":
    main()
