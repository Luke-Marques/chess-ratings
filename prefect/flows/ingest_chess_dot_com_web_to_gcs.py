from typing import Literal, List

import requests


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

    # Construct the API URL
    url = f"https://api.chess.com/pub/titled/{title_abbrv}"

    # Define headers for request
    default_email = "default@domain.com"
    headers = {"User-Agent": default_email}

    # Query API
    response = requests.get(url, headers=headers)

    # Check for empty response body
    response.raise_for_status()
    if response.status_code != 204:
        titled_players_usernames: List[str] = response.json()["players"]
        return titled_players_usernames


def main() -> None:
    response = get_titled_players_usernames("GM")
    print(response)


if __name__ == "__main__":
    main()
