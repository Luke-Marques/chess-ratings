from typing import Literal, List, Dict

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


def request_from_chess_dot_com_public_api(
    api_endpoint_suffix: str, headers: Dict = {"User-Agent": "default@domain.com"}
) -> Dict:
    """Function which queries the public Chess.com API and returns the JSON response."""
    # Construct the API request URL
    api_endpoint_url = f"https://api.chess.com/pub/{api_endpoint_suffix}"

    # Query API
    response = requests.get(api_endpoint_url, headers=headers)

    # Check for empty response body
    response.raise_for_status()
    if response.status_code != 204:
        return response.json()
    raise ValueError(
        f"API response for URL {api_endpoint_url} is empty and has status code 204."
    )


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


def get_player_stats(username: str) -> Dict:
    """
    Function which uses the public Chess.com API to return the game statistics of a
    given player.
    """
    # Define the API endpoint suffix
    api_endpoint_suffix = f"player/{username}/stats"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    return response


def main() -> None:
    gm_players = get_titled_players_usernames("GM")
    gm_player_profile_details = get_player_profile_details(gm_players[0])
    print(gm_player_profile_details)
    gm_player_stats = get_player_stats(gm_players[0])
    print(gm_player_stats)


if __name__ == "__main__":
    main()
