from typing import Dict, List, Literal

import requests

from prefect import task


@task
def check_title_abbrv(
    title_abbrv: str,
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
    if title_abbrv.upper() not in allowed_title_abbrvs:
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


@task(retries=3)
def get_titled_players_usernames(
    title_abbrv: Literal[
        "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
    ],
) -> List[str]:
    """
    Function which uses the public Chess.com API to return a list of Chess.com usernames
    of players with a given title.
    """
    # Define the API endpoint suffix
    api_endpoint_suffix = f"titled/{title_abbrv}"

    # Query API
    response: Dict = request_from_chess_dot_com_public_api(api_endpoint_suffix)

    return response["players"]
