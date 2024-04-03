from typing import Dict

import requests


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
