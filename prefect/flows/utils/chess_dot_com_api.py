from typing import Dict, List, Literal

import requests


type ChessTitle = Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
]


class ChessAPI:
    def __init__(self, headers: Dict[str, str] = {"User-Agent": "default@domain.com"}):
        self.api_endpoint_prefix: str = "https://api.chess.com/pub/"
        self.headers = headers

    @staticmethod
    def _check_title_abbrv(title_abbrv: str) -> None:
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

    def _request(self, api_endpoint_suffix: str) -> Dict:
        """Function which queries the public Chess.com API and returns the JSON response."""
        api_endpoint_url = self.api_endpoint_prefix + api_endpoint_suffix
        response = requests.get(api_endpoint_url, headers=self.headers)
        # Check for empty response body
        response.raise_for_status()
        if response.status_code == 204:
            raise ValueError(
                f"API response for URL {api_endpoint_url} is empty and has status code 204."
            )
        return response.json()

    def get_titled_players_usernames(
        self,
        title_abbrvs: List[ChessTitle] | ChessTitle,
    ) -> Dict[str, List[str]]:
        """
        Function which uses the public Chess.com API to return a list of Chess.com usernames
        of players with a given title.
        """
        if not isinstance(title_abbrvs, List):
            title_abbrvs = [title_abbrvs]
        usernames = {}
        for title_abbrv in title_abbrvs:
            self._check_title_abbrv(title_abbrv)
            api_endpoint_suffix = f"titled/{title_abbrv}"
            response: Dict = self._request(api_endpoint_suffix)
            usernames[title_abbrv] = response["players"]
        return usernames

    def get_player_id(self, username: str) -> int:
        """
        Function which uses the public Chess.com API to retrieve the non-changing, Chess.com
        player ID for a given player username.
        """
        api_endpoint_suffix = f"player/{username}"
        response: Dict = self._request(api_endpoint_suffix)
        player_id: int = response["player_id"]
        return player_id

    def get_player_profile(self, username: str) -> Dict:
        """
        Function which uses the public Chess.com API to return the profile details of a
        given player.
        """
        api_endpoint_suffix = f"player/{username}"
        response: Dict = self._request(api_endpoint_suffix)
        return response

    def get_player_stats(self, username: str) -> Dict:
        """
        Function which uses the public Chess.com API to return the game statistics of a
        given player.
        """
        api_endpoint_suffix = f"player/{username}/stats"
        response: Dict = self._request(api_endpoint_suffix)
        return response
