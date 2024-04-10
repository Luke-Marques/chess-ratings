from typing import Dict, List, Literal

import requests


type ChessTitle = Literal[
    "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", "WCM"
]


class ChessAPI:
    """
    A class that interacts with the public Chess.com API to retrieve information about 
    chess players.
    """

    def __init__(self, headers: Dict[str, str] = {"User-Agent": "default@domain.com"}):
        self.api_endpoint_prefix: str = "https://api.chess.com/pub/"
        self.headers = headers

    @staticmethod
    def _check_title_abbrv(title_abbrv: str) -> None:
        """
        Checks if the given title abbreviation is valid.

        Args:
            title_abbrv (str): The title abbreviation to check.

        Raises:
            ValueError: If the title abbreviation is not valid - does not appear in the 
            following list "GM", "WGM", "IM", "WIM", "FM", "WFM", "NM", "WNM", "CM", 
            "WCM".

        Returns:
            None
        """
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
        """
        Queries the public Chess.com API for a given API url endpoint suffix and returns 
        the JSON response as a dictionary object.

        Args:
            api_endpoint_suffix (str): The suffix of the desired API endpoint URL.

        Returns:
            dict: The JSON response from the API.

        Raises:
            ValueError: If the API response is empty.
        """
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
        Queries the public Chess.com API to retrieve a list of Chess.com usernames of 
        Chess.com players with a given chess title.

        Args:
            title_abbrvs (List[ChessTitle] | ChessTitle): 
                The title abbreviation(s) to filter the players.

        Returns:
            dict: 
                A dictionary where the keys are the title abbreviations and the values 
                are lists of Chess.com player usernames.

        Raises:
            ValueError: If the title abbreviation is not valid.
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
        Queries the public Chess.com API to retrieve the non-changing, Chess.com player 
        ID for a given player's username.

        Args:
            username (str): The Chess.com player's username.

        Returns:
            int: The Chess.com player's player ID.

        Raises:
            ValueError: If the API response is empty.
        """
        api_endpoint_suffix = f"player/{username}"
        response: Dict = self._request(api_endpoint_suffix)
        player_id: int = response["player_id"]
        return player_id

    def get_player_profile(self, username: str) -> Dict:
        """
        Queries the public Chess.com API to retrieve a Chess.com player's profile details 
        for a given Chess.com player's username.

        Args:
            username (str): The username of the Chess.com player.

        Returns:
            dict: The profile details of the Chess.com player.

        Raises:
            ValueError: If the API response is empty.
        """
        api_endpoint_suffix = f"player/{username}"
        response: Dict = self._request(api_endpoint_suffix)
        return response

    def get_player_stats(self, username: str) -> Dict:
        """
        Queries the public Chess.com API to retrieve a Chess.com player's game 
        statistics for a given Chess.com player's username.

        Args:
            username (str): The username of the Chess.com player.

        Returns:
            dict: The game statistics of the Chess.com player.

        Raises:
            ValueError: If the API response is empty.
        """
        api_endpoint_suffix = f"player/{username}/stats"
        response: Dict = self._request(api_endpoint_suffix)
        return response
