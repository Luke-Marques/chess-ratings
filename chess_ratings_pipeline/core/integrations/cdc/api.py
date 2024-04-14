import logging
from typing import Dict, List

import requests

from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle


class ChessDotComAPI:
    """
    A class that interacts with the public Chess.com API to retrieve information about
    Chess.com players.
    """

    def __init__(self, headers: Dict[str, str] = {"User-Agent": "default@domain.com"}):
        self.api_endpoint_prefix: str = "https://api.chess.com/pub/"
        self.headers = headers

    def _request(self, api_endpoint_suffix: str) -> dict | None:
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
        logger = logging.getLogger()
        api_endpoint_url = self.api_endpoint_prefix + api_endpoint_suffix
        try:
            response = requests.get(api_endpoint_url, headers=self.headers)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error fetching data from Chess.com API: {e}")
            return None
        # Check for empty response body
        response.raise_for_status()
        if response.status_code == 204:
            raise ValueError(
                f"API response for URL {api_endpoint_url} is empty and has status code 204."
            )
        response: dict = response.json()
        return response

    def fetch_titled_players_usernames(
        self,
        chess_titles: List[ChessTitle] | ChessTitle,
    ) -> Dict[str, List[str]]:
        """
        Queries the public Chess.com API to retrieve a list of Chess.com usernames of
        Chess.com players with a given chess title.

        Args:
            chess_titles (List[ChessTitle] | ChessTitle):
                The chess title(s) to retrieve usernames for.

        Returns:
            dict:
                A dictionary where the keys are the title abbreviations and the values
                are lists of Chess.com player usernames.

        Raises:
            ValueError: If the title abbreviation is not valid.
        """
        if not isinstance(chess_titles, List):
            chess_titles = [chess_titles]
        usernames = {}
        for chess_title in chess_titles:
            api_endpoint_suffix = f"titled/{chess_title.name}"
            response: Dict = self._request(api_endpoint_suffix)
            usernames[chess_title.name] = response["players"]
        return usernames

    def fetch_player_id(self, username: str) -> int:
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

    def fetch_player_profile(self, username: str) -> dict:
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
        response: dict = self._request(api_endpoint_suffix)
        return response

    def fetch_player_stats(self, username: str) -> dict:
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
        response: dict = self._request(api_endpoint_suffix)
        return response
