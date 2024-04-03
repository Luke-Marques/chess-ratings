from typing import Dict

from utils.chess_dot_com_api import request_from_chess_dot_com_public_api


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
