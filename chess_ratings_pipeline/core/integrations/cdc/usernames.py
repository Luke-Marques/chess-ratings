from typing import List

from chess_ratings_pipeline.core.integrations.cdc.api import ChessDotComAPI
from chess_ratings_pipeline.core.integrations.cdc.chess_title import ChessTitle
from prefect import task
from prefect.logging import get_run_logger


@task(retries=3, log_prints=True)
def fetch_titled_cdc_usernames(chess_title: ChessTitle) -> List[str]:
    """
    Retrieves usernames of titled players from Chess.com API.

    Args:
        chess_title (ChessTitle):
            The chess title of Chess.com players to retrieve usernames for.

    Returns:
        List[str]: A list of usernames of titled players.
    """
    # Create Prefect info logger
    logger = get_run_logger()

    # Retrieve usernames from Chess.com API
    logger.info(f"Fetching {chess_title.value} titled Chess.com players' usernames...")
    usernames: List[str] = ChessDotComAPI().fetch_titled_players_usernames(chess_title)[
        chess_title.name
    ]
    logger.info(
        f"Finished fetching usernames for {chess_title.value} titled Chess.com players."
    )
    logger.info(f"{len(usernames):_} usernames retrieved.")

    return usernames
