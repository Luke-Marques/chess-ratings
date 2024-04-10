from enum import StrEnum


class GameFormat(StrEnum):
    """
    Enumeration class representing different FIDE chess game-formats.
    """

    STANDARD = "standard"
    RAPID = "rapid"
    BLITZ = "blitz"
