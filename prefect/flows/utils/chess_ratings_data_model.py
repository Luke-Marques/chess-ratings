from datetime import date
from typing import Optional

import patito as pt
import polars as pl
from pydantic import validator


class ChessRating(pt.Model):
    """
    Patito data model/schema which represents a FIDE chess rating entry.

    Attributes:
        fide_id (int): The FIDE ID of the player.
        player_name (str, optional): The name of the player.
        fide_federation (str, optional): The FIDE federation of the player.
        sex (int, optional): The sex of the player.
        title (str, optional): The title of the player.
        w_title (str, optional): The women's title of the player.
        o_title (str, optional): The open title of the player.
        foa_title (str, optional): The FIDE Online Arena title of the player.
        rating (int): The rating of the player.
        game_count (int): The number of games played by the player.
        k (int): The K-factor used for rating calculation.
        birth_year (int, optional): The birth year of the player.
        flag (str, optional): The flag of the player.
        period_year (int): The year of the rating period.
        period_month (int): The month of the rating period.

    Methods:
        within_date_range(cls, value): 
            Validates that the birth year is within a valid range.

    """

    fide_id: int = pt.Field(unique=True)
    player_name: Optional[str]
    fide_federation: Optional[str] = pt.Field(pattern=r"(?i)[A-Z]{3}")
    sex: Optional[int] = pt.Field(dtype=pl.Int8)
    title: Optional[str]
    w_title: Optional[str]
    o_title: Optional[str]
    foa_title: Optional[str]
    rating: int
    game_count: int
    k: int
    birth_year: Optional[int] = None
    flag: Optional[str]
    period_year: int = pt.Field(ge=2000, le=date.today().year)
    period_month: int = pt.Field(ge=1, le=12)

    @validator("birth_year")
    def within_date_range(cls, value):
        assert 1900 <= value <= date.today().year
        return value
