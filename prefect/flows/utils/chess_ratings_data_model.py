from datetime import date
from typing import Optional

import patito as pt
import polars as pl
from pydantic import validator


class ChessRating(pt.Model):
    """Data model for validation of individual ratings datasets."""

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
