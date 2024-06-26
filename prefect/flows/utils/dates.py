from datetime import date
from typing import List, Tuple

import polars as pl


def convert_numeric_month_to_string(month: int) -> str:
    """Convert a numeric month (e.g. integer between 1-12 inclusive) to a 3 character code."""
    check_valid_month(month)
    months = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    ]
    return months[month - 1]


def get_date_range(start_date: date, end_date: date) -> List[Tuple[int, int]]:
    """Generate a list of dates as years and months between two dates."""
    dates = pl.date_range(start_date, end_date, interval="1mo", eager=True)
    return [(d.year, d.month) for d in dates]


def check_valid_year(year: int) -> None:
    min_year, max_year = 2015, date.today().year
    error_message = (
        "Year value is not valid. Please enter an integer year value between "
        f"{min_year} and {max_year}, inclusive. You entered {year =}."
    )
    if (year < min_year) or (year > max_year) or not isinstance(year, int):
        raise ValueError(error_message)


def check_valid_month(month: int) -> None:
    min_month, max_month = 1, 12
    error_message = (
        "Month value is not valid. Please enter an integer month value between "
        f"{min_month} and {max_month}, inclusive. You entered {month =}."
    )
    if (month < min_month) or (month > max_month) or not isinstance(month, int):
        raise ValueError(error_message)
