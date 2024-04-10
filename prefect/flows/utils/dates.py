from datetime import date


def convert_numeric_month_to_string(month: int) -> str:
    """
    Converts a numeric month to its corresponding 3-character string representation.

    Args:
        month (int): The numeric representation of the month (1-12).

    Returns:
        str: The string representation of the month.

    Raises:
        ValueError: If month integer value is not valid.
    """
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


def check_valid_year(year: int) -> None:
    """
    Checks if the given year is valid for the purposes of FIDE chess ratings data
    extraction.

    Args:
        year (int): The year to be checked.

    Raises:
        ValueError: If the year is not a valid integer year value between 2015 and the
        current year inclusive.

    Returns:
        None
    """
    MIN_YEAR, MAX_YEAR = 2015, date.today().year
    error_message = (
        "Year value is not valid. Please enter an integer year value between "
        f"{MIN_YEAR} and {MAX_YEAR}, inclusive. You entered {year =}."
    )
    if (year < MIN_YEAR) or (year > MAX_YEAR) or not isinstance(year, int):
        raise ValueError(error_message)


def check_valid_month(month: int) -> None:
    """
    Check if the given month value is valid.

    Args:
        month (int): The month value to be checked.

    Raises:
        ValueError: If the month value is not a valid integer between 1 and 12 inclusive.

    Returns:
        None
    """
    MIN_MONTH, MAX_MONTH = 1, 12
    error_message = (
        "Month value is not valid. Please enter an integer month value between "
        f"{MIN_MONTH} and {MAX_MONTH}, inclusive. You entered {month =}."
    )
    if (month < MIN_MONTH) or (month > MAX_MONTH) or not isinstance(month, int):
        raise ValueError(error_message)
