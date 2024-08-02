"""Module sheet.py"""
import typing

class Sheet(typing.NamedTuple):
    """
    Attributes
    ----------

    For [pandas.read_excel](https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html#pandas-read-excel)

    io : str
        Input string

    sheet_name : str
        An Excel Sheet name.

    header : int
        The row number of the header row

    usecols : str
            "... comma separated list of Excel column letters and column ranges
            (e.g. “A:E” or “A,C,E:F”). Ranges are inclusive of both sides."

    skiprows : int
        "Line numbers to skip (0-indexed) or number of lines to skip (int) ..."

    parse_dates: list
        The list of date fields to parse.

    dtype: dict
        A dictionary of fields, and their types.
    """

    io: str
    sheet_name: str
    header: int
    usecols: str
    skiprows: int
    parse_dates: list
    dtype: dict
