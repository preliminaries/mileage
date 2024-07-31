"""Module reading.py"""
import dask

import pandas as pd

import src.functions.xlsx
import src.elements.sheet

import numpy as np

class Reading:

    def __init__(self):

        self.__spreadsheet = src.elements.sheet.Sheet()

    def __sheet(self, sheet_name: str):

        dictionary = {'sheet_name': sheet_name, 'header': 0, 'usecols': 'A:M'}

        return self.__spreadsheet._replace(**dictionary)

    @dask.delayed
    def __read(self, sheet: src.elements.sheet.Sheet) -> pd.DataFrame:
        """

        :param sheet: @ src.elements.sheet.Sheet
        """

        try:
            # noinspection PyTypeChecker
            return pd.read_excel(io=sheet.io, sheet_name=sheet.sheet_name, header=sheet.header,
                                 usecols=sheet.usecols, engine='openpyxl')
        except OSError as err:
            raise err from err

    def exc(self, tabs: np.ndarray):

        computations = []
        for tab in tabs:

            sheet = self.__sheet(sheet_name=tab)
            readings = self.__read(sheet=sheet)
            dask.compute(readings.head())
