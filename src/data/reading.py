"""Module reading.py"""
import glob
import os

import dask
import pandas as pd

import src.elements.sheet
import src.functions.directories
import src.functions.streams
import src.functions.xlsx


class Reading:

    def __init__(self, raw_: str, initial_:str):
        """

        :param raw_: The raw file's directory
        :param initial_: The directory for the extracted spreadsheets data
        """

        self.__file = glob.glob(pathname=os.path.join(raw_, '*.xlsx'))[0]
        self.__initial_ = initial_

        # Storage
        src.functions.directories.Directories().create(
            path=self.__initial_)

        # An instance for interacting with spreadsheets
        self.__spreadsheet = src.elements.sheet.Sheet()

        # An instance for writing CSV (comma separated values)
        self.__streams = src.functions.streams.Streams()

    @dask.delayed
    def __sheet(self, sheet_name: str):
        """

        :param sheet_name:
        :return:
        """

        dictionary = {'io': self.__file,  'sheet_name': sheet_name,
                      'header': 0, 'usecols': 'A:M'}

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

    @dask.delayed
    def __temp(self, readings: pd.DataFrame, organisation_id: int) -> str:

        message = self.__streams.write(
            blob=readings, path=os.path.join(self.__initial_, f'{organisation_id}.csv'))

        return message

    def exc(self, organisations: pd.DataFrame):
        """

        :param organisations:
        :return:
        """

        tabs: list[dict] = organisations[['organisation_id', 'mileage_tab']].to_dict(orient='records')

        computations = []
        for tab in tabs:

            sheet = self.__sheet(sheet_name=tab['mileage_tab'])
            readings = self.__read(sheet=sheet)
            message = self.__temp(readings=readings, organisation_id=tab['organisation_id'])
            computations.append(message)

        details = dask.compute(computations)[0]

        return details
