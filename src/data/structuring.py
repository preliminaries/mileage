"""Module reading.py"""
import glob
import os

import dask
import pandas as pd

import config
import src.elements.sheet
import src.functions.directories
import src.functions.streams
import src.functions.xlsx
import src.data.prepare


class Reading:

    def __init__(self, raw_: str, initial_:str):
        """

        :param raw_: The raw file's directory
        :param initial_: The directory for the extracted spreadsheets data
        """

        self.__file = glob.glob(pathname=os.path.join(raw_, '*.xlsx'))[0]
        self.__initial_ = initial_

        # Configurations
        self.__configurations = config.Config()

        # An instance for interacting with spreadsheets, for writing CSV (comma separated values),
        # for preparing the data extracted from the spreadsheets
        self.__spreadsheet = src.elements.sheet.Sheet(
            io='', sheet_name='', header=0, usecols='A:M', skiprows=0,
            parse_dates=self.__configurations.parse_dates, dtype=self.__configurations.dtype)
        self.__streams = src.functions.streams.Streams()
        self.__prepare = src.data.prepare.Prepare()

        self.__xlsx = src.functions.xlsx.XLSX()

    @dask.delayed
    def __sheet(self, sheet_name: str):
        """

        :param sheet_name: The name of an Excel document's sheet
        :return:
        """

        dictionary = {'io': self.__file, 'sheet_name': sheet_name}

        return self.__spreadsheet._replace(**dictionary)

    @dask.delayed
    def __read(self, sheet: src.elements.sheet.Sheet) -> pd.DataFrame:
        """

        :param sheet: @ src.elements.sheet.Sheet
        """

        return self.__xlsx.read(sheet=sheet)

    @dask.delayed
    def __preparing(self, blob: pd.DataFrame, organisation_id: int) -> pd.DataFrame:
        """

        :param blob: The data read from a spreadsheet; it is the data of a single organisation.
        :param organisation_id: The identification code of the organisation whence the data came.
        :return:
        """

        return self.__prepare.exc(blob=blob, organisation_id=organisation_id)

    @dask.delayed
    def __persist(self, blob: pd.DataFrame, organisation_id: int) -> str:
        """

        :param blob: The data read from a spreadsheet; each spreadsheet records the data of a single organisation
        :param organisation_id: The identification code of the organisation whence the data came.
        :return:
        """

        message = self.__streams.write(
            blob=blob, path=os.path.join(self.__initial_, f'{organisation_id}.csv'))

        return message

    def exc(self, organisations: pd.DataFrame):
        """

        :param organisations: The inventory of organisations in focus.
        :return:
        """

        # Storage
        src.functions.directories.Directories().create(
            path=self.__initial_)

        # Iterable
        tabs: list[dict] = organisations[['organisation_id', 'mileage_tab']].to_dict(orient='records')

        # Hence
        computations = []
        for tab in tabs:

            sheet = self.__sheet(sheet_name=tab['mileage_tab'])
            readings = self.__read(sheet=sheet)
            blob = self.__preparing(blob=readings, organisation_id=tab['organisation_id'])
            message = self.__persist(blob=blob, organisation_id=tab['organisation_id'])
            computations.append(message)

        messages = dask.compute(computations)[0]

        return messages
