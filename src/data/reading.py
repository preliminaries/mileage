"""Module reading.py"""
import dask

import pandas as pd

import src.functions.xlsx
import src.elements.sheet

import numpy as np

class Reading:

    def __init__(self, file: str):
        """

        :param file:
        """

        self.__file = file

        # Rename
        self.__rename = {'Area Pay Division': 'area_pay_division', 'Claim Line Start': 'claim_line_start',
                         'Claim Line End': 'claim_line_end', 'Engine Size': 'engine_size',
                         'Fuel Type': 'fuel_type', 'CO2 Emissions': 'co2_emissions',
                         'Business Mileage': 'business_mileage', 'Business Rate High': 'business_rate_high',
                         'Business Rate Low': 'business_rate_low', 'Business Value': 'business_value',
                         'Commute Miles Not Undertaken': 'commute_miles_not_undertaken',
                         'Overtime Mileage': 'overtime_mileage', 'Journey Details': 'journey_details'}

        # An instance for interacting with spreadsheets
        self.__spreadsheet = src.elements.sheet.Sheet()

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
    def __temp(self, readings: pd.DataFrame) -> pd.DataFrame:

        readings = readings.rename(columns=self.__rename)

        return readings.head()

    def exc(self, tabs: np.ndarray):
        """

        :param tabs:
        :return:
        """

        computations = []
        for tab in tabs:

            sheet = self.__sheet(sheet_name=tab)
            readings = self.__read(sheet=sheet)
            excerpt = self.__temp(readings=readings)
            computations.append(excerpt)

        details = dask.compute(computations)[0]
        print(details)
