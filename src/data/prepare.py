"""Module prepare.py"""
import pandas as pd

import config


class Prepare:

    def __init__(self):
        """
        Constructor
        """

        # Configurations
        self.__configurations = config.Config()

    @staticmethod
    def __lower(blob: pd.DataFrame):
        """

        :param blob:
        :return:
        """

        frame: pd.DataFrame = blob.copy()
        for field in ['fuel_type', 'journey_details']:
            frame[field] = frame[field].str.lower()

        return frame

    def __fuel_type(self, blob: pd.DataFrame):
        """

        :param blob: A dataframe of mileage data
        :return:
        """

        frame = blob.copy()
        frame['fuel_type'] = frame['fuel_type'].map(self.__configurations.fuel_type)

        return frame

    def exc(self, blob: pd.DataFrame, organisation_id: int) -> pd.DataFrame:
        """

        :param blob: A dataframe of mileage data
        :param organisation_id: The identification code of the organisation whence the data came from
        :return:
        """

        frame = blob.copy()

        # Foremost, rename the fields; in line with naming conventions.
        frame = frame.copy().rename(columns=self.__configurations.rename)

        # Convert the content of text fields to lower case
        frame = self.__lower(blob=frame)

        # Update, correct, fuel type categories
        frame = self.__fuel_type(blob=frame)

        # Next, append a source organisation identification code.
        frame = frame.assign(organisation_id=organisation_id)

        return frame
