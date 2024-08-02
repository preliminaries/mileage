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

        frame: pd.DataFrame = blob.copy()
        for field in ['fuel_type', 'journey_details']:
            frame = frame.assign(field=frame[field].str.lower())

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

        # Lower
        frame = self.__lower(blob=frame)

        # Next, append a source organisation identification code.
        frame = frame.assign(organisation_id=organisation_id)

        return frame
