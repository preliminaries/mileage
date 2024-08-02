"""Module dictionary.py"""
import glob
import os

import pandas as pd


class Dictionary:
    """
    Class KeyStrings
    """

    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def __local(path: str, extension: str) -> pd.DataFrame:
        """

        :param path: The path wherein the files of interest lie
        :param extension: The extension type of the files of interest
        :return:
        """

        # The list of files within the path directory, including its child directories.
        files: list[str] = glob.glob(pathname=os.path.join(path, '**',  f'*.{extension}'),
                                     recursive=True)

        details: list[dict] = [
            {'file': file,
             'vertex': file.rsplit(os.path.sep, maxsplit=1)[1]}
            for file in files]

        return pd.DataFrame.from_records(details)

    def exc(self, path: str, extension: str, prefix: str) -> pd.DataFrame:
        """

        :param path: The path wherein the files of interest lie
        :param extension: The extension type of the files of interest
        :param prefix: The Amazon S3 (Simple Storage Service) where the files of path are heading
        :return:
        """

        # Inventory of files
        frame: pd.DataFrame = self.__local(path=path, extension=extension)

        # Building the Amazon S3 strings
        frame = frame.assign(key=prefix + frame["vertex"])

        return frame[['file', 'key']]
