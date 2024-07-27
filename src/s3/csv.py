"""
Module csv.py
"""
import pandas as pd

import src.elements.service as sr
import src.s3.unload


class CSV:
    """

    Notes
    -----
    This class reads CSV (comma separated values) files held
    within an Amazon S3 (Simple Storage Service) bucket.

    """

    def __init__(self, service: sr.Service, bucket_name: str, prefix: str):
        """

        :param service:  A suite of services for interacting with Amazon Web Services.
        :param bucket_name: The Amazon S3 (Simple Storage Service) bucket in focus
        :param prefix: The prefix of the Amazon S3 bucket wherein a document of interest lies
        """

        self.__service: sr.Service = service
        self.__bucket_name = bucket_name
        self.__prefix = prefix

        # S3 Unload Instance
        self.__unload = src.s3.unload.Unload(service=self.__service)

    def __read(self, filename: str) -> pd.DataFrame:
        """

        :param filename: The name of the Amazon S3 (Simple Storage Service) file being read.
        :return:
        """

        key_name = self.__prefix + filename
        buffer = self.__unload.exc(
            bucket_name=self.__bucket_name, key_name=key_name)

        try:
            return pd.read_csv(filepath_or_buffer=buffer, header=0, encoding='utf-8')
        except ImportError as err:
            raise err from err

    def exc(self, filename: str) -> pd.DataFrame:

        return self.__read(filename=filename)
