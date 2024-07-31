"""Module raw.py"""
import os

import pandas as pd

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.directories
import src.s3.egress
import src.s3.keys


class Raw:
    """
    Class Raw
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, storage: str):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 (Simple Storage Service) parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param storage: The temporary storage hub of the raw files
        """

        self.__service = service
        self.__s3_parameters: s3p.S3Parameters = s3_parameters
        self.__storage = storage

        # Storage
        src.functions.directories.Directories().create(
            path=self.__storage)

        # Configurations
        self.__configurations = config.Config()

    def __keys(self) -> list[str]:
        """

        :return:
        """

        prefix = self.__configurations.s3_internal_prefix + 'raw/'

        # The instance for requesting the list of keys within an Amazon S3 bucket
        objects = src.s3.keys.Keys(
            service=self.__service, bucket_name=self.__s3_parameters.internal)

        return objects.excerpt(prefix=prefix)

    def __strings(self, keys: list[str]) -> pd.DataFrame:
        """

        :param keys: A list of Amazon S3 keys, i.e., prefix + vertex
        :return:
        """

        # A data frame consisting of the S3 keys, and the vertex of each
        # key, i.e., file name + extension
        frame = pd.DataFrame(data={'key': keys})
        frame = frame.assign(vertex=frame['key'].str.rsplit('/', n=1, expand=True)[1])

        # This line construct's the local storage string of each file
        frame = frame.assign(filename= self.__storage + os.path.sep + frame['vertex'])

        return frame

    def exc(self) -> list[str]:
        """

        :return:
        """

        keys = self.__keys()
        strings = self.__strings(keys=keys)

        # Download
        messages: list[str] = src.s3.egress.Egress(
            service=self.__service, bucket_name=self.__s3_parameters.internal).exc(strings=strings)

        return messages
