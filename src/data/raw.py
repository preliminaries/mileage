import os
import logging

import pandas as pd

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys
import src.s3.egress
import src.functions.directories


class Raw:

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 (Simple Storage Service) parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__service = service
        self.__s3_parameters: s3p.S3Parameters = s3_parameters

        # Configurations
        self.__configurations = config.Config()
        src.functions.directories.Directories().create(
            path=self.__configurations.raw_)

        # Logging
        logging.basicConfig(level=logging.INFO, format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

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
        frame = frame.assign(filename= self.__configurations.raw_ + os.path.sep + frame['vertex'])

        return frame

    def exc(self):

        keys = self.__keys()
        strings = self.__strings(keys=keys)

        # Download
        messages = src.s3.egress.Egress(
            service=self.__service, bucket_name=self.__s3_parameters.internal).exc(strings=strings)

        self.__logger.info(type(messages))
        self.__logger.info(messages)