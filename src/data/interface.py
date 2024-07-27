import logging

import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.csv


class Interface:

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service:
        :param s3_parameters:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

        # Logging
        logging.basicConfig(level=logging.INFO, format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def __organisations(self) -> pd.DataFrame:
        """

        :return:
        """

        bucket_name = self.__s3_parameters.external
        prefix = self.__s3_parameters.path_external_references

        return src.s3.csv.CSV(service=self.__service, bucket_name=bucket_name, prefix=prefix).exc(
            filename='organisations.csv')

    def exc(self):
        """

        :return:
        """

        organisations = self.__organisations()
        self.__logger.info(organisations)
