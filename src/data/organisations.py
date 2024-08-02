"""Module organisation.py"""
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.csv


class Organisations:
    """
    Class Organisations
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 (Simple Storage Service) parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

    def __organisations(self) -> pd.DataFrame:
        """
        Reads the organisations.csv file, which is hosted within
        Amazon S3 (Simple Storage Service)

        :return:
        """

        bucket_name = self.__s3_parameters.external
        prefix = self.__s3_parameters.path_external_references

        data = src.s3.csv.CSV(
            service=self.__service, bucket_name=bucket_name, prefix=prefix).exc(
            filename='organisations.csv')

        return data

    def exc(self):
        """

        :return:
        """

        return self.__organisations()
