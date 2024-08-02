"""
Module interface.py
"""
import logging

import pandas as pd

import config
import src.data.dictionary
import src.data.organisations
import src.data.raw
import src.data.reading
import src.elements.s3_parameters as s3p
import src.elements.service as sr


class Interface:
    """
    Class Interface
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 (Simple Storage Service) parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

        # Configurations
        self.__configurations = config.Config()
        # self.__raw_ = configurations.raw_
        # self.__initial_ = configurations.initial_

        # Logging
        logging.basicConfig(level=logging.INFO, format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def exc(self):
        """

        :return:
        """

        # The organisations whence the mileage records were requested
        organisations: pd.DataFrame = src.data.organisations.Organisations(
            service=self.__service, s3_parameters=self.__s3_parameters).exc()
        self.__logger.info(organisations)

        # Unload
        messages: list[str] = src.data.raw.Raw(
            service=self.__service, s3_parameters=self.__s3_parameters, raw_=self.__configurations.raw_).exc()
        self.__logger.info(messages)

        # Separately read & save the spreadsheets; as CSV (comma separated values) files
        messages = src.data.reading.Reading(
            raw_=self.__configurations.raw_, initial_=self.__configurations.initial_).exc(organisations=organisations)
        self.__logger.info(type(messages))
        self.__logger.info(messages)

        # Transferring to Amazon S3
        strings: pd.DataFrame = src.data.dictionary.Dictionary().exc(
            path=self.__configurations.initial_, extension='csv', prefix=self.__configurations.s3_internal_prefix)
        self.__logger.info(strings)
