"""
Module interface.py
"""
import logging
import os
import glob
from typing import Any, List

import pandas as pd
import numpy as np

import config
import src.data.raw
import src.data.organisations
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.data.reading



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
        configurations = config.Config()
        self.__raw_ = configurations.raw_
        self.__initial_ = configurations.initial_

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
            service=self.__service, s3_parameters=self.__s3_parameters, raw_=self.__raw_).exc()
        self.__logger.info(messages)

        # Separately read & save spreadsheets
        src.data.reading.Reading(
            raw_=self.__raw_, initial_=self.__initial_).exc(organisations=organisations)
