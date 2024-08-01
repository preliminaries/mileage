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
        self.__storage = config.Config().raw_

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
        raw: list[str] = src.data.raw.Raw(
            service=self.__service, s3_parameters=self.__s3_parameters, storage=self.__storage).exc()
        self.__logger.info(raw)

        tabs: list[dict] = organisations[['organisation_id', 'mileage_tab']].to_dict(orient='records')
        for tab in tabs:
            self.__logger.info(tab['organisation_id'])

        file = glob.glob(pathname=os.path.join(self.__storage, '*.xlsx'))[0]
        src.data.reading.Reading(file=file).exc(tabs=tabs)
