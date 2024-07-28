import os
import logging

import pandas as pd

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys


class Raw:

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__service = service
        self.__s3_parameters: s3p.S3Parameters = s3_parameters

        # Configurations
        self.__configurations = config.Config()

        # Logging
        logging.basicConfig(level=logging.INFO, format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def exc(self):

        prefix = self.__configurations.s3_prefix + 'raw/'

        keys = src.s3.keys.Keys(service=self.__service, bucket_name=self.__s3_parameters.internal)
        objects: list[str] = keys.excerpt(prefix=prefix)
        self.__logger.info(objects)

        frame = pd.DataFrame(data={'key': objects})
        frame = frame.assign(vertex=frame['key'].str.rsplit('/', n=1, expand=True)[1])
        self.__logger.info(frame)
