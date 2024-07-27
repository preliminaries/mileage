import logging
import boto3
import botocore.exceptions

import src.elements.service as sr


class Objects:

    def __init__(self, service: sr.Service, location_constraint: str, bucket_name: str):
        """
        Constructor

        :param service: A suite of services for interacting with Amazon Web Services.
        :param location_constraint: The location constraint of an Amazon S3 (Simple Storage Service) bucket.
        :param bucket_name: The name of an Amazon S3 bucket in focus.
        """

        self.__s3_resource: boto3.session.Session.resource = service.s3_resource

        self.__location_constraint = location_constraint
        self.__bucket_name = bucket_name

        # A bucket instance
        self.__bucket = self.__s3_resource.Bucket(name=self.__bucket_name)

        # Logging
        logging.basicConfig(level=logging.INFO,
                            format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.__logger = logging.getLogger(__name__)

    def drop(self, keys: list) -> bool:
        """

        :param keys: The list of Amazon S3 (simple storage service) keys to be deleted
        :return:
        """

        try:
            response = self.__bucket.delete_objects(
                Delete={'Objects': [{'Key': key} for key in keys]}
            )
        except botocore.exceptions.ClientError as err:
            raise err

        if 'Errors' in response:
            for node in response['Errors']:
                self.__logger.info('%s: %s', node['Key'], node['Code'])
            raise 'The listed keys could not be deleted.'

        return True