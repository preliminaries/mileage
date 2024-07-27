"""
Module unload.py
"""
import io

import botocore.exceptions

import src.elements.service as sr


class Unload:
    """
    Unloads from Amazon S3 (Simple Storage Service).
    """

    def __init__(self, service: sr.Service):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        """

        self.__s3_client = service.s3_client

    def exc(self, bucket_name: str, key_name: str):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client-exceptions

        :param bucket_name:
        :param key_name: The S3 path of the data file, excluding the bucket name, including the file name.
        :return:
        """

        try:
            blob = self.__s3_client.get_object(Bucket=bucket_name, Key=key_name)
        except self.__s3_client.exceptions.NoSuchKey as err:
            raise f'The key {key_name} does not exist.\n{err}'
        except self.__s3_client.exceptions.InvalidObjectState as err:
            raise err.response
        except botocore.exceptions.ClientError as err:
            raise err.response

        buffer = io.StringIO(blob['Body'].read().decode('utf-8'))

        return buffer
