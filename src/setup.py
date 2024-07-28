"""Module setup.py"""
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.directories
import src.s3.bucket
import src.s3.keys
import src.s3.objects


class Setup:
    """
    Description
    -----------

    Sets up local & cloud environments.  Prepares an Amazon S3 (Simple Storage Service)
    bucket area.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, warehouse: str):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param warehouse: The temporary local directory where data sets are initially placed,
                          prior to transfer to Amazon S3 (Simple Storage Service)
        """

        self.__service: sr.Service = service
        self.__s3_parameters: s3p.S3Parameters = s3_parameters
        self.__warehouse = warehouse

    def __s3_prefix(self, bucket_name: str, prefix: str) -> bool:
        """

        :param bucket_name:  An Amazon S3 bucket name.
        :param prefix: An Amazon S3 prefix; if not None, all file objects within the prefix area are dropped.
        :return:
        """

        keys = src.s3.keys.Keys(service=self.__service, bucket_name=self.__s3_parameters.external
                                ).excerpt(prefix=prefix)

        objects = src.s3.objects.Objects(service=self.__service,
                                         location_constraint=self.__s3_parameters.location_constraint,
                                         bucket_name=bucket_name)

        return objects.drop(keys=keys) if keys else True

    def __s3(self, bucket_name: str) -> bool:
        """

        :param bucket_name:  An Amazon S3 bucket name.  If the bucket does not
                             exist, it is created.
        :return:
        """

        # An instance for interacting with Amazon S3 buckets. If the bucket does not exist, it's created
        bucket = src.s3.bucket.Bucket(service=self.__service,
                                      location_constraint=self.__s3_parameters.location_constraint,
                                      bucket_name=bucket_name)

        if not bucket.exists():
            return bucket.create()

        return True

    def __local(self) -> bool:
        """
        Prepares the local warehouse, i.e., local depository.

        :return:
        """

        directories = src.functions.directories.Directories()
        directories.cleanup(path=self.__warehouse)

        return directories.create(path=self.__warehouse)

    def exc(self, bucket_name: str, prefix: str = None) -> bool:
        """

        :param bucket_name:  An Amazon S3 bucket name.  If the bucket does not exist, it is created.
        :param prefix: An Amazon S3 prefix; if not None, all file objects within the prefix will be dropped.
        :return:
        """

        local = self.__local()
        s3 = self.__s3(bucket_name=bucket_name)
        s3_prefix = self.__s3_prefix(bucket_name=bucket_name, prefix=prefix) if prefix else True

        return local & s3 & s3_prefix
