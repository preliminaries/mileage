"""Module s3_parameters.py"""
import config
import src.elements.s3_parameters as s3p
import src.functions.secret
import src.functions.serial


class S3Parameters:
    """
    Class S3Parameters

    Description
    -----------

    This class reads-in the YAML file of this project repository's overarching Amazon S3 (Simple Storage Service)
    parameters.

    S3 Express One Zone, which has 4 overarching regions
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-Regions-and-Zones.html
    """

    def __init__(self):
        """
        Constructor
        """

        # Hence
        self.__url = config.Config().s3_parameters_template
        self.__secret = src.functions.secret.Secret()

    def __get_dictionary(self) -> dict:
        """

        :return:
            A dictionary, or excerpt dictionary, of YAML file contents
        """

        blob = src.functions.serial.Serial().api(url=self.__url)

        return blob['parameters']

    def __build_collection(self, dictionary: dict) -> s3p.S3Parameters:
        """

        :param dictionary:
        :return:
            A re-structured form of the parameters.
        """

        s3_parameters = s3p.S3Parameters(**dictionary)

        # Parsing variables
        region_name = self.__secret.exc(secret_id='RegionCodeDefault')
        internal = self.__secret.exc(secret_id='SustainabilityProject', node='internal')
        external = self.__secret.exc(secret_id='SustainabilityProject', node='external')

        s3_parameters: s3p.S3Parameters = s3_parameters._replace(
            location_constraint=region_name, region_name=region_name,
            internal=internal, external=external)

        return s3_parameters

    def exc(self) -> s3p.S3Parameters:
        """

        :return:
            The re-structured form of the parameters.
        """

        dictionary = self.__get_dictionary()

        return self.__build_collection(dictionary=dictionary)
