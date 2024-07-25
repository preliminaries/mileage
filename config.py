"""Module config.py"""
import os


class Config:
    """
    Configuration
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse = os.path.join(os.getcwd(), 'warehouse')

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/preliminaries/.github/master/profile/s3_parameters.yaml'
