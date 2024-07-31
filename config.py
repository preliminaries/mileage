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

        # Temporary local areas
        self.warehouse = os.path.join(os.getcwd(), 'warehouse')
        self.raw_ = os.path.join(self.warehouse, 'raw')
        self.initial_ = os.path.join(self.warehouse, 'initial')
        self.points_ = os.path.join(self.warehouse, 'points')

        # A S3 parameters template
        self.s3_parameters_template = 'https://raw.githubusercontent.com/preliminaries/.github/master/profile/s3_parameters.yaml'

        self.s3_internal_prefix = 'mileage/'
