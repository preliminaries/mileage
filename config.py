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

        # Spreadsheets & Fields
        self.parse_dates = ['Claim Line Start', 'Claim Line End']

        self.rename = {'Area Pay Division': 'area_pay_division', 'Claim Line Start': 'claim_line_start',
                       'Claim Line End': 'claim_line_end', 'Engine Size': 'engine_size',
                       'Fuel Type': 'fuel_type', 'CO2 Emissions': 'co2_emissions',
                       'Business Mileage': 'business_mileage', 'Business Rate High': 'business_rate_high',
                       'Business Rate Low': 'business_rate_low', 'Business Value': 'business_value',
                       'Commute Miles Not Undertaken': 'commute_miles_not_undertaken',
                       'Overtime Mileage': 'overtime_mileage', 'Journey Details': 'journey_details'}

        self.dtype = {'Area Pay Division': str, 'Engine Size': float,
                      'Fuel Type': str, 'CO2 Emissions': float,
                      'Business Mileage': float, 'Business Rate High': float,
                      'Business Rate Low': float, 'Business Value': float,
                      'Commute Miles Not Undertaken': float,
                      'Overtime Mileage': float, 'Journey Details': str}
