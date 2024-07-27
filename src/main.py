"""Module main.py"""
import logging
import os
import sys


def main():
    """
    Entry point
    """

    # Logging
    logger: logging.Logger = logging.getLogger(name=__name__)

    # Setting Up
    setup = src.setup.Setup(service=service, s3_parameters=s3_parameters, warehouse=configurations.warehouse)
    state = setup.exc(bucket_name=s3_parameters.internal, prefix=configurations.s3_prefix)
    logger.info(state)

    # Hence
    src.data.interface.Interface(service=service, s3_parameters=s3_parameters).exc()

    # Deleting __pycache__
    src.functions.cache.Cache().exc()


if __name__ == '__main__':

    # Setting-up
    root: str = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    logging.captureWarnings(capture=True)

    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import config
    import src.data.interface
    import src.functions.cache
    import src.functions.service
    import src.s3.s3_parameters
    import src.setup

    configurations = config.Config()

    s3_parameters = src.s3.s3_parameters.S3Parameters().exc()
    service = src.functions.service.Service(region_name=s3_parameters.region_name).exc()

    main()
