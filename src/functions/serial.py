"""
Module serial.py
"""
import yaml
import requests


class Serial:
    """
    Class Serial

    Description
    -----------
    Present, this class reads-in local YAML data files; YAML is a data serialisation language.
    """

    def __init__(self):
        """
        Constructor
        """

    @staticmethod
    def api(url: str) -> dict:
        """

        :param url:
        :return:
        """

        try:
            response = requests.get(url=url, timeout=600)
            response.raise_for_status()
        except requests.exceptions.Timeout as err:
            raise err from err
        except Exception as err:
            raise err from err

        if response.status_code == 200:
            content = response.content.decode(encoding='utf-8')
            return yaml.safe_load(content)
        raise f'Failure code: {response.status_code}'

    @staticmethod
    def read(uri: str) -> dict:
        """

        :param uri: The file string of a local YAML file; path + file name + extension.
        :return:
        """

        with open(file=uri, mode='r', encoding='utf-8') as stream:
            try:
                return yaml.load(stream=stream, Loader=yaml.CLoader)
            except yaml.YAMLError as err:
                raise err from err
