"""
This is data type S3Parameters
"""
import typing


class S3Parameters(typing.NamedTuple):
    """
    The data type class ⇾ S3Parameters

    Attributes
    ----------
    region_name : str
        The Amazon Web Services region code.

    location_constraint : str
        The region code of the region that the data is limited to.

    internal : str
        The Amazon S3 (Simple Storage Service) bucket that hosts this project's internally facing data.

    path_internal_raw: str
        The raw data's path

    path_internal_points: str
        After inspection & re-structuring data

    path_internal_references: str
        The references

    external: str
        The name of the bucket that host's externally facing data.

    path_external_points: str
        The investigation, modelling, exploration, etc., points.

    path_external_references: str
        The references
    """

    region_name: str
    location_constraint: str
    internal: str
    path_internal_raw: str
    path_internal_points: str
    path_internal_references: str
    external: str
    path_external_points: str
    path_external_references: str
