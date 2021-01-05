# -*- coding: utf-8 -*-
"""Locations in HDX"""
from typing import List, Tuple, Optional, Dict

from hdx.hdx_configuration import Configuration


class Locations(object):
    """Methods to help with countries and continents
    """
    _validlocations = None

    @classmethod
    def validlocations(cls, configuration=None):
        # type: () -> List[Dict]
        """
        Read valid locations from HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[Dict]: A list of valid locations
        """
        if cls._validlocations is None:
            if configuration is None:
                configuration = Configuration.read()
            cls._validlocations = configuration.call_remoteckan('group_list', {'all_fields': True})
        return cls._validlocations

    @classmethod
    def set_validlocations(cls, locations):
        # type: (List[Dict]) -> None
        """
        Set valid locations using list of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Args:
            locations (List[Dict]): List of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Returns:
            None
        """
        cls._validlocations = locations

    @classmethod
    def get_location_from_HDX_code(cls, code, locations=None, configuration=None):
        # type: (str, Optional[List[Dict]], Optional[Configuration]) -> Optional[str]
        """Get location from HDX location code

        Args:
            code (str): code for which to get location name
            locations (Optional[List[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[str]: location name
        """
        if locations is None:
            locations = cls.validlocations(configuration)
        for locdict in locations:
            if code.upper() == locdict['name'].upper():
                return locdict['title']
        return None

    @classmethod
    def get_HDX_code_from_location(cls, location, locations=None, configuration=None):
        # type: (str, Optional[List[Dict]], Optional[Configuration]) -> Optional[str]
        """Get HDX code for location

        Args:
            location (str): Location for which to get HDX code
            locations (Optional[List[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[str]: HDX code or None
        """
        if locations is None:
            locations = cls.validlocations(configuration)
        locationupper = location.upper()
        for locdict in locations:
            locationcode = locdict['name'].upper()
            if locationupper == locationcode:
                return locationcode

        for locdict in locations:
            if locationupper == locdict['title'].upper():
                return locdict['name'].upper()
        return None

    @classmethod
    def get_HDX_code_from_location_partial(cls, location, locations=None, configuration=None):
        # type: (str, Optional[List[Dict]], Optional[Configuration]) -> Tuple[Optional[str], bool]
        """Get HDX code for location

        Args:
            location (str): Location for which to get HDX code
            locations (Optional[List[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Tuple[Optional[str], bool]: HDX code and if the match is exact or (None, False) for no match
        """
        hdx_code = cls.get_HDX_code_from_location(location, locations, configuration)

        if hdx_code is not None:
            return hdx_code, True

        if locations is None:
            locations = cls.validlocations(configuration)
        locationupper = location.upper()
        for locdict in locations:
            locationname = locdict['title'].upper()
            if locationupper in locationname or locationname in locationupper:
                return locdict['name'].upper(), False

        return None, False
