# -*- coding: utf-8 -*-
"""Locations in HDX"""
from typing import List, Tuple, Optional

from hdx.hdx_configuration import Configuration


class Locations(object):
    """Methods to help with countries and continents
    """
    _validlocations = None

    @staticmethod
    def validlocations(configuration=None):
        # type: () -> List[Dict]
        """
        Read valid locations from HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[Dict]: A list of valid locations
        """
        if Locations._validlocations is None:
            if configuration is None:
                configuration = Configuration.read()
            Locations._validlocations = configuration.call_remoteckan('group_list', {'all_fields': True})
        return Locations._validlocations

    @staticmethod
    def set_validlocations(locations):
        # type: (List[Dict]) -> None
        """
        Set valid locations using list of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Args:
            locations (List[Dict]): List of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Returns:
            None
        """
        Locations._validlocations = locations

    @staticmethod
    def get_location_from_HDX_code(code, locations=None, configuration=None):
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
            locations = Locations.validlocations(configuration)
        for locdict in locations:
            if code.lower() == locdict['name'].lower():
                return locdict['title']

    @staticmethod
    def get_HDX_code_from_location(location, exact=True, locations=None, configuration=None):
        # type: (str, Optional[bool], Optional[List[Dict]], Optional[Configuration]) -> Tuple[Optional[str], bool]
        """Get HDX code for location

        Args:
            location (str): Location for which to get HDX code
            exact (Optional[bool]): True for exact matching or False to allow fuzzy matching. Defaults to True.
            locations (Optional[List[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Tuple[Optional[str], bool]: HDX code and if the match is strong or (None, False) for no match
        """
        if locations is None:
            locations = Locations.validlocations(configuration)
        locationlower = location.lower()
        for locdict in locations:
            locationcode = locdict['name']
            if locationlower == locationcode.lower():
                return locationcode, True

        for locdict in locations:
            if locationlower == locdict['title'].lower():
                return locdict['name'], True

        if not exact:
            for locdict in locations:
                locationname = locdict['title'].lower()
                if locationlower in locationname or locationname in locationlower:
                    return locdict['name'], False
        return None, False
