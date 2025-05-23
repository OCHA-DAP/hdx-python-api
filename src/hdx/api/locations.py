"""Locations in HDX"""

from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.utilities.typehint import ListTuple


class Locations:
    """Methods to help with countries and continents"""

    _validlocations = None

    @classmethod
    def validlocations(cls, configuration=None) -> List[Dict]:
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
            cls._validlocations = configuration.call_remoteckan(
                "group_list", {"all_fields": True}
            )
        return cls._validlocations

    @classmethod
    def set_validlocations(cls, locations: ListTuple[Dict]) -> None:
        """
        Set valid locations using list of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Args:
            locations (ListTuple[Dict]): List of dictionaries of form {'name': 'zmb', 'title', 'Zambia'}

        Returns:
            None
        """
        cls._validlocations = locations

    @classmethod
    def get_location_from_HDX_code(
        cls,
        code: str,
        locations: Optional[ListTuple[Dict]] = None,
        configuration: Optional[Configuration] = None,
    ) -> Optional[str]:
        """Get location from HDX location code

        Args:
            code (str): code for which to get location name
            locations (Optional[ListTuple[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[str]: location name or None
        """
        if locations is None:
            locations = cls.validlocations(configuration)
        code = code.upper()
        for locdict in locations:
            if code == locdict["name"].upper():
                return locdict["title"]
        return None

    @classmethod
    def get_HDX_code_from_location(
        cls,
        location: str,
        locations: Optional[ListTuple[Dict]] = None,
        configuration: Optional[Configuration] = None,
    ) -> Optional[str]:
        """Get HDX code for location

        Args:
            location (str): Location for which to get HDX code
            locations (Optional[ListTuple[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[str]: HDX code or None
        """
        if locations is None:
            locations = cls.validlocations(configuration)
        locationupper = location.upper()
        for locdict in locations:
            locationcode = locdict["name"].upper()
            if locationupper == locationcode:
                return locationcode

        for locdict in locations:
            if locationupper == locdict["title"].upper():
                return locdict["name"].upper()
        return None

    @classmethod
    def get_HDX_code_from_location_partial(
        cls,
        location: str,
        locations: Optional[ListTuple[Dict]] = None,
        configuration: Optional[Configuration] = None,
    ) -> Tuple[Optional[str], bool]:
        """Get HDX code for location

        Args:
            location (str): Location for which to get HDX code
            locations (Optional[ListTuple[Dict]]): Valid locations list. Defaults to list downloaded from HDX.
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
            locationname = locdict["title"].upper()
            if locationupper in locationname or locationname in locationupper:
                return locdict["name"].upper(), False

        return None, False
