# -*- coding: utf-8 -*-
"""Location utilities"""
from typing import List, Callable, Tuple, Optional

import geonamescache


class Location(object):
    """Methods to help with countries and continents
    """

    gc = geonamescache.GeonamesCache()
    continents = gc.get_continents()
    countries = gc.get_countries().values()

    @staticmethod
    def get_country_name_from_iso3(iso3):
        # type: (str) -> Optional[str]
        """Get country name from iso3 code

        Args:
            iso3 (str): Iso 3 code for which to get country name

        Returns:
            Optional[str]: country name
        """
        iso3lower = iso3.lower()
        for countrydetails in Location.countries:
            if iso3lower == countrydetails.get('iso3').lower():
                return countrydetails.get('name')
        return None

    @staticmethod
    def get_iso3_country_code(country):
        # type: (str) -> Tuple[Optional[str], bool]
        """Get iso 3 code for country

        Args:
            country (str): Country for which to get iso 3 code

        Returns:
            Tuple[Optional[str], bool]: iso 3 country code and if the match is strong or (None, False) for no match
        """
        countrylower = country.lower()
        if len(countrylower) == 3:
            for countrydetails in Location.countries:
                if countrylower == countrydetails.get('iso3').lower():
                    return countrylower, True

        for countrydetails in Location.countries:
            if countrylower == countrydetails.get('name').lower():
                return countrydetails.get('iso3').lower(), True

        for countrydetails in Location.countries:
            countryname = countrydetails.get('name').lower()
            if countrylower in countryname or countryname in countrylower:
                return countrydetails.get('iso3').lower(), False
        return None, False

    @staticmethod
    def get_countries_in_continent(continent, function=lambda x: x):
        # type: (str, Callable[[str], Any]) -> List[str]
        """Get countries (iso 3 codes) in continent

        Args:
            continent (str): Two letter continent code or continent name
            function (Callable[[str], Any]: Format of each list element. Defaults to str (iso 3 country code).

        Returns:
            List(str): List of iso 3 country names
        """
        continentcode = None
        for code in Location.continents:
            if continent.upper() == code:
                continentcode = code
                break
            continentdetails = Location.continents[code]
            if continent.lower() == continentdetails['name'].lower():
                continentcode = code
                break
        countries = list()  # type: List[str]
        if continentcode is None:
            return countries
        for country in Location.countries:
            if country.get('continentcode') == continentcode:
                countries.append(function(country.get('iso3').lower()))
        return sorted(countries)
