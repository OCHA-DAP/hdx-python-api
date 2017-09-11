# -*- coding: UTF-8 -*-
"""Location Tests"""
import pytest

from hdx.utilities.location import Location


class TestLocation:
    def test_get_country_name_from_iso3(self):
        assert Location.get_country_name_from_iso3('jpn') == 'Japan'
        assert Location.get_country_name_from_iso3('awe') is None
        assert Location.get_country_name_from_iso3('Pol') == 'Poland'
        assert Location.get_country_name_from_iso3('SGP') == 'Singapore'
        assert Location.get_country_name_from_iso3('uy') is None

    def test_get_iso3_country_code(self):
        assert Location.get_iso3_country_code('jpn') == 'jpn'
        assert Location.get_iso3_country_code_partial('jpn') == ('jpn', True)
        assert Location.get_iso3_country_code_partial('ZWE') == ('zwe', True)
        assert Location.get_iso3_country_code_partial('Vut') == ('vut', True)
        assert Location.get_iso3_country_code('abc') is None
        assert Location.get_iso3_country_code_partial('abc') == (None, False)
        assert Location.get_iso3_country_code_partial('United Kingdom') == ('gbr', True)
        assert Location.get_iso3_country_code_partial('united states') == ('usa', True)
        assert Location.get_iso3_country_code('UZBEKISTAN') == 'uzb'
        assert Location.get_iso3_country_code_partial('UZBEKISTAN') == ('uzb', True)
        assert Location.get_iso3_country_code('Sierra') is None
        assert Location.get_iso3_country_code_partial('Sierra') == ('sle', False)
        assert Location.get_iso3_country_code('Venezuela (Bolivarian Republic of)') is None
        assert Location.get_iso3_country_code_partial('Venezuela (Bolivarian Republic of)') == ('ven', False)
        with pytest.raises(ValueError):
            Location.get_iso3_country_code('abc', exception=ValueError)
        with pytest.raises(ValueError):
            Location.get_iso3_country_code_partial('abc', exception=ValueError)

    def test_get_countries_in_continent(self):
        assert len(Location.get_countries_in_continent('AF')) == 58
        assert len(Location.get_countries_in_continent('eu')) == 54
        assert len(Location.get_countries_in_continent('As')) == 52
        assert len(Location.get_countries_in_continent('ab')) == 0
        assert len(Location.get_countries_in_continent('North America')) == 42
        assert len(Location.get_countries_in_continent('North America')) == 42
