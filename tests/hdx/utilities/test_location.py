# -*- coding: UTF-8 -*-
"""Location Tests"""

from hdx.utilities.location import Location


class TestLocation:
    def test_get_country_name_from_iso3(self):
        assert Location.get_country_name_from_iso3('jpn') == 'Japan'
        assert Location.get_country_name_from_iso3('awe') is None
        assert Location.get_country_name_from_iso3('Pol') == 'Poland'
        assert Location.get_country_name_from_iso3('SGP') == 'Singapore'
        assert Location.get_country_name_from_iso3('uy') is None

    def test_get_iso3_country_code(self):
        assert Location.get_iso3_country_code('jpn') == ('jpn', True)
        assert Location.get_iso3_country_code('ZWE') == ('zwe', True)
        assert Location.get_iso3_country_code('Vut') == ('vut', True)
        assert Location.get_iso3_country_code('abc') == (None, False)
        assert Location.get_iso3_country_code('United Kingdom') == ('gbr', True)
        assert Location.get_iso3_country_code('united states') == ('usa', True)
        assert Location.get_iso3_country_code('UZBEKISTAN') == ('uzb', True)
        assert Location.get_iso3_country_code('Sierra') == ('sle', False)
        assert Location.get_iso3_country_code('Venezuela (Bolivarian Republic of)') == ('ven', False)

    def test_get_countries_in_continent(self):
        assert len(Location.get_countries_in_continent('AF')) == 58
        assert len(Location.get_countries_in_continent('eu')) == 54
        assert len(Location.get_countries_in_continent('As')) == 52
        assert len(Location.get_countries_in_continent('ab')) == 0
        assert len(Location.get_countries_in_continent('North America')) == 42
        assert len(Location.get_countries_in_continent('North America')) == 42
