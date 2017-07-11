# -*'coding: UTF-8 -*-
"""HDX Location Tests"""
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations


class MyConfiguration:
    def call_remoteckan(self, a, b):
        return [{'name': 'zaf', 'title': 'South Africa'}]


class TestHDXLocations:
    def test_validlocations(self, project_config_yaml):
        validlocations = [{'name': 'shn', 'title': 'St. Helena'}]
        assert Locations.get_HDX_code_from_location('sh', exact=False, locations=validlocations) == (None, False)
        assert Locations.get_location_from_HDX_code('shn', locations=validlocations) == 'St. Helena'
        validlocations = [{'name': 'zmb', 'title': 'Zambia'}, {'name': 'pry', 'title': 'Paraguay'}]
        Locations.set_validlocations(validlocations)
        assert Locations.validlocations() == validlocations
        assert Locations.get_HDX_code_from_location('NOT') == (None, False)
        assert Locations.get_location_from_HDX_code('pr') is None
        assert Locations.get_HDX_code_from_location('zmb') == ('zmb', True)
        assert Locations.get_HDX_code_from_location('Z') == (None, False)
        assert Locations.get_HDX_code_from_location('Z', exact=False) == ('zmb', False)
        assert Locations.get_HDX_code_from_location('Zambia') == ('zmb', True)
        assert Locations.get_HDX_code_from_location('ZAM', exact=False) == ('zmb', False)
        assert Locations.get_location_from_HDX_code('zmb', locations=validlocations) == 'Zambia'
        validlocations = [{'name': 'shn', 'title': 'St. Helena'}]
        assert Locations.get_HDX_code_from_location('sh', exact=False, locations=validlocations) == (None, False)
        assert Locations.get_location_from_HDX_code('shn', locations=validlocations) == 'St. Helena'
        Configuration.setup(MyConfiguration())
        Locations.set_validlocations(None)
        assert Locations.get_HDX_code_from_location('zaf') == ('zaf', True)
        assert Locations.get_location_from_HDX_code('zaf') == 'South Africa'

