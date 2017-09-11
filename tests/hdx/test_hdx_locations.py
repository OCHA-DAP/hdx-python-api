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
        assert Locations.get_HDX_code_from_location('sh', locations=validlocations) is None
        assert Locations.get_HDX_code_from_location_partial('sh', locations=validlocations) == (None, False)
        assert Locations.get_location_from_HDX_code('shn', locations=validlocations) == 'St. Helena'
        validlocations = [{'name': 'zmb', 'title': 'Zambia'}, {'name': 'pry', 'title': 'Paraguay'}]
        Locations.set_validlocations(validlocations)
        assert Locations.validlocations() == validlocations
        assert Locations.get_HDX_code_from_location_partial('NOT') == (None, False)
        assert Locations.get_location_from_HDX_code('pr') is None
        assert Locations.get_HDX_code_from_location('zmb') == 'zmb'
        assert Locations.get_HDX_code_from_location_partial('zmb') == ('zmb', True)
        assert Locations.get_HDX_code_from_location('Z') is None
        assert Locations.get_HDX_code_from_location_partial('Z') == ('zmb', False)
        assert Locations.get_HDX_code_from_location_partial('Zambia') == ('zmb', True)
        assert Locations.get_HDX_code_from_location_partial('ZAM') == ('zmb', False)
        assert Locations.get_location_from_HDX_code('zmb', locations=validlocations) == 'Zambia'
        validlocations = [{'name': 'shn', 'title': 'St. Helena'}]
        assert Locations.get_HDX_code_from_location('sh', locations=validlocations) is None
        assert Locations.get_HDX_code_from_location_partial('sh', locations=validlocations) == (None, False)
        assert Locations.get_location_from_HDX_code('shn', locations=validlocations) == 'St. Helena'
        Configuration.setup(MyConfiguration())
        Locations.set_validlocations(None)
        assert Locations.get_HDX_code_from_location('zaf') == 'zaf'
        assert Locations.get_HDX_code_from_location_partial('zaf') == ('zaf', True)
        assert Locations.get_location_from_HDX_code('zaf') == 'South Africa'

