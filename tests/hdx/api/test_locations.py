"""HDX Location Tests"""
from hdx.location.country import Country

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations


class MyConfiguration:
    def call_remoteckan(self, a, b):
        return [{"name": "zaf", "title": "South Africa"}]


class TestHDXLocations:
    def test_validlocations(self, project_config_yaml):
        Country.countriesdata(use_live=False)
        validlocations = [{"name": "shn", "title": "St. Helena"}]
        assert (
            Locations.get_HDX_code_from_location(
                "sh", locations=validlocations
            )
            is None
        )
        assert Locations.get_HDX_code_from_location_partial(
            "sh", locations=validlocations
        ) == (None, False)
        assert (
            Locations.get_location_from_HDX_code(
                "shn", locations=validlocations
            )
            == "St. Helena"
        )
        validlocations = [
            {"name": "zmb", "title": "Zambia"},
            {"name": "pry", "title": "Paraguay"},
        ]
        Locations.set_validlocations(validlocations)
        assert Locations.validlocations() == validlocations
        assert Locations.get_HDX_code_from_location_partial("NOT") == (
            None,
            False,
        )
        assert Locations.get_location_from_HDX_code("pr") is None
        assert Locations.get_HDX_code_from_location("zmb") == "ZMB"
        assert Locations.get_HDX_code_from_location_partial("zmb") == (
            "ZMB",
            True,
        )
        assert Locations.get_HDX_code_from_location("Z") is None
        assert Locations.get_HDX_code_from_location_partial("Z") == (
            "ZMB",
            False,
        )
        assert Locations.get_HDX_code_from_location_partial("Zambia") == (
            "ZMB",
            True,
        )
        assert Locations.get_HDX_code_from_location_partial("ZAM") == (
            "ZMB",
            False,
        )
        assert (
            Locations.get_location_from_HDX_code(
                "zmb", locations=validlocations
            )
            == "Zambia"
        )
        validlocations = [{"name": "shn", "title": "St. Helena"}]
        assert (
            Locations.get_HDX_code_from_location(
                "sh", locations=validlocations
            )
            is None
        )
        assert Locations.get_HDX_code_from_location_partial(
            "sh", locations=validlocations
        ) == (None, False)
        assert (
            Locations.get_location_from_HDX_code(
                "shn", locations=validlocations
            )
            == "St. Helena"
        )
        Configuration.setup(MyConfiguration())
        Locations.set_validlocations(None)
        assert Locations.get_HDX_code_from_location("zaf") == "ZAF"
        assert Locations.get_HDX_code_from_location_partial("zaf") == (
            "ZAF",
            True,
        )
        assert Locations.get_location_from_HDX_code("zaf") == "South Africa"
