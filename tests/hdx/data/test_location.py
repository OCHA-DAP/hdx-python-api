"""Location Tests"""

import copy
import json
from pathlib import Path

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml

from .. import MockResponse, location_data
from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.location import Location

resultdict = load_yaml(
    Path("tests") / "fixtures" / "location" / "location_show_results.yaml"
)

location_list = [
    "afg",
    "bgd",
    "cod",
    "eth",
    "irq",
    "mli",
    "ner",
    "som",
    "ssd",
    "yem",
]
location_list_all_fields = load_yaml(
    Path("tests") / "fixtures" / "location" / "location_list_all_fields.yaml"
)

searchdict = load_yaml(Path("tests") / "fixtures" / "dataset_search_results.yaml")

location_autocomplete = [
    {
        "title": "Zambia",
        "id": "zmb",
        "name": "zmb",
    }
]


def location_mockshow(url, datadict):
    if "show" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_show"}',
        )
    result = json.dumps(resultdict)
    if (
        datadict["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        or datadict["id"] == "TEST1"
    ):
        return MockResponse(
            200,
            f'{{"success": true, "result": {result}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_show"}}',
        )
    if datadict["id"] == "TEST2":
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_show"}',
        )
    if datadict["id"] == "TEST3":
        return MockResponse(
            200,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_show"}',
        )
    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_show"}',
    )


def mocklist(url):
    if "list" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_list"}',
        )
    return MockResponse(
        200,
        f'{{"success": true, "result": {json.dumps(location_list)}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_list"}}',
    )


def mockgetdatasets(url, datadict):
    if "search" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not Search Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}',
        )
    if datadict["fq"] == "groups:gnq":
        return MockResponse(
            200,
            f'{{"success": true, "result": {json.dumps(searchdict)}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}}',
        )


class TestLocation:
    @pytest.fixture(scope="function")
    def read(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                return location_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_create(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return location_mockshow(url, datadict)
                if "create" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_create"}',
                    )

                resultdictcopy = copy.deepcopy(resultdict)
                resultdictcopy["state"] = datadict["state"]
                result = json.dumps(resultdictcopy)
                if datadict["name"] == "MyLocation1":
                    return MockResponse(
                        200,
                        f'{{"success": true, "result": {result}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_create"}}',
                    )
                if datadict["name"] == "MyLocation2":
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_create"}',
                    )
                if datadict["name"] == "MyLocation3":
                    return MockResponse(
                        200,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_create"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_create"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_update(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return location_mockshow(url, datadict)
                if "update" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_update"}',
                    )
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict["name"] == "MyLocation1":
                    return MockResponse(
                        200,
                        f'{{"success": true, "result": {result}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_update"}}',
                    )
                if datadict["name"] == "MyLocation2":
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_update"}',
                    )
                if datadict["name"] == "MyLocation3":
                    return MockResponse(
                        200,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_update"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_update"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_delete(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                decodedata = data.decode("utf-8")
                datadict = json.loads(decodedata)
                if "show" in url:
                    return location_mockshow(url, datadict)
                if "delete" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_delete"}',
                    )
                if datadict["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a":
                    return MockResponse(
                        200,
                        f'{{"success": true, "result": {decodedata}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_delete"}}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_delete"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_list(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                json.loads(data.decode("utf-8"))
                return mocklist(url)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_all_fields(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                kwargs = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return location_mockshow(url, kwargs)
                if kwargs["all_fields"]:
                    return MockResponse(
                        200,
                        f'{{"success": true, "result": {json.dumps(location_list_all_fields)}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_list"}}',
                    )
                names = [x["name"] for x in location_list_all_fields]
                names.append("TEST1")
                return MockResponse(
                    200,
                    f'{{"success": true, "result": {json.dumps(names)}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_list"}}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def datasets_get(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                return mockgetdatasets(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_autocomplete(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                decodedata = data.decode("utf-8")
                datadict = json.loads(decodedata)
                if "autocomplete" not in url or "zmb" not in datadict["q"]:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not autocomplete", "__type": "TEST ERROR: Not Autocomplete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_autocomplete"}',
                    )
                result = json.dumps(location_autocomplete)
                return MockResponse(
                    200,
                    f'{{"success": true, "result": {result}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=group_autocomplete"}}',
                )

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read, mocksmtp):
        location = Location.read_from_hdx("f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a")
        assert location["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        assert location["name"] == "gnq"
        location = Location.read_from_hdx("TEST2")
        assert location is None
        location = Location.read_from_hdx("TEST3")
        assert location is None

    def test_create_in_hdx(self, configuration, post_create):
        location = Location()
        with pytest.raises(HDXError):
            location.create_in_hdx()
        location["id"] = "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        location["name"] = "LALA"
        with pytest.raises(HDXError):
            location.create_in_hdx()

        loc_data = copy.deepcopy(location_data)
        location = Location(loc_data)
        location.create_in_hdx()
        assert location["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        assert location["state"] == "active"

        loc_data["name"] = "MyLocation2"
        location = Location(loc_data)
        with pytest.raises(HDXError):
            location.create_in_hdx()

        loc_data["name"] = "MyLocation3"
        location = Location(loc_data)
        with pytest.raises(HDXError):
            location.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        location = Location()
        location["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            location.update_in_hdx()
        location["name"] = "LALA"
        with pytest.raises(HDXError):
            location.update_in_hdx()

        location = Location.read_from_hdx("f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a")
        assert location["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        assert location["name"] == "gnq"

        location["title"] = "New Title"
        location["id"] = "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        location["name"] = "MyLocation1"
        location.update_in_hdx()
        assert location["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        assert location["title"] == "New Title"
        assert location["state"] == "active"

        location["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            location.update_in_hdx()

        del location["id"]
        with pytest.raises(HDXError):
            location.update_in_hdx()

        loc_data = copy.deepcopy(location_data)
        loc_data["name"] = "MyLocation1"
        loc_data["id"] = "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        location = Location(loc_data)
        location.create_in_hdx()
        assert location["id"] == "f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a"
        assert location["name"] == "MyLocation1"
        assert location["state"] == "active"

    def test_delete_from_hdx(self, configuration, post_delete):
        location = Location.read_from_hdx("f8b0e58f-9be5-4b25-9d5f-1c8b6d8b6a0a")
        location.delete_from_hdx()
        del location["id"]
        with pytest.raises(HDXError):
            location.delete_from_hdx()

    def test_get_all_location_names(self, configuration, post_list):
        locations = Location.get_all_location_names()
        assert len(locations) == 10

    def test_get_all_location_names_all_fields(self, configuration, post_all_fields):
        locations = Location.get_all_location_names(all_fields=True)
        assert len(locations) == 4

    def test_get_datasets(self, configuration, datasets_get):
        loc_data = copy.deepcopy(resultdict)
        location = Location(loc_data)
        datasets = location.get_datasets()
        assert len(datasets) == 10

    def test_autocomplete(self, configuration, post_autocomplete):
        assert Location.autocomplete("zmb") == location_autocomplete

    def test_get_data_grid_countries(self, configuration, post_all_fields):
        countries = Location.get_data_grid_countries()
        assert countries == ["afg", "gnq"]
