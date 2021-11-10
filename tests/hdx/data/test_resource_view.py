"""Resource view Tests"""
import copy
import json
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.resource_view import ResourceView

from . import MockResponse

hxl_preview_config = '{"configVersion":2,"bites":[{"init":true,"type":"key figure","filteredValues":[],"errorMsg":null,"ingredient":{"aggregateColumn":null,"valueColumn":"#affected+killed","aggregateFunction":"sum"},"dataTitle":"#affected+killed","displayCategory":"Key Figures","unit":null,"hashCode":-1955043658,"title":"Sum of fatalities","value":null},{"init":true,"type":"chart","filteredValues":[],"errorMsg":null,"swapAxis":true,"showGrid":true,"pieChart":false,"ingredient":{"aggregateColumn":"#adm1+name","valueColumn":"#affected+killed","aggregateFunction":"sum"},"dataTitle":"#affected+killed","displayCategory":"Charts","hashCode":738289179,"title":"Sum of fatalities grouped by admin1","values":null,"categories":null},{"init":true,"type":"chart","filteredValues":[],"errorMsg":null,"swapAxis":true,"showGrid":true,"pieChart":false,"ingredient":{"aggregateColumn":"#adm2+name","valueColumn":"#affected+killed","aggregateFunction":"sum"},"dataTitle":"#affected+killed","displayCategory":"Charts","hashCode":766918330,"title":"Sum of fatalities grouped by admin2","values":null,"categories":null}]}'

resource_view_list = [
    {
        "description": "",
        "resource_id": "25982d1c-f45a-45e1-b14e-87d367413045",
        "view_type": "recline_view",
        "title": "Data Explorer",
        "package_id": "53f4375e-8872-4bcd-9746-c0fda941dadb",
        "id": "d80301b5-4abd-49bd-bf94-fa4af7b6e7a4",
    },
    {
        "description": "",
        "resource_id": "25982d1c-f45a-45e1-b14e-87d367413045",
        "hxl_preview_config": hxl_preview_config,
        "view_type": "hdx_hxl_preview",
        "title": "Quick Charts",
        "package_id": "53f4375e-8872-4bcd-9746-c0fda941dadb",
        "id": "c06b5a0d-1d41-4a74-a196-41c251c76023",
    },
]

resultdict = resource_view_list[1]


def resource_view_mockshow(url, datadict):
    if "show" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}',
        )
    result = json.dumps(resultdict)
    if (
        datadict["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        or datadict["id"] == "MyResourceView1"
    ):
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}'
            % result,
        )
    if datadict["id"] == "TEST2":
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}',
        )
    if datadict["id"] == "TEST3":
        return MockResponse(
            200,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}',
        )
    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}',
    )


def resource_view_mocklist(url, datadict):
    if "list" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_list"}',
        )
    if datadict["id"] == "25982d1c-f45a-45e1-b14e-87d367413045":
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_list"}'
            % json.dumps(resource_view_list),
        )
    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_list"}',
    )


def resource_view_mockcreate(url, datadict):
    if "create" not in url:
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
        )
    if datadict["title"] == "A Preview":
        result = json.dumps(resultdict)
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}'
            % result,
        )
    if datadict["title"] == "Quick Charts":
        resultdictcopy = copy.deepcopy(resultdict)
        result = json.dumps(merge_two_dictionaries(resultdictcopy, datadict))
        return MockResponse(
            200,
            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}'
            % result,
        )
    if datadict["title"] == "XXX":
        return MockResponse(
            404,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
        )
    if datadict["title"] == "YYY":
        return MockResponse(
            200,
            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
        )

    return MockResponse(
        404,
        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
    )


class TestResourceView:
    resource_view_data = {
        "resource_id": "25982d1c-f45a-45e1-b14e-87d367413045",
        "title": "Data Explorer",
        "view_type": "recline_view",
    }

    @pytest.fixture(scope="class")
    def static_yaml(self):
        return join(
            "tests", "fixtures", "config", "hdx_resource_view_static.yml"
        )

    @pytest.fixture(scope="class")
    def static_json(self):
        return join(
            "tests", "fixtures", "config", "hdx_resource_view_static.json"
        )

    @pytest.fixture(scope="function")
    def read(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return resource_view_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_create(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return resource_view_mockshow(url, datadict)
                if "list" in url:
                    return resource_view_mocklist(url, datadict)
                if "create" in url:
                    return resource_view_mockcreate(url, datadict)
                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_update(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    return resource_view_mockshow(url, datadict)
                if "list" in url:
                    return resource_view_mocklist(url, datadict)
                if "update" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_update"}',
                    )
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict["title"] == "Quick Charts":
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_update"}'
                        % result,
                    )
                if datadict["title"] == "XXX":
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_update"}',
                    )
                if datadict["title"] == "YYY":
                    return MockResponse(
                        200,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_update"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_update"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_delete(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode("utf-8")
                datadict = json.loads(decodedata)
                if "show" in url:
                    return resource_view_mockshow(url, datadict)
                if "delete" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_delete"}',
                    )
                if datadict["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023":
                    return MockResponse(
                        200,
                        '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_delete"}',
                    )

                return MockResponse(
                    404,
                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_delete"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def post_list(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return resource_view_mocklist(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read):
        resource_view = ResourceView.read_from_hdx(
            "c06b5a0d-1d41-4a74-a196-41c251c76023"
        )
        assert resource_view["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert resource_view["title"] == "Quick Charts"
        resource_view = ResourceView.read_from_hdx("TEST2")
        assert resource_view is None
        resource_view = ResourceView.read_from_hdx("TEST3")
        assert resource_view is None

    def test_create_in_hdx(self, configuration, post_create):
        resource_view = ResourceView()
        with pytest.raises(HDXError):
            resource_view.create_in_hdx()
        resource_view["title"] = "Data Explorer"
        with pytest.raises(HDXError):
            resource_view.create_in_hdx()

        data = copy.deepcopy(self.resource_view_data)
        resource_view = ResourceView(data)
        resource_view["title"] = "A Preview"
        resource_view.create_in_hdx()
        assert resource_view["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert resource_view["view_type"] == "hdx_hxl_preview"
        assert "state" not in resource_view

        data["title"] = "XXX"
        resource_view = ResourceView(data)
        with pytest.raises(HDXError):
            resource_view.create_in_hdx()

        data["title"] = "YYY"
        resource_view = ResourceView(data)
        with pytest.raises(HDXError):
            resource_view.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        resource_view = ResourceView()
        resource_view["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            resource_view.update_in_hdx()
        resource_view["id"] = "LALA"
        with pytest.raises(HDXError):
            resource_view.update_in_hdx()

        resource_view = ResourceView.read_from_hdx(
            "c06b5a0d-1d41-4a74-a196-41c251c76023"
        )
        assert resource_view["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert resource_view["view_type"] == "hdx_hxl_preview"

        resource_view["id"] = "c06b5a0d-1d41-4a74-a196-41c251c76023"
        resource_view["view_type"] = "recline_view"
        resource_view["resource_id"] = "LALA"
        resource_view.update_in_hdx()
        assert resource_view["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert resource_view["view_type"] == "recline_view"
        assert resource_view["resource_id"] == "LALA"
        assert "state" not in resource_view

        resource_view["id"] = "NOTEXIST"
        with pytest.raises(HDXError):
            resource_view.update_in_hdx()

        resource_view["view_type"] = "hdx_hxl_preview"
        resource_view["resource_id"] = "25982d1c-f45a-45e1-b14e-87d367413045"
        resource_view.update_in_hdx()
        assert resource_view["id"] == "NOTEXIST"
        assert resource_view["view_type"] == "hdx_hxl_preview"
        assert (
            resource_view["resource_id"]
            == "25982d1c-f45a-45e1-b14e-87d367413045"
        )

        del resource_view["id"]
        resource_view["title"] = "NOTEXIST"
        with pytest.raises(HDXError):
            resource_view.update_in_hdx()

        data = copy.deepcopy(self.resource_view_data)
        data["id"] = "c06b5a0d-1d41-4a74-a196-41c251c76023"
        data["title"] = "Quick Charts"
        data["description"] = "Custom chart X"
        resource_view = ResourceView(data)
        resource_view.create_in_hdx()
        assert resource_view["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert resource_view["description"] == "Custom chart X"
        assert resource_view["view_type"] == "recline_view"
        assert "state" not in resource_view

    def test_delete_from_hdx(self, configuration, post_delete):
        resource_view = ResourceView.read_from_hdx(
            "c06b5a0d-1d41-4a74-a196-41c251c76023"
        )
        resource_view.delete_from_hdx()
        resource_view = ResourceView.read_from_hdx(
            "c06b5a0d-1d41-4a74-a196-41c251c76023"
        )
        del resource_view["id"]
        with pytest.raises(HDXError):
            resource_view.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        data = copy.deepcopy(self.resource_view_data)
        resource_view = ResourceView(data)
        assert resource_view["view_type"] == "recline_view"
        assert resource_view["title"] == "Data Explorer"
        resource_view.update_from_yaml(static_yaml)
        assert resource_view["view_type"] == "hdx_hxl_preview"
        assert resource_view["title"] == "Quick Charts"
        assert resource_view["description"] == "lala"
        assert (
            resource_view["resource_id"]
            == "25982d1c-f45a-45e1-b14e-87d367413045"
        )

    def test_update_json(self, configuration, static_json):
        data = copy.deepcopy(self.resource_view_data)
        resource_view = ResourceView(data)
        assert resource_view["view_type"] == "recline_view"
        assert resource_view["title"] == "Data Explorer"
        resource_view["view_type"] = "hdx_hxl_preview"
        resource_view.update_from_json(static_json)
        assert resource_view["view_type"] == "recline_view"
        assert resource_view["title"] == "Data Explorer"
        assert resource_view["description"] == "haha"
        assert (
            resource_view["resource_id"]
            == "25982d1c-f45a-45e1-b14e-87d367413045"
        )

    def test_copy(self, configuration, read):
        data = copy.deepcopy(self.resource_view_data)
        resource_view = ResourceView(data)
        resource_view.copy(resultdict)
        assert (
            resource_view["resource_id"]
            == self.resource_view_data["resource_id"]
        )
        assert resource_view["view_type"] == "hdx_hxl_preview"
        assert resource_view["hxl_preview_config"] == hxl_preview_config
        data = copy.deepcopy(self.resource_view_data)
        resource_view = ResourceView(data)
        resource_view.copy("c06b5a0d-1d41-4a74-a196-41c251c76023")
        assert (
            resource_view["resource_id"]
            == self.resource_view_data["resource_id"]
        )
        assert resource_view["view_type"] == "hdx_hxl_preview"
        assert resource_view["hxl_preview_config"] == hxl_preview_config
        with pytest.raises(HDXError):
            resource_view.copy("123")
        with pytest.raises(HDXError):
            resource_view.copy(5)

    def test_get_all_for_resource(self, configuration, post_list):
        resource_views = ResourceView.get_all_for_resource(
            "25982d1c-f45a-45e1-b14e-87d367413045"
        )
        assert (
            resource_views[0]["id"] == "d80301b5-4abd-49bd-bf94-fa4af7b6e7a4"
        )
        assert (
            resource_views[1]["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        )
