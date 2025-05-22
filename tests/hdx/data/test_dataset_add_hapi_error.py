import copy
import json

import pytest

from .. import MockResponse, dataset_data, dataset_resultdict, resources_data
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset


class TestDatasetAddHAPIError:
    @pytest.fixture(scope="function")
    def hapi_resource_update(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "show" in url:
                    resource_id = datadict["id"]
                    resources_json = dataset_resultdict["resources"]
                    if resource_id == "de6549d8-268b-4dfe-adaf-a4ae5c8510d5":
                        resource_json = resources_json[0]
                    elif resource_id == "3d777226-96aa-4239-860a-703389d16d1f":
                        resource_json = resources_json[1]
                    elif resource_id == "3d777226-96aa-4239-860a-703389d16d1g":
                        resource_json = resources_json[2]
                    else:
                        return MockResponse(
                            404,
                            '{"success": false, "error": {"message": "TEST ERROR: Invalid id", "__type": "TEST ERROR: Invalid id Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_patch"}',
                        )
                    result = json.dumps(resource_json)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}'
                        % result,
                    )
                if "patch" not in url:
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not patch", "__type": "TEST ERROR: Not Patch Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_patch"}',
                    )
                result = json.dumps(datadict)
                return MockResponse(
                    200,
                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_patch"}'
                    % result,
                )

        Configuration.read().remoteckan().session = MockSession()

    def test_add_hapi_error(self, configuration, hapi_resource_update):
        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        datasetdata["resources"] = resourcesdata
        dataset = Dataset(datasetdata)
        success = dataset.add_hapi_error(
            error_message="test message",
            resource_name="Resource1",
        )
        assert success is True
        success = dataset.add_hapi_error(
            error_message="test message",
            resource_id="de6549d8-268b-4dfe-adaf-a4ae5c8510d5",
        )
        assert success is False
        success = dataset.add_hapi_error(
            error_message="test message 1",
            resource_name="Resource1",
        )
        assert success is True
