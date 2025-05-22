import copy
import re

from hdx.api.utilities.filestore_helper import FilestoreHelper
from hdx.data.resource import Resource

resource_data = {
    "name": "MyResource1",
    "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
    "format": "xlsx",
    "url": "http://test/spreadsheet.xlsx",
    "description": "My Resource",
    "api_type": "api",
    "resource_type": "api",
}


class TestFilestoreHelper:
    def test_dataset_update_filestore_resource(self, configuration):
        resource_data_copy = copy.deepcopy(resource_data)
        resource = Resource(resource_data_copy)
        filestore_resources = {}
        FilestoreHelper.dataset_update_filestore_resource(
            resource, filestore_resources, 0
        )
        assert resource == {
            "api_type": "api",
            "description": "My Resource",
            "format": "xlsx",
            "name": "MyResource1",
            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
            "resource_type": "api",
            "url": "http://test/spreadsheet.xlsx",
        }
        assert filestore_resources == {}

        resource.set_file_to_upload("test")
        FilestoreHelper.dataset_update_filestore_resource(
            resource, filestore_resources, 0
        )
        assert resource == {
            "api_type": "api",
            "description": "My Resource",
            "format": "xlsx",
            "name": "MyResource1",
            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
            "resource_type": "api",
            "url": "updated_by_file_upload_step",
        }
        assert filestore_resources == {0: "test"}

        resource.mark_data_updated()
        FilestoreHelper.dataset_update_filestore_resource(
            resource, filestore_resources, 0
        )
        regex = r"^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d$"
        assert re.match(regex, resource["last_modified"])
        assert filestore_resources == {0: "test"}
