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
    "url_type": "api",
    "resource_type": "api",
}


class TestFilestoreHelper:
    def test_dataset_update_filestore_resource(self, configuration, test_data):
        resource_data_copy = copy.deepcopy(resource_data)
        orig_resource = Resource(resource_data_copy)
        resource = Resource(resource_data_copy)
        filestore_resources = {}
        status = FilestoreHelper.dataset_update_filestore_resource(
            orig_resource, resource, filestore_resources, 0
        )
        assert status == 1
        assert resource == {
            "description": "My Resource",
            "format": "xlsx",
            "name": "MyResource1",
            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
            "resource_type": "api",
            "url_type": "api",
            "url": "http://test/spreadsheet.xlsx",
        }
        assert filestore_resources == {}

        resource.set_file_to_upload(test_data)
        status = FilestoreHelper.dataset_update_filestore_resource(
            orig_resource, resource, filestore_resources, 0
        )
        assert status == 2
        assert resource == {
            "description": "My Resource",
            "format": "xlsx",
            "name": "MyResource1",
            "hash": "3790da698479326339fa99a074cbc1f7",
            "size": 1548,
            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
            "resource_type": "file.upload",
            "url_type": "upload",
            "url": "updated_by_file_upload_step",
        }
        assert filestore_resources == {0: test_data}

        filestore_resources = {}
        status = FilestoreHelper.dataset_update_filestore_resource(
            resource, resource, filestore_resources, 0, force_update=True
        )
        assert status == 2
        filestore_resources = {}
        status = FilestoreHelper.dataset_update_filestore_resource(
            resource, resource, filestore_resources, 0
        )
        assert status == 3
        assert resource == {
            "description": "My Resource",
            "format": "xlsx",
            "name": "MyResource1",
            "hash": "3790da698479326339fa99a074cbc1f7",
            "size": 1548,
            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
            "resource_type": "file.upload",
            "url_type": "upload",
            "url": "updated_by_file_upload_step",
        }
        assert filestore_resources == {}

        resource._file_to_upload = None
        resource.mark_data_updated()
        status = FilestoreHelper.dataset_update_filestore_resource(
            orig_resource, resource, filestore_resources, 0
        )
        assert status == 0
        regex = r"^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d$"
        assert re.match(regex, resource["last_modified"])
        assert filestore_resources == {}
