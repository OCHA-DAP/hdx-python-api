#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Resource Tests"""
import json
from os.path import join

import pytest
import requests

from hdx.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


class TestResource():
    @pytest.fixture(scope="function")
    def return_resource(self, monkeypatch):
        def mockreturn(url, headers, auth):
            if 'TEST1' in url:
                return MockResponse(200,
                                    '{"success": true, "result": {"cache_last_updated": null, "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "webstore_last_updated": null, "datastore_active": false, "id": "de6549d8-268b-4dfe-adaf-a4ae5c8510d5", "size": null, "state": "active", "hash": "", "description": "ACLED-All-Africa-File_20160101-to-20160604.xlsx", "format": "XLSX", "tracking_summary": {"total": 0, "recent": 0}, "last_modified": null, "url_type": null, "mimetype": null, "cache_url": null, "name": "ACLED-All-Africa-File_20160101-to-date.xlsx", "created": "2016-06-07T08:57:27.367939", "url": "http://www.acleddata.com/wp-content/uploads/2016/06/ACLED-All-Africa-File_20160101-to-20160604.xlsx", "webstore_url": null, "mimetype_inner": null, "position": 0, "revision_id": "43765383-1fce-471f-8166-d6c8660cc8a9", "resource_type": null}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
            if 'TEST2' in url:
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
            if 'TEST3' in url:
                return MockResponse(404,
                                    '{"success": true, "result": {"cache_last_updated": null, "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "webstore_last_updated": null, "datastore_active": false, "id": "de6549d8-268b-4dfe-adaf-a4ae5c8510d5", "size": null, "state": "active", "hash": "", "description": "ACLED-All-Africa-File_20160101-to-20160604.xlsx", "format": "XLSX", "tracking_summary": {"total": 0, "recent": 0}, "last_modified": null, "url_type": null, "mimetype": null, "cache_url": null, "name": "ACLED-All-Africa-File_20160101-to-date.xlsx", "created": "2016-06-07T08:57:27.367939", "url": "http://www.acleddata.com/wp-content/uploads/2016/06/ACLED-All-Africa-File_20160101-to-20160604.xlsx", "webstore_url": null, "mimetype_inner": null, "position": 0, "revision_id": "43765383-1fce-471f-8166-d6c8660cc8a9", "resource_type": null}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
            if 'TEST4' in url:
                return MockResponse(200,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')

        monkeypatch.setattr(requests, 'get', mockreturn)

    @pytest.fixture(scope="class")
    def configuration(self):
        hdx_key_file = join('fixtures', '.hdxkey')
        collector_config_yaml = join('fixtures', 'config', 'collector_configuration.yml')
        return Configuration(hdx_key_file=hdx_key_file, collector_config_yaml=collector_config_yaml)

    def test_read_from_hdx(self, configuration, return_resource):
        resource = Resource.read_from_hdx(configuration, 'TEST1')
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource['name'] == 'ACLED-All-Africa-File_20160101-to-date.xlsx'
        assert resource['package_id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        resource = Resource.read_from_hdx(configuration, 'TEST2')
        assert resource is None
        with pytest.raises(HDXError):
            resource = Resource.read_from_hdx(configuration, 'TEST3')
        resource = Resource.read_from_hdx(configuration, 'TEST4')
        assert resource is None
