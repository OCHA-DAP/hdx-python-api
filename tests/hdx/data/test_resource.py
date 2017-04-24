# -*- coding: UTF-8 -*-
"""Resource Tests"""
import copy
import json
import os
from os import unlink
from os.path import join

import pytest
import requests

from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.downloader import DownloadError


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


resultdict = {'cache_last_updated': None, 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
              'webstore_last_updated': None, 'datastore_active': None,
              'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'size': None, 'state': 'active',
              'hash': '', 'description': 'My Resource', 'format': 'XLSX', 'last_modified': None, 'url_type': 'api',
              'mimetype': None, 'cache_url': None, 'name': 'MyResource1', 'created': '2016-06-07T08:57:27.367939',
              'url': 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv',
              'webstore_url': None, 'mimetype_inner': None, 'position': 0,
              'revision_id': '43765383-1fce-471f-8166-d6c8660cc8a9', 'resource_type': 'api'}

searchdict = {'count': 4, 'results': [{'size': None, 'description': 'ACLED-All-Africa-File_20160101-to-20160903.xlsx',
                                       'revision_id': '796b639e-f217-4d0e-bedd-4e3b35da4461',
                                       'cache_last_updated': None,
                                       'created': '2016-06-22T12:41:02.857171',
                                       'name': 'ACLED-All-Africa-File_20160101-to-date.xlsx',
                                       'last_modified': None, 'url_type': 'api', 'format': 'XLSX', 'cache_url': None,
                                       'url': 'http://www.acleddata.com/wp-content/uploads/2016/09/ACLED.slxs',
                                       'state': 'active',
                                       'position': 0, 'revision_last_updated': '2016-06-28T14:17:27.150541',
                                       'webstore_last_updated': None,
                                       'mimetype': None, 'package_id': '45f53bde-544c-4a4a-9c6f-d609481b8719',
                                       'resource_type': 'api',
                                       'id': 'd6d1c367-1980-4dc7-ab20-c23f02d3b9e7', 'mimetype_inner': None, 'hash': '',
                                       'webstore_url': None},
                                      {'size': None, 'description': '',
                                       'revision_id': '5259be68-e72f-4c02-be8e-61d7cd594a9b',
                                       'cache_last_updated': None, 'created': '2016-03-23T14:17:02.272572',
                                       'name': 'ACLED-Version-6-All-Africa-1997-2015_csv_dyadic.zip',
                                       'last_modified': None,
                                       'url_type': 'api', 'format': 'ZIP', 'cache_url': None,
                                       'url': 'http://www.acleddata.com/wp-content/uploads/2016/01/ACLED.zip',
                                       'state': 'active',
                                       'position': 0, 'webstore_last_updated': None, 'mimetype': None,
                                       'package_id': '71d852e4-e41e-4320-a770-9fc2bb87fb64', 'resource_type': 'api',
                                       'id': '866b3b60-5b2a-4ca3-9b76-665870cc6d71', 'mimetype_inner': None,
                                       'webstore_url': None,
                                       'hash': '', 'originalHash': '97196323'},
                                      {'size': None,
                                       'description': 'ACLED-All-Africa-File_20160101-to-20160903_csv.zip',
                                       'revision_id': '796b639e-f217-4d0e-bedd-4e3b35da4461',
                                       'cache_last_updated': None,
                                       'created': '2016-06-22T12:41:02.857194',
                                       'name': 'ACLED-All-Africa-File_20160101-to-date_csv.zip',
                                       'last_modified': None, 'url_type': 'api', 'format': 'zipped csv',
                                       'cache_url': None,
                                       'url': 'http://www.acleddata.com/wp-content/uploads/2016/09/ACLED.zip',
                                       'state': 'active',
                                       'position': 1, 'revision_last_updated': '2016-06-28T14:17:27.150541',
                                       'webstore_last_updated': None,
                                       'mimetype': None, 'package_id': '45f53bde-544c-4a4a-9c6f-d609481b8719',
                                       'resource_type': 'api',
                                       'id': '74b74ae1-df0c-4716-829f-4f939a046823', 'mimetype_inner': None, 'hash': '',
                                       'webstore_url': None},
                                      {'size': None, 'description': '',
                                       'revision_id': '5259be68-e72f-4c02-be8e-61d7cd594a9b',
                                       'cache_last_updated': None, 'created': '2016-03-23T14:17:08.594232',
                                       'name': 'ACLED-Version-6-All-Africa-1997-2015_dyadic-file.xlsx',
                                       'last_modified': None,
                                       'url_type': 'api', 'format': 'XLSX', 'cache_url': None,
                                       'url': 'http://www.acleddata.com/wp-content/uploads/2016/01/ACLEDxlsx',
                                       'state': 'active',
                                       'position': 1, 'webstore_last_updated': None, 'mimetype': None,
                                       'package_id': '71d852e4-e41e-4320-a770-9fc2bb87fb64', 'resource_type': 'api',
                                       'id': 'e1e16f5c-2380-4a28-87b1-f5d644f248e5', 'mimetype_inner': None,
                                       'webstore_url': None,
                                       'hash': '', 'originalHash': '97196323'}]}

def mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == 'TEST1':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    if datadict['id'] == 'TEST2':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
    if datadict['id'] == 'TEST3':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
    if datadict['id'] == 'TEST4':
        resdictcopy = copy.deepcopy(resultdict)
        resdictcopy['url'] = 'lalalala'
        result = json.dumps(resdictcopy)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    if datadict['id'] == 'TEST5':
        resdictcopy = copy.deepcopy(resultdict)
        resdictcopy['id'] = 'datastore_unknown_resource'
        result = json.dumps(resdictcopy)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')


def mocksearch(url, datadict):
    if 'search' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not Search Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_search"}')
    result = json.dumps(searchdict)
    if datadict['query'] == 'name:ACLED':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_search"}' % result)
    if datadict['query'] == 'fail':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Validation Error", "__type": "Validation Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_search"}')
    if datadict['query'] == 'name:ajyhgr':
        return MockResponse(200,
                            '{"success": true, "result": {"count": 0, "results": []}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_search"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_search"}')


class TestResource:
    resource_data = {
        'name': 'MyResource1',
        'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
        'format': 'xlsx',
        'url': 'http://test/spreadsheet.xlsx',
        'description': 'My Resource',
        'api_type': 'api',
        'resource_type': 'api'
    }

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_resource_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_resource_static.json')

    @pytest.fixture(scope='class')
    def topline_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_datasource_topline.yml')

    @pytest.fixture(scope='class')
    def topline_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_datasource_topline.json')

    @pytest.fixture(scope='function')
    def read(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                datadict = json.loads(data.decode('utf-8'))
                return mockshow(url, datadict)

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_create(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                if isinstance(data, dict):
                    datadict = {k.decode('utf8'): v.decode('utf8') for k, v in data.items()}
                else:
                    datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'resource_id' in datadict:
                    if datadict['resource_id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                        return MockResponse(200,
                                            '{"success": true, "result": {"fields": [{"type": "text", "id": "code"}, {"type": "text", "id": "title"}, {"type": "float", "id": "value"}, {"type": "timestamp", "id": "latest_date"}, {"type": "text", "id": "source"}, {"type": "text", "id": "source_link"}, {"type": "text", "id": "notes"}, {"type": "text", "id": "explore"}, {"type": "text", "id": "units"}], "method": "insert", "primary_key": "code", "resource_id": "bfa6b55f-10b6-4ba2-8470-33bb9a5194a5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}')
                if datadict['name'] == 'MyResource1':
                    resultdictcopy = copy.deepcopy(resultdict)
                    if files is not None:
                        resultdictcopy['url_type'] = 'upload'
                        resultdictcopy['resource_type'] = 'file.upload'
                        filename = os.path.basename(files[0][1].name)
                        resultdictcopy[
                            'url'] = 'http://test-data.humdata.org/dataset/6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d/resource/de6549d8-268b-4dfe-adaf-a4ae5c8510d5/download/%s' % filename

                    result = json.dumps(resultdictcopy)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}' % result)
                if datadict['name'] == 'MyResource2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}')
                if datadict['name'] == 'MyResource3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}')
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_update(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                if isinstance(data, dict):
                    datadict = {k.decode('utf8'): v.decode('utf8') for k, v in data.items()}
                else:
                    datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'resource_id' in datadict:
                    if datadict['resource_id'] == 'TEST1':
                        return MockResponse(200,
                                            '{"success": true, "result": {"fields": [{"type": "text", "id": "code"}, {"type": "text", "id": "title"}, {"type": "float", "id": "value"}, {"type": "timestamp", "id": "latest_date"}, {"type": "text", "id": "source"}, {"type": "text", "id": "source_link"}, {"type": "text", "id": "notes"}, {"type": "text", "id": "explore"}, {"type": "text", "id": "units"}], "method": "insert", "primary_key": "code", "resource_id": "bfa6b55f-10b6-4ba2-8470-33bb9a5194a5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                if 'update' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}')
                if datadict['name'] == 'MyResource1':
                    resultdictcopy = copy.deepcopy(resultdict)
                    merge_two_dictionaries(resultdictcopy, datadict)
                    if files is not None:
                        resultdictcopy['url_type'] = 'upload'
                        resultdictcopy['resource_type'] = 'file.upload'
                        filename = os.path.basename(files[0][1].name)
                        resultdictcopy[
                            'url'] = 'http://test-data.humdata.org/dataset/6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d/resource/de6549d8-268b-4dfe-adaf-a4ae5c8510d5/download/%s' % filename
                    result = json.dumps(resultdictcopy)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}' % result)
                if datadict['name'] == 'MyResource2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}')
                if datadict['name'] == 'MyResource3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_delete(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')
                if datadict['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_datastore(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'create' not in url and 'insert' not in url and 'upsert' not in url and 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create or delete", "__type": "TEST ERROR: Not Create or Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_action"}')
                if 'delete' in url and datadict['resource_id'] == 'datastore_unknown_resource':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_delete"}')
                if 'delete' in url and datadict['resource_id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                    return MockResponse(200,
                                        '{"success": true, "result": {"resource_id": "de6549d8-268b-4dfe-adaf-a4ae5c8510d5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')
                if 'create' in url and datadict['resource_id'] == 'datastore_unknown_resource':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                if ('create' in url or 'insert' in url or 'upsert' in url) and datadict[
                    'resource_id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                    return MockResponse(200,
                                        '{"success": true, "result": {"fields": [{"type": "text", "id": "code"}, {"type": "text", "id": "title"}, {"type": "float", "id": "value"}, {"type": "timestamp", "id": "latest_date"}, {"type": "text", "id": "source"}, {"type": "text", "id": "source_link"}, {"type": "text", "id": "notes"}, {"type": "text", "id": "explore"}, {"type": "text", "id": "units"}], "method": "insert", "primary_key": "code", "resource_id": "bfa6b55f-10b6-4ba2-8470-33bb9a5194a5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def search(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                datadict = json.loads(data.decode('utf-8'))
                return mocksearch(url, datadict)

        monkeypatch.setattr(requests, 'Session', MockSession)

    def test_read_from_hdx(self, configuration, read):
        resource = Resource.read_from_hdx('TEST1')
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource['name'] == 'MyResource1'
        assert resource['package_id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        resource = Resource.read_from_hdx('TEST2')
        assert resource is None
        resource = Resource.read_from_hdx('TEST3')
        assert resource is None

    def test_create_in_hdx(self, configuration, post_create):
        resource = Resource()
        with pytest.raises(HDXError):
            resource.create_in_hdx()
        resource['id'] = 'TEST1'
        resource['name'] = 'LALA'
        with pytest.raises(HDXError):
            resource.create_in_hdx()

        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        resource.create_in_hdx()
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource['url_type'] == 'api'
        assert resource['resource_type'] == 'api'
        assert resource[
                   'url'] == 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'

        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        filetoupload = join('tests', 'fixtures', 'test_data.csv')
        resource.set_file_to_upload(filetoupload)
        assert resource.get_file_to_upload() == filetoupload
        resource.create_in_hdx()
        assert resource['url_type'] == 'upload'
        assert resource['resource_type'] == 'file.upload'
        assert resource[
                   'url'] == 'http://test-data.humdata.org/dataset/6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d/resource/de6549d8-268b-4dfe-adaf-a4ae5c8510d5/download/test_data.csv'

        resource_data['name'] = 'MyResource2'
        resource = Resource(resource_data)
        with pytest.raises(HDXError):
            resource.create_in_hdx()

        resource_data['name'] = 'MyResource3'
        resource = Resource(resource_data)
        with pytest.raises(HDXError):
            resource.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        resource = Resource()
        resource['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            resource.update_in_hdx()
        resource['name'] = 'LALA'
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        resource = Resource.read_from_hdx('TEST1')
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource['format'] == 'XLSX'

        resource['format'] = 'CSV'
        resource['id'] = 'TEST1'
        resource['name'] = 'MyResource1'
        resource.update_in_hdx()
        assert resource['id'] == 'TEST1'
        assert resource['format'] == 'CSV'
        assert resource['url_type'] == 'api'
        assert resource['resource_type'] == 'api'
        assert resource[
                   'url'] == 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'

        filetoupload = join('tests', 'fixtures', 'test_data.csv')
        resource.set_file_to_upload(filetoupload)
        resource.update_in_hdx()
        assert resource['url_type'] == 'upload'
        assert resource['resource_type'] == 'file.upload'
        assert resource[
                   'url'] == 'http://test-data.humdata.org/dataset/6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d/resource/de6549d8-268b-4dfe-adaf-a4ae5c8510d5/download/test_data.csv'

        resource['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        del resource['id']
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        resource_data = copy.deepcopy(TestResource.resource_data)
        resource_data['name'] = 'MyResource1'
        resource_data['id'] = 'TEST1'
        resource = Resource(resource_data)
        resource.create_in_hdx()
        assert resource['id'] == 'TEST1'
        assert resource['format'] == 'xlsx'

    def test_delete_from_hdx(self, configuration, post_delete):
        resource = Resource.read_from_hdx('TEST1')
        resource.delete_from_hdx()
        del resource['id']
        with pytest.raises(HDXError):
            resource.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        assert resource['name'] == 'MyResource1'
        assert resource['format'] == 'xlsx'
        resource.update_from_yaml(static_yaml)
        assert resource['name'] == 'MyResource1'
        assert resource['format'] == 'csv'

    def test_update_json(self, configuration, static_json):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        assert resource['name'] == 'MyResource1'
        assert resource['format'] == 'xlsx'
        resource.update_from_json(static_json)
        assert resource['name'] == 'MyResource1'
        assert resource['format'] == 'zipped csv'

    def test_search_in_hdx(self, configuration, search):
        resources = Resource.search_in_hdx('name:ACLED')
        assert len(resources) == 4
        resources = Resource.search_in_hdx('name:ajyhgr')
        assert len(resources) == 0
        with pytest.raises(HDXError):
            Resource.search_in_hdx('fail')

    def test_download(self, configuration, read, monkeypatch):
        resource = Resource.read_from_hdx('TEST1')
        resource2 = Resource.read_from_hdx('TEST4')
        monkeypatch.undo()
        url, path = resource.download()
        unlink(path)
        assert url == 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'
        resource['url'] = ''
        with pytest.raises(HDXError):
            resource.download()
        with pytest.raises(DownloadError):
            resource2.download()

    def test_datastore(self, configuration, post_datastore, topline_yaml, topline_json, monkeypatch):
        resource = Resource.read_from_hdx('TEST1')
        resource2 = Resource.read_from_hdx('TEST5')
        monkeypatch.undo()
        resource.create_datastore(delete_first=0)
        resource.create_datastore(delete_first=1)
        resource.create_datastore(delete_first=2)
        with pytest.raises(HDXError):
            resource.create_datastore(delete_first=3)
        resource.update_datastore()
        resource.update_datastore_for_topline()
        resource.update_datastore_from_yaml_schema(topline_yaml)
        filefordatastore = join('tests', 'fixtures', 'test_data.csv')
        resource.update_datastore_from_json_schema(topline_json, path=filefordatastore)
        filefordatastore = join('tests', 'fixtures', 'test_data.zip')
        resource.update_datastore_from_json_schema(topline_json, path=filefordatastore)
        with pytest.raises(HDXError):
            resource2.update_datastore_from_json_schema(topline_json)
