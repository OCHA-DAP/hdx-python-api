# -*- coding: UTF-8 -*-
"""Resource Tests"""
import copy
import json
import os
import datetime
from os import remove
from os.path import join, basename

import pytest
import six
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.downloader import DownloadError

from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.hdx_configuration import Configuration
from . import MockResponse, dataset_resultdict
from .test_resource_view import resource_view_list, resource_view_mocklist

resultdict = {'cache_last_updated': None, 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
              'webstore_last_updated': None, 'datastore_active': None,
              'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'size': None,
              'hash': '', 'description': 'My Resource', 'format': 'csv', 'last_modified': None, 'url_type': 'api',
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
                                       'id': '74b74ae1-df0c-4716-829f-4f939a046811', 'mimetype_inner': None, 'hash': '',
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
    if datadict['id'] == '74b74ae1-df0c-4716-829f-4f939a046811':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    if datadict['id'] == '74b74ae1-df0c-4716-829f-4f939a046812':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
    if datadict['id'] == '74b74ae1-df0c-4716-829f-4f939a046813':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')
    if datadict['id'] == '74b74ae1-df0c-4716-829f-4f939a046814':
        resdictcopy = copy.deepcopy(resultdict)
        resdictcopy['url'] = 'lalalala'
        result = json.dumps(resdictcopy)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    if datadict['id'] == '74b74ae1-df0c-4716-829f-4f939a046815':
        resdictcopy = copy.deepcopy(resultdict)
        resdictcopy['id'] = 'datastore_unknown_resource'
        result = json.dumps(resdictcopy)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}')


def mockpatch(url, datadict):
    if 'patch' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not patch", "__type": "TEST ERROR: Not Patch Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_patch"}')
    result = json.dumps(resultdict)
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_patch"}' % result)


def mockdataset(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
    result = json.dumps(dataset_resultdict)
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)


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


def mockresourceview(url, decodedata):
    if 'delete' in url:
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_delete"}' % decodedata)
    datadict = json.loads(decodedata)
    if 'show' in url:
        if id not in datadict:
            return MockResponse(404,
                                '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}')
        if datadict['title'] == 'Data Explorer':
            result = json.dumps(resource_view_list[0])
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}' % result)
        if datadict['title'] == 'Quick Charts':
            result = json.dumps(resource_view_list[1])
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}' % result)
    if 'list' in url:
        return resource_view_mocklist(url, datadict)

    if 'create' in url or 'update' in url:
        if datadict['title'] == 'Data Explorer':
            result = json.dumps(resource_view_list[0])
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}' % result)
        if datadict['title'] == 'Quick Charts':
            result = json.dumps(resource_view_list[1])
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}' % result)
    if 'reorder' in url:
        result = json.dumps({'id': '25982d1c-f45a-45e1-b14e-87d367413045', 'order': ['c06b5a0d-1d41-4a74-a196-41c251c76023', 'd80301b5-4abd-49bd-bf94-fa4af7b6e7a4']})
        return MockResponse(200, '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_reorder"}' % result)

    return MockResponse(404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_show"}')


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

    datastore = None

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
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_create(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
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
                    resultdictcopy['state'] = datadict['state']

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

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_update(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                if isinstance(data, dict):
                    datadict = {k.decode('utf8'): v.decode('utf8') for k, v in data.items()}
                else:
                    datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'resource_id' in datadict:
                    if datadict['resource_id'] == '74b74ae1-df0c-4716-829f-4f939a046811':
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

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_delete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
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

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_datastore(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'create' not in url and 'insert' not in url and 'upsert' not in url and 'delete' not in url and 'search' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create or delete", "__type": "TEST ERROR: Not Create or Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_action"}')
                if 'delete' in url and datadict['resource_id'] == 'datastore_unknown_resource':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_delete"}')
                if 'delete' in url and datadict['resource_id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                    TestResource.datastore = 'delete'
                    return MockResponse(200,
                                        '{"success": true, "result": {"resource_id": "de6549d8-268b-4dfe-adaf-a4ae5c8510d5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')
                if 'create' in url and datadict['resource_id'] == 'datastore_unknown_resource':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                if 'search' in url and datadict['resource_id'] == 'datastore_unknown_resource':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                if 'search' in url and datadict['resource_id'] == '_table_metadata':
                    return MockResponse(200,
                                        '{"success": true, "result": {"include_total": true, "resource_id": "_table_metadata", "fields": [{"type": "int", "id": "_id"}, {"type": "name", "id": "name"}, {"type": "oid", "id": "oid"}, {"type": "name", "id": "alias_of"}], "records_format": "objects", "records": [{"_id":"f9cd60f3d7f2f6d0","name":"f9228459-d808-4b51-948f-68a5850abfde","oid":"919290","alias_of":null},{"_id":"7ae63490de9b7d7b","name":"af618a0b-09b8-42c8-836f-2be597e1ea34","oid":"135294","alias_of":null},{"_id":"1dc37f4e89988644","name":"748b40dd-7bd3-40a3-941b-e76f0bfbe0eb","oid":"117144","alias_of":null},{"_id":"2a554a61bd366206","name":"91c78d24-eab3-40b5-ba91-6b29bcda7178","oid":"116963","alias_of":null},{"_id":"fd787575143afe90","name":"9320cfce-4620-489a-bcbe-25c73867d4fc","oid":"107430","alias_of":null},{"_id":"a70093abd230f647","name":"b9d2eb36-e65c-417a-bc28-f4dadb149302","oid":"107409","alias_of":null},{"_id":"95fbdd2d06c07aea","name":"ca6a0891-8395-4d58-9168-6c44e17e0193","oid":"107385","alias_of":null}], "limit": 10000, "_links": {"start": "/api/action/datastore_search?limit=10000&resource_id=_table_metadata", "next": "/api/action/datastore_search?offset=10000&limit=10000&resource_id=_table_metadata"}, "total": 7}}')
                if ('create' in url or 'insert' in url or 'upsert' in url or 'search' in url) and datadict[
                    'resource_id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5':
                    TestResource.datastore = 'create'
                    return MockResponse(200,
                                        '{"success": true, "result": {"fields": [{"type": "text", "id": "code"}, {"type": "text", "id": "title"}, {"type": "float", "id": "value"}, {"type": "timestamp", "id": "latest_date"}, {"type": "text", "id": "source"}, {"type": "text", "id": "source_link"}, {"type": "text", "id": "notes"}, {"type": "text", "id": "explore"}, {"type": "text", "id": "units"}], "method": "insert", "primary_key": "code", "resource_id": "bfa6b55f-10b6-4ba2-8470-33bb9a5194a5"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=datastore_create"}')
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_delete"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_patch(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return mockshow(url, datadict)
                return mockpatch(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_dataset(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mockdataset(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def search(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mocksearch(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_resourceview(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                return mockresourceview(url, decodedata)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read):
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046811')
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource['name'] == 'MyResource1'
        assert resource['package_id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046812')
        assert resource is None
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046813')
        assert resource is None
        with pytest.raises(HDXError):
            Resource.read_from_hdx('ABC')

    def test_check_url_filetoupload(self, configuration):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        resource.check_url_filetoupload()
        resource.set_file_to_upload('abc')
        resource.check_url_filetoupload()
        resource['url'] = 'lala'
        with pytest.raises(HDXError):
            resource.check_url_filetoupload()

    def test_get_set_date_of_resource(self):
        resource = Resource({'daterange_for_data': '[2020-01-07T00:00:00 TO *]'})
        result = resource.get_date_of_resource(today=datetime.date(2020, 11, 17))
        assert result == {'startdate': datetime.datetime(2020, 1, 7, 0, 0), 'enddate': datetime.datetime(2020, 11, 17, 0, 0), 'startdate_str': '2020-01-07T00:00:00', 'enddate_str': '2020-11-17T00:00:00', 'ongoing': True}
        resource.set_date_of_resource('2020-02-09', '2020-10-20')
        result = resource.get_date_of_resource('%d/%m/%Y')
        assert result == {'startdate': datetime.datetime(2020, 2, 9, 0, 0), 'enddate': datetime.datetime(2020, 10, 20, 0, 0), 'startdate_str': '09/02/2020', 'enddate_str': '20/10/2020', 'ongoing': False}

    def test_check_required_fields(self, configuration):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        resource.check_url_filetoupload()
        resource.check_required_fields()

    def test_create_in_hdx(self, configuration, post_create):
        resource = Resource()
        with pytest.raises(HDXError):
            resource.create_in_hdx()
        resource['id'] = '74b74ae1-df0c-4716-829f-4f939a046811'
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
        assert resource['state'] == 'active'
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

        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046811')
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resource.get_file_type() == 'csv'

        resource.set_file_type('XLSX')
        resource['id'] = '74b74ae1-df0c-4716-829f-4f939a046811'
        resource['name'] = 'MyResource1'
        resource.update_in_hdx()
        assert resource['id'] == '74b74ae1-df0c-4716-829f-4f939a046811'
        assert resource['format'] == 'xlsx'
        assert resource.get_file_type() == 'xlsx'
        assert resource['url_type'] == 'api'
        assert resource['resource_type'] == 'api'
        assert resource[
                   'url'] == 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'
        assert resource['state'] == 'active'

        filetoupload = join('tests', 'fixtures', 'test_data.csv')
        resource.set_file_to_upload(filetoupload)
        resource.update_in_hdx()
        assert resource['url_type'] == 'upload'
        assert resource['resource_type'] == 'file.upload'
        assert resource[
                   'url'] == 'http://test-data.humdata.org/dataset/6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d/resource/de6549d8-268b-4dfe-adaf-a4ae5c8510d5/download/test_data.csv'
        assert resource['state'] == 'active'

        resource['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        del resource['id']
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        resource.data = dict()
        with pytest.raises(HDXError):
            resource.update_in_hdx()

        resource_data = copy.deepcopy(TestResource.resource_data)
        resource_data['name'] = 'MyResource1'
        resource_data['id'] = '74b74ae1-df0c-4716-829f-4f939a046811'
        resource = Resource(resource_data)
        resource.create_in_hdx()
        assert resource['id'] == '74b74ae1-df0c-4716-829f-4f939a046811'
        assert resource.get_file_type() == 'xlsx'
        assert resource['state'] == 'active'

    def test_delete_from_hdx(self, configuration, post_delete):
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046811')
        resource.delete_from_hdx()
        del resource['id']
        with pytest.raises(HDXError):
            resource.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        assert resource['name'] == 'MyResource1'
        assert resource.get_file_type() == 'xlsx'
        resource.update_from_yaml(static_yaml)
        assert resource['name'] == 'MyResource1'
        assert resource.get_file_type() == 'csv'

    def test_update_json(self, configuration, static_json):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        assert resource['name'] == 'MyResource1'
        assert resource.get_file_type() == 'xlsx'
        resource.update_from_json(static_json)
        assert resource['name'] == 'MyResource1'
        assert resource.get_file_type() == 'zipped csv'

    def test_patch(self, configuration, post_patch):
        resource = Resource()
        resource['id'] = '74b74ae1-df0c-4716-829f-4f939a046811'
        resource.update_in_hdx(operation='patch', batch_mode='KEEP_OLD', skip_validation=True)
        assert resource['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'

    def test_get_dataset(self, configuration, post_dataset):
        resource_data = copy.deepcopy(TestResource.resource_data)
        resource = Resource(resource_data)
        dataset = resource.get_dataset()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        del resource['package_id']
        with pytest.raises(HDXError):
            resource.get_dataset()

    def test_search_in_hdx(self, configuration, search):
        resources = Resource.search_in_hdx('name:ACLED')
        assert len(resources) == 4
        resources = Resource.search_in_hdx('name:ajyhgr')
        assert len(resources) == 0
        with pytest.raises(HDXError):
            Resource.search_in_hdx('fail')

    def test_download(self, configuration, read):
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046811')
        resource2 = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046814')
        url, path = resource.download()
        remove(path)
        assert url == 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'
        assert basename(path) == 'MyResource1.csv'
        resource['url'] = ''
        with pytest.raises(HDXError):
            resource.download()
        with pytest.raises(DownloadError):
            resource2.download()

    def test_datastore(self, configuration, post_datastore, topline_yaml, topline_json):
        resource_ids = Resource.get_all_resource_ids_in_datastore()
        assert resource_ids == ['f9228459-d808-4b51-948f-68a5850abfde', 'af618a0b-09b8-42c8-836f-2be597e1ea34', '748b40dd-7bd3-40a3-941b-e76f0bfbe0eb', '91c78d24-eab3-40b5-ba91-6b29bcda7178', '9320cfce-4620-489a-bcbe-25c73867d4fc', 'b9d2eb36-e65c-417a-bc28-f4dadb149302', 'ca6a0891-8395-4d58-9168-6c44e17e0193']
        resource = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046811')
        resource2 = Resource.read_from_hdx('74b74ae1-df0c-4716-829f-4f939a046815')
        TestResource.datastore = None
        resource.create_datastore(delete_first=0)
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        resource.create_datastore(delete_first=1)
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        resource.create_datastore(delete_first=2)
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        with pytest.raises(HDXError):
            resource.create_datastore(delete_first=3)
        resource.update_datastore()
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        resource.update_datastore_for_topline()
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        resource.update_datastore_from_dict_schema({
          "schema": [
            {
              "id": "code",
              "type": "text"
            },
          ],
          "primary_key": "code"
        })
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        resource.update_datastore_from_yaml_schema(topline_yaml)
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        filefordatastore = join('tests', 'fixtures', 'test_data.csv')
        resource.update_datastore_from_json_schema(topline_json, path=filefordatastore)
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        assert resource.has_datastore() is True
        assert TestResource.datastore == 'create'
        TestResource.datastore = None
        assert resource2.has_datastore() is False
        TestResource.datastore = None
        filefordatastore = join('tests', 'fixtures', 'datastore', 'ACLED-All-Africa-File_20170101-to-20170708.xlsx')
        resource.update_datastore(path=filefordatastore)
        assert TestResource.datastore == 'create'
        with pytest.raises(HDXError):
            resource2.update_datastore_from_json_schema(topline_json)
        resource.delete_datastore()
        assert TestResource.datastore == 'delete'
        TestResource.datastore = None
        with pytest.raises(HDXError):
            del resource['url']
            resource.create_datastore()
        if six.PY3:
            filefordatastore = join('tests', 'fixtures', 'test_data.zip')
            resource.update_datastore_from_json_schema(topline_json, path=filefordatastore)
            assert TestResource.datastore == 'create'

    def test_resource_views(self, configuration, post_resourceview):
        resource = Resource({'id': '25982d1c-f45a-45e1-b14e-87d367413045'})
        with pytest.raises(HDXError):
            resource.add_update_resource_view('123')
        resource_view = copy.deepcopy(resource_view_list[0])
        del resource_view['id']
        del resource_view['package_id']
        resource.add_update_resource_view(resource_view)
        resource_view = copy.deepcopy(resource_view_list[1])
        del resource_view['id']
        del resource_view['package_id']
        with pytest.raises(HDXError):
            resource.add_update_resource_views('123')
        resource.add_update_resource_views([resource_view])
        resource_views = resource.get_resource_views()
        assert resource_views[0]['id'] == 'd80301b5-4abd-49bd-bf94-fa4af7b6e7a4'
        assert resource_views[1]['id'] == 'c06b5a0d-1d41-4a74-a196-41c251c76023'
        with pytest.raises(HDXError):
            resource.delete_resource_view('123')
        resource.delete_resource_view('d80301b5-4abd-49bd-bf94-fa4af7b6e7a4')
        resource.delete_resource_view(resource_view)
        resource_view['title'] = 'XXX'
        with pytest.raises(HDXError):
            resource.delete_resource_view(resource_view)
        with pytest.raises(HDXError):
            resource.reorder_resource_views('123')
        resource.reorder_resource_views(['c06b5a0d-1d41-4a74-a196-41c251c76023', 'd80301b5-4abd-49bd-bf94-fa4af7b6e7a4'])
        resource.reorder_resource_views(resource_view_list)
        resource_view = copy.deepcopy(resource_view_list[0])
        resource_view['id'] = '123'
        with pytest.raises(HDXError):
            resource.reorder_resource_views([resource_view_list[1], resource_view])
