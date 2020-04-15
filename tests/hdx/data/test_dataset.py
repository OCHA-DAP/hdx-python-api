# -*- coding: UTF-8 -*-
"""Dataset Tests"""
import copy
import datetime
import json
import re
import tempfile
from os import remove
from os.path import join
from parser import ParserError

import pytest
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import temp_dir

from hdx.data.dataset import Dataset, NotRequestableError
from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.resource import Resource
from hdx.data.user import User
from hdx.data.vocabulary import Vocabulary, ChainRuleError
from hdx.hdx_configuration import Configuration
from hdx.version import get_api_version
from . import MockResponse, user_data, organization_data
from .test_organization import organization_mockshow
from .test_resource_view import resource_view_list, resource_view_mockshow, resource_view_mocklist, \
    resource_view_mockcreate
from .test_showcase import showcase_resultdict
from .test_user import user_mockshow
from .test_vocabulary import vocabulary_mockshow

resulttags = [{'state': 'active', 'display_name': 'conflict', 'vocabulary_id': None,
               'id': '1dae41e5-eacd-4fa5-91df-8d80cf579e53', 'name': 'conflict'},
              {'state': 'active', 'display_name': 'political violence', 'vocabulary_id': None,
               'id': 'aaafc63b-2234-48e3-8ccc-198d7cf0f3f3', 'name': 'political violence'}]

resultgroups = [{'description': '', 'name': 'dza', 'image_display_url': '', 'display_name': 'Algeria', 'id': 'dza',
                 'title': 'Algeria'},
                {'description': '', 'name': 'zwe', 'image_display_url': '', 'display_name': 'Zimbabwe', 'id': 'zwe',
                 'title': 'Zimbabwe'}]

dataset_resultdict = {
    'resources': [{'revision_id': '43765383-1fce-471f-8166-d6c8660cc8a9', 'cache_url': None,
                   'datastore_active': False, 'format': 'XLSX', 'webstore_url': None,
                   'last_modified': None, 'tracking_summary': {'recent': 0, 'total': 0},
                   'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'webstore_last_updated': None,
                   'mimetype': None, 'state': 'active', 'created': '2016-06-07T08:57:27.367939',
                   'description': 'Resource1', 'position': 0,
                   'hash': '', 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                   'name': 'Resource1',
                   'url': 'http://resource1.xlsx',
                   'resource_type': 'api', 'url_type': 'api', 'size': None, 'mimetype_inner': None,
                   'cache_last_updated': None},
                  {'revision_id': '387e5d1a-50ca-4039-b5a7-f7b6b88d0f2b', 'cache_url': None,
                   'datastore_active': False, 'format': 'zipped csv', 'webstore_url': None,
                   'last_modified': None, 'tracking_summary': {'recent': 0, 'total': 0},
                   'id': '3d777226-96aa-4239-860a-703389d16d1f', 'webstore_last_updated': None,
                   'mimetype': None, 'state': 'active', 'created': '2016-06-07T08:57:27.367959',
                   'description': 'Resource2', 'position': 1,
                   'hash': '', 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                   'name': 'Resource2',
                   'url': 'http://resource2_csv.zip',
                   'resource_type': 'api', 'url_type': 'api', 'size': None, 'mimetype_inner': None,
                   'cache_last_updated': None}],
    'isopen': True, 'caveats': 'Various',
    'revision_id': '032833ca-c403-40cc-8b86-69d5a6eecb1b', 'url': None, 'author': 'acled',
    'metadata_created': '2016-03-23T14:28:48.664205',
    'license_url': 'http://www.opendefinition.org/licenses/cc-by-sa',
    'relationships_as_object': [], 'creator_user_id': '154de241-38d6-47d3-a77f-0a9848a61df3',
    'methodology_other': "This page contains information.",
    'subnational': '1', 'maintainer_email': 'me@me.com',
    'license_title': 'Creative Commons Attribution Share-Alike',
    'title': 'MyDataset', 'private': False,
    'maintainer': '8b84230c-e04a-43ec-99e5-41307a203a2f', 'methodology': 'Other', 'num_tags': 4, 'license_id': 'cc-by-sa',
    'tracking_summary': {'recent': 32, 'total': 178}, 'relationships_as_subject': [],
    'owner_org': 'b67e6c74-c185-4f43-b561-0e114a736f19', 'id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
    'dataset_source': 'ACLED', 'type': 'dataset',
    'notes': 'Notes',
    'organization': {'revision_id': '684f3eee-b708-4f91-bd22-7860d4eca423', 'description': 'MyOrganisation',
                     'name': 'acled', 'type': 'organization', 'image_url': '',
                     'approval_status': 'approved', 'state': 'active',
                     'title': 'MyOrganisation',
                     'created': '2015-01-09T14:44:54.006612',
                     'id': 'b67e6c74-c185-4f43-b561-0e114a736f19',
                     'is_organization': True},
    'state': 'active', 'author_email': 'me@me.com', 'package_creator': 'someone',
    'num_resources': 2, 'total_res_downloads': 4, 'name': 'MyDataset1',
    'metadata_modified': '2016-06-09T12:49:33.854367',
    'groups': resultgroups,
    'data_update_frequency': '7',
    'tags': resulttags,
    'version': None,
    'solr_additions': '{"countries": ["Algeria", "Zimbabwe"]}',
    'dataset_date': '06/04/2016'}

searchdict = load_yaml(join('tests', 'fixtures', 'dataset_search_results.yml'))
dataset_list = ['acled-conflict-data-for-libya', 'acled-conflict-data-for-liberia', 'acled-conflict-data-for-lesotho',
                'acled-conflict-data-for-kenya', 'acled-conflict-data-for-guinea', 'acled-conflict-data-for-ghana',
                'acled-conflict-data-for-gambia', 'acled-conflict-data-for-gabon', 'acled-conflict-data-for-ethiopia',
                'acled-conflict-data-for-eritrea']
hxlupdate_list = [{'title': 'Quick Charts', 'resource_id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 
                   'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d', 'view_type': 'hdx_hxl_preview', 
                   'description': '', 'id': '29cc5894-4306-4bef-96ce-b7a833e7986a'}]


def mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
    if 'resource_show' in url:
        result = json.dumps(TestDataset.resources_data[0])
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    else:
        if datadict['id'] == 'TEST1':
            result = json.dumps(dataset_resultdict)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)
        if datadict['id'] == 'DatasetExist':
            result = json.dumps(dataset_resultdict)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)
        if datadict['id'] == 'TEST2':
            return MockResponse(404,
                                '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
        if datadict['id'] == 'TEST3':
            return MockResponse(200,
                                '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
        if datadict['id'] == 'TEST4':
            resultdictcopy = copy.deepcopy(dataset_resultdict)
            resultdictcopy['id'] = 'TEST4'
            result = json.dumps(resultdictcopy)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)

    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')


def mocksearch(url, datadict):
    if 'search' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not Search Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    if datadict['q'] == 'ACLED':
        newsearchdict = copy.deepcopy(searchdict)
        if datadict['rows'] == 11:
            newsearchdict['results'].append(newsearchdict['results'][0])
        elif datadict['rows'] == 6:
            if datadict['start'] == 2:
                newsearchdict['results'] = newsearchdict['results'][2:8]
        elif datadict['rows'] == 5:
            if datadict['start'] == 0:
                newsearchdict['count'] = 5
                newsearchdict['results'] = newsearchdict['results'][:5]
            elif datadict['start'] == 5:
                newsearchdict['count'] = 6  # return wrong count
                newsearchdict['results'] = newsearchdict['results'][:5]
            else:
                newsearchdict['count'] = 0
                newsearchdict['results'] = list()
        result = json.dumps(newsearchdict)
        return MockResponse(200, '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % result)
    if datadict['q'] == '"':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Validation Error", "__type": "Validation Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    if datadict['q'] == 'ajyhgr':
        return MockResponse(200,
                            '{"success": true, "result": {"count": 0, "results": []}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')


def mocklist(url, datadict):
    if 'list' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_list"}')

    offset = datadict.get('offset', 0)
    limit = datadict.get('limit', len(dataset_list))
    result = json.dumps(dataset_list[offset:offset + limit])
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_list"}' % result)


def mockall(url, datadict):
    if 'search' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    newsearchdict = copy.deepcopy(searchdict)
    if datadict['rows'] == 11:
        newsearchdict['results'].append(newsearchdict['results'][0])
    elif datadict['rows'] == 7:
        if datadict['start'] == 2:
            newsearchdict['results'] = newsearchdict['results'][2:9]
        else:
            newsearchdict['results'] = newsearchdict['results'][4:5]  # repeated dataset
    elif datadict['rows'] == 5:
        newsearchdict['count'] = 6
        if datadict['sort'] == 'metadata_modified desc':
            if datadict['start'] == 0:
                newsearchdict['results'] = newsearchdict['results'][:5]
            else:
                newsearchdict['results'] = newsearchdict['results'][4:5]  # repeated dataset
        elif datadict['sort'] == 'metadata_modified asc':
            if datadict['start'] == 0:
                newsearchdict['results'] = newsearchdict['results'][:5]
            else:
                newsearchdict['results'] = newsearchdict['results'][5:6]
        else:
            if datadict['start'] == 0:
                newsearchdict['results'] = newsearchdict['results'][:5]
            else:
                newsearchdict['count'] = 7
                newsearchdict['results'] = newsearchdict['results'][5:7]
    result = json.dumps(newsearchdict)
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % result)


def mockhxlupdate(url, datadict):
    if 'hxl' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not HXL Update", "__type": "TEST ERROR: Not HXL Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_hxl_update"}')
    result = json.dumps(hxlupdate_list)
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_hxl_update"}' % result)


class TestDataset:
    dataset_data = {
        'name': 'MyDataset1',
        'title': 'MyDataset1',
        'dataset_date': '03/23/2016',  # has to be MM/DD/YYYY
        'groups': [{'description': '', 'name': 'dza', 'image_display_url': '', 'display_name': 'Algeria', 'id': 'dza',
                    'title': 'Algeria'},
                   {'description': '', 'name': 'zwe', 'image_display_url': '', 'display_name': 'Zimbabwe', 'id': 'zwe',
                    'title': 'Zimbabwe'}],
        'owner_org': 'My Org',
        'author': 'AN Other',
        'author_email': 'another@another.com',
        'maintainer': 'AN Other',
        'maintainer_email': 'another@another.com',
        'license_id': 'cc-by-sa',
        'subnational': 0,
        'notes': 'some notes',
        'caveats': 'some caveats',
        'data_update_frequency': '7',
        'methodology': 'other',
        'methodology_other': 'methodology description',
        'dataset_source': 'Mr Org',
        'package_creator': 'AN Other',
        'private': False,
        'url': None,
        'state': 'active',
        'tags': [{'state': 'active', 'display_name': 'conflict', 'vocabulary_id': None,
                  'id': '1dae41e5-eacd-4fa5-91df-8d80cf579e53', 'name': 'conflict'},
                 {'state': 'active', 'display_name': 'political violence', 'vocabulary_id': None,
                  'id': 'aaafc63b-2234-48e3-8ccc-198d7cf0f3f3', 'name': 'political violence'}],
    }

    resources_data = [{'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'description': 'Resource1',
                       'name': 'Resource1',
                       'url': 'http://resource1.xlsx',
                       'format': 'xlsx'},
                      {'id': 'DEF', 'description': 'Resource2',
                       'name': 'Resource2',
                       'url': 'http://resource2.csv',
                       'format': 'csv'}]

    association = None

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_dataset_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_dataset_static.json')

    @pytest.fixture(scope='class')
    def static_resource_view_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_resource_view_static.yml')

    @pytest.fixture(scope='function')
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'vocabulary' in url:
                    return vocabulary_mockshow(url, datadict)
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
                if 'vocabulary' in url:
                    return vocabulary_mockshow(url, datadict)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'hxl' in url:
                    return mockhxlupdate(url, datadict)
                if 'default' in url:
                    result = json.dumps(resource_view_list)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_create_default_resource_views"}' % result)
                if 'resource_view' in url:
                    result = json.dumps(resource_view_list[1])
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}' % result)
                if 'resource' in url:
                    result = json.dumps(TestDataset.resources_data[0])
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}' % result)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}')

                if datadict['name'] == 'MyDataset1':
                    resultdictcopy = copy.deepcopy(dataset_resultdict)
                    resultdictcopy['state'] = datadict['state']
                    result = json.dumps(resultdictcopy)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}' % result)
                if datadict['name'] == 'MyDataset2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}')
                if datadict['name'] == 'MyDataset3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}')
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}')

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
                if 'default' in url:
                    result = json.dumps(resource_view_list)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_create_default_resource_views"}' % result)
                if 'resource_view' in url:
                    if 'show' in url:
                        return resource_view_mockshow(url, datadict)
                    if 'list' in url:
                        return resource_view_mocklist(url, datadict)
                    if 'create' in url:
                        if datadict['title'] == 'Quick Charts':
                            return resource_view_mockcreate(url, datadict)
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}')
                if 'vocabulary' in url:
                    return vocabulary_mockshow(url, datadict)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'hxl' in url:
                    return mockhxlupdate(url, datadict)
                if 'resource' in url:
                    result = json.dumps(TestDataset.resources_data[0])
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}' % result)
                else:
                    if 'update' not in url:
                        return MockResponse(404,
                                            '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}')
                    resultdictcopy = copy.deepcopy(dataset_resultdict)
                    merge_two_dictionaries(resultdictcopy, datadict)
                    for i, resource in enumerate(resultdictcopy['resources']):
                        for j, resource2 in enumerate(resultdictcopy['resources']):
                            if i != j:
                                if resource == resource2:
                                    del resultdictcopy['resources'][j]
                                    break
                        resource['package_id'] = resultdictcopy['id']

                    if datadict['name'] == 'MyDataset1':
                        result = json.dumps(resultdictcopy)
                        return MockResponse(200,
                                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}' % result)
                    if datadict['name'] == 'DatasetExist':
                        resultdictcopy['name'] = 'DatasetExist'
                        result = json.dumps(resultdictcopy)
                        return MockResponse(200,
                                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}' % result)
                    if datadict['name'] == 'MyDataset2':
                        return MockResponse(404,
                                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}')
                    if datadict['name'] == 'MyDataset3':
                        return MockResponse(200,
                                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_reorder(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'hxl' in url:
                    return mockhxlupdate(url, datadict)
                if 'reorder' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not reorder", "__type": "TEST ERROR: Not Reorder Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_resource_reorder"}')
                if datadict['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_resource_reorder"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_resource_reorder"}')

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
                if 'purge' not in url and 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_delete"}')
                if 'resource' in url:
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_delete"}' % decodedata)

                if datadict['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_delete"}')

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
    def post_list(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mocklist(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def all(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mockall(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def user_read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return user_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def organization_read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return organization_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def showcase_read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'showcase_list' in url:
                    result = json.dumps([showcase_resultdict])
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_package_showcase_list"}' % result)
                if 'association_delete' in url:
                    TestDataset.association = 'delete'
                    return MockResponse(200,
                                        '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_delete"}')
                elif 'association_create' in url:
                    TestDataset.association = 'create'
                    result = json.dumps(datadict)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_create"}' % result)
                return mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['name'] == 'MyDataset1'
        assert dataset['dataset_date'] == '06/04/2016'
        assert dataset.number_of_resources() == 2
        dataset = Dataset.read_from_hdx('TEST2')
        assert dataset is None
        dataset = Dataset.read_from_hdx('TEST3')
        assert dataset is None

    def test_create_in_hdx(self, configuration, post_create):
        dataset = Dataset()
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset['id'] = 'TEST1'
        dataset['name'] = 'LALA'
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset.create_in_hdx(allow_no_resources=True)
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2

        dataset_data['name'] = 'MyDataset2'
        dataset = Dataset(dataset_data)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        dataset_data['name'] = 'MyDataset3'
        dataset = Dataset(dataset_data)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        dataset.add_update_resource(resource)
        del dataset['tags']
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset.create_in_hdx(ignore_check=True)

        config = Configuration(user_agent='test', hdx_read_only=True)
        config.setup_remoteckan()
        config.remoteckan().session = Configuration.read().remoteckan().session
        uniqueval = 'myconfig'
        config.unique = uniqueval

        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        dataset_data['resources'] = resources_data
        dataset = Dataset(dataset_data)
        assert len(dataset.get_resources()) == 2
        del dataset_data['resources']
        uniqueval = 'myconfig2'
        config.unique = uniqueval
        dataset = Dataset(dataset_data, configuration=config)
        del resources_data[0]['id']
        del resources_data[1]['id']
        dataset.add_update_resources(resources_data)
        dataset.create_in_hdx()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert len(dataset.resources) == 2
        assert dataset.resources[0].configuration.unique == uniqueval
        assert dataset['state'] == 'active'
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        resource = Resource(resources_data[0])
        file = tempfile.NamedTemporaryFile(delete=False)
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        dataset.create_in_hdx()
        remove(file.name)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2
        # Dataset creates that end up updating are in the test below

    def test_update_in_hdx(self, configuration, post_update):
        dataset = Dataset()
        dataset['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            dataset.update_in_hdx()
        dataset['name'] = 'LALA'
        with pytest.raises(HDXError):
            dataset.update_in_hdx()

        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['dataset_date'] == '06/04/2016'

        dataset['dataset_date'] = '02/26/2016'
        dataset['id'] = 'TEST1'
        dataset['name'] = 'MyDataset1'
        dataset.update_in_hdx()
        assert dataset['id'] == 'TEST1'
        assert dataset['dataset_date'] == '02/26/2016'
        assert dataset['state'] == 'active'
        pattern = r'HDXPythonLibrary/%s-test \([12]\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d\)' % get_api_version()
        match = re.search(pattern, dataset['updated_by_script'])
        assert match
        dataset.update_in_hdx(updated_by_script='hi')
        pattern = r'hi \([12]\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d\)'
        match = re.search(pattern, dataset['updated_by_script'])
        assert match
        dataset.update_in_hdx(updated_by_script='hi', batch='1234')
        pattern = r'hi \([12]\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d\) \[1234\]'
        match = re.search(pattern, dataset['updated_by_script'])
        assert match

        dataset['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            dataset.update_in_hdx()

        del dataset['id']
        with pytest.raises(HDXError):
            dataset.update_in_hdx()

        dataset['id'] = 'TEST1'
        dataset['groups'] = list()
        with pytest.raises(HDXError):
            dataset.update_in_hdx()
        dataset.update_in_hdx(ignore_check=True)

        # These dataset creates actually do updates
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        dataset['id'] = 'NOTEXIST'
        dataset['name'] = 'DatasetExist'
        dataset.create_in_hdx(allow_no_resources=True)
        assert dataset['name'] == 'DatasetExist'
        dataset_data['name'] = 'MyDataset1'
        dataset_data['id'] = 'TEST1'
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx(update_resources_by_name=False)
        assert dataset['id'] == 'TEST1'
        assert dataset['dataset_date'] == '03/23/2016'
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2
        dataset.update_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2
        dataset = Dataset.read_from_hdx('TEST4')
        dataset['id'] = 'TEST4'
        assert dataset['state'] == 'active'
        dataset.update_in_hdx()
        assert len(dataset.resources) == 2
        dataset = Dataset.read_from_hdx('TEST4')
        file = tempfile.NamedTemporaryFile(delete=False)
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        dataset.update_in_hdx()
        assert len(dataset.resources) == 2
        resource['name'] = '123'
        resource.set_file_to_upload(None)
        resource['url'] = 'http://lala'
        dataset.add_update_resource(resource)
        dataset.update_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resource(resource)
        resource = Resource(resources_data[1])
        dataset.add_update_resource(resource)
        resource = Resource(resources_data[0])
        resource['name'] = 'ResourcePosition'
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        resource = dataset.get_resources()[0]
        resource['name'] = 'changed name'
        resource.set_file_to_upload(file.name)
        dataset.update_in_hdx(update_resources_by_name=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 4
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resource(resource)
        resource = Resource(resources_data[1])
        dataset.add_update_resource(resource)
        resource = Resource(resources_data[0])
        resource['name'] = 'ResourcePosition'
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        resource = dataset.get_resources()[0]
        resource['name'] = 'changed name'
        resource.set_file_to_upload(file.name)
        dataset.update_in_hdx(update_resources_by_name=False)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        remove(file.name)
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(remove_additional_resources=False)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 2
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(remove_additional_resources=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 1
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(update_resources_by_name=False, remove_additional_resources=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 1

    def test_delete_from_hdx(self, configuration, post_delete):
        dataset = Dataset.read_from_hdx('TEST1')
        dataset.delete_from_hdx()
        del dataset['id']
        with pytest.raises(HDXError):
            dataset.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        assert dataset['name'] == 'MyDataset1'
        assert dataset['author'] == 'AN Other'
        dataset.update_from_yaml(static_yaml)
        assert dataset['name'] == 'MyDataset1'
        assert dataset['author'] == 'acled'
        assert dataset.get_resources() == [{"id": "ABC", "description": "Resource1",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource1",
                                            "url": "http://resource1.xlsx",
                                            "format": "xlsx"},
                                           {"id": "DEF", "description": "Resource2",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource2",
                                            "url": "http://resource2.csv",
                                            "format": "csv"}]
        dataset.get_resources()[0]['url'] = 'http://lalala.xlsx'
        assert dataset.get_resources() == [{"id": "ABC", "description": "Resource1",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource1",
                                            "url": "http://lalala.xlsx",
                                            "format": "xlsx"},
                                           {"id": "DEF", "description": "Resource2",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource2",
                                            "url": "http://resource2.csv",
                                            "format": "csv"}]
        dataset.update_from_yaml(static_yaml)
        assert dataset.get_resources() == [{"id": "ABC", "description": "Resource1",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource1",
                                            "url": "http://resource1.xlsx",
                                            "format": "xlsx"},
                                           {"id": "DEF", "description": "Resource2",
                                            "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d", "name": "Resource2",
                                            "url": "http://resource2.csv",
                                            "format": "csv"}]

    def test_update_json(self, configuration, static_json):
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        assert dataset['name'] == 'MyDataset1'
        assert dataset['author'] == 'AN Other'
        dataset.update_from_json(static_json)
        assert dataset['name'] == 'MyDataset1'
        assert dataset['author'] == 'Someone'
        assert dataset.get_resource() == {'id': '123', 'description': 'Resource1',
                                            'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d', 'name': 'Resource1',
                                            'url': 'http://resource1.xlsx',
                                            'format': 'xlsx'}
        assert dataset.get_resources() == [{'id': '123', 'description': 'Resource1',
                                            'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d', 'name': 'Resource1',
                                            'url': 'http://resource1.xlsx',
                                            'format': 'xlsx'},
                                           {'id': '456', 'description': 'Resource2',
                                            'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d', 'name': 'Resource2',
                                            'url': 'http://resource2.csv',
                                            'format': 'csv'}]

    def test_add_update_delete_resources(self, configuration, post_delete):
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        dataset = Dataset(dataset_data)
        dataset.add_update_resources(resources_data)
        dataset.add_update_resources(resources_data)
        assert len(dataset.resources) == 2
        dataset.delete_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d6')
        assert len(dataset.resources) == 2
        dataset.delete_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d5')
        assert len(dataset.resources) == 1
        resources_data = copy.deepcopy(TestDataset.resources_data)
        resource = Resource(resources_data[0])
        resource.set_file_to_upload('lala')
        dataset.add_update_resource(resource)
        assert dataset.resources[1].get_file_to_upload() == 'lala'
        dataset.add_update_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d5')
        assert len(dataset.resources) == 2
        with pytest.raises(HDXError):
            dataset.add_update_resource(123)
        with pytest.raises(HDXError):
            dataset.add_update_resource('123')
        resources_data[0]['package_id'] = '123'
        with pytest.raises(HDXError):
            dataset.add_update_resources(resources_data)
        with pytest.raises(HDXError):
            dataset.add_update_resources(123)
        with pytest.raises(HDXError):
            dataset.delete_resource('NOTEXIST')

    def test_reorder_resources(self, configuration, post_reorder):
        dataset = Dataset.read_from_hdx('TEST1')
        dataset.reorder_resources(['3d777226-96aa-4239-860a-703389d16d1f', 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'])
        del dataset['id']
        with pytest.raises(HDXError):
            dataset.reorder_resources(['3d777226-96aa-4239-860a-703389d16d1f', 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'])

    def test_search_in_hdx(self, configuration, search):
        datasets = Dataset.search_in_hdx('ACLED')
        assert len(datasets) == 10
        datasets = Dataset.search_in_hdx('ACLED', offset=2, limit=6)
        assert len(datasets) == 6
        datasets = Dataset.search_in_hdx('ajyhgr')
        assert len(datasets) == 0
        with pytest.raises(HDXError):
            Dataset.search_in_hdx('"')
        with pytest.raises(HDXError):
            Dataset.search_in_hdx('ACLED', rows=11)
        with pytest.raises(HDXError):
            # Test returned row counts per page mismatch (wrong count of 6 purposely in mocksearch)
            Dataset.search_in_hdx('ACLED', page_size=5)

    def test_get_all_dataset_names(self, configuration, post_list):
        dataset_names = Dataset.get_all_dataset_names()
        assert dataset_names == dataset_list
        dataset_names = Dataset.get_all_dataset_names(start=3, rows=5)
        assert dataset_names == dataset_list[3:8]

    def test_get_all_datasets(self, configuration, all):
        datasets = Dataset.get_all_datasets()
        assert len(datasets) == 10
        datasets = Dataset.get_all_datasets(start=2, rows=7)
        assert len(datasets) == 7
        with pytest.raises(HDXError):
            Dataset.get_all_datasets(limit=11)
        with pytest.raises(HDXError):
            Dataset.get_all_datasets(page_size=5, sort='metadata_modified desc')  # test repeated dataset (see mockall)
        with pytest.raises(HDXError):
            Dataset.get_all_datasets(page_size=5, sort='relevance desc')  # test changed count (see mockall)
        datasets = Dataset.get_all_datasets(page_size=5)  # test no repeating dataset (see mockall)
        assert len(datasets) == 6

    def test_get_all_resources(self, configuration, search):
        datasets = Dataset.search_in_hdx('ACLED')
        resources = Dataset.get_all_resources(datasets)
        assert len(resources) == 3

    def test_get_set_dataset_date(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['dataset_date'] == '06/04/2016'
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2016, 6, 4, 0, 0)
        assert dataset.get_dataset_date() == '2016-06-04'
        assert dataset.get_dataset_date('%Y/%m/%d') == '2016/06/04'
        testdate = datetime.datetime(2013, 12, 25, 0, 0)
        dataset.set_dataset_date_from_datetime(testdate)
        assert dataset['dataset_date'] == '12/25/2013'
        assert dataset.get_dataset_date_as_datetime() == testdate
        assert dataset.get_dataset_date() == '2013-12-25'
        assert dataset.get_dataset_date('%y-%m-%d %H:%M:%S%Z') == '13-12-25 00:00:00'
        dataset.set_dataset_date_from_datetime(testdate, testdate)
        assert dataset['dataset_date'] == '12/25/2013'
        dataset.set_dataset_date('2007-01-25T12:00:00Z')
        assert dataset['dataset_date'] == '01/25/2007'
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2007, 1, 25, 0, 0)
        assert dataset.get_dataset_date() == '2007-01-25'
        assert dataset.get_dataset_date('%Y-%m-%dT%H:%M:%S%Z') == '2007-01-25T00:00:00'
        dataset.set_dataset_date('2013-09-11')
        assert dataset['dataset_date'] == '09/11/2013'
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2013, 9, 11, 0, 0)
        assert dataset.get_dataset_date() == '2013-09-11'
        assert dataset.get_dataset_date('%Y/%m/%d') == '2013/09/11'
        test_date = '2021/05/06'
        dataset.set_dataset_date(test_date, date_format='%Y/%m/%d')
        assert dataset['dataset_date'] == '05/06/2021'
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2021, 5, 6, 0, 0)
        assert dataset.get_dataset_date() == '2021-05-06'
        assert dataset.get_dataset_date('%Y/%m/%d') == test_date
        assert dataset.get_dataset_date_type() == 'date'
        test_date = '2021/05/06'
        dataset.set_dataset_date(test_date, None, '%Y/%m/%d', allow_range=False)
        assert dataset['dataset_date'] == '05/06/2021'
        test_end_date = '2021/07/08'
        dataset.set_dataset_date(test_date, test_end_date, '%Y/%m/%d')
        assert dataset['dataset_date'] == '05/06/2021-07/08/2021'
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2021, 5, 6, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2021, 7, 8, 0, 0)
        assert dataset.get_dataset_date() == '2021-05-06'
        assert dataset.get_dataset_end_date() == '2021-07-08'
        assert dataset.get_dataset_date('%Y/%m/%d') == test_date
        assert dataset.get_dataset_end_date('%Y/%m/%d') == test_end_date
        assert dataset.get_dataset_date_type() == 'range'
        dataset.set_dataset_date(test_date, test_end_date, '%Y/%m/%d', allow_range=False)
        assert dataset['dataset_date'] == '05/06/2021-07/08/2021'
        dataset.set_dataset_date(test_date, test_end_date)
        assert dataset['dataset_date'] == '05/06/2021-07/08/2021'
        dataset.set_dataset_date(test_date, test_end_date, allow_range=False)
        assert dataset['dataset_date'] == '05/06/2021-07/08/2021'
        retval = dataset.set_dataset_year_range(2001, 2015)
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2001, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2015, 12, 31, 0, 0)
        assert retval == [2001, 2015]
        retval = dataset.set_dataset_year_range('2010', '2017')
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2010, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2017, 12, 31, 0, 0)
        assert retval == [2010, 2017]
        retval = dataset.set_dataset_year_range('2013')
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2013, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2013, 12, 31, 0, 0)
        assert retval == [2013]
        retval = dataset.set_dataset_year_range({2005, 2002, 2003})
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2002, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2005, 12, 31, 0, 0)
        assert retval == [2002, 2003, 2005]
        retval = dataset.set_dataset_year_range([2005, 2002, 2003])
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2002, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2005, 12, 31, 0, 0)
        assert retval == [2002, 2003, 2005]
        retval = dataset.set_dataset_year_range((2005, 2002, 2003))
        assert dataset.get_dataset_date_as_datetime() == datetime.datetime(2002, 1, 1, 0, 0)
        assert dataset.get_dataset_end_date_as_datetime() == datetime.datetime(2005, 12, 31, 0, 0)
        assert retval == [2002, 2003, 2005]
        with pytest.raises(ParserError):
            dataset.set_dataset_date('lalala')
        with pytest.raises(ParserError):
            dataset.set_dataset_date('lalala', 'lalala')
        with pytest.raises(ParserError):
            dataset.set_dataset_date('lalala', 'lalala', date_format='%Y/%m/%d')
        with pytest.raises(HDXError):
            dataset.set_dataset_year_range(23.5)
        with pytest.raises(HDXError):
            dataset.set_dataset_year_range(2015, 23.5)
        del dataset['dataset_date']
        assert dataset.get_dataset_date_as_datetime() is None
        assert dataset.get_dataset_end_date_as_datetime() is None
        assert dataset.get_dataset_date() is None
        assert dataset.get_dataset_date('YYYY/MM/DD') is None
        assert dataset.get_dataset_date_type() is None
        dataset.set_dataset_date('2013-09')
        assert dataset['dataset_date'] == '09/01/2013-09/30/2013'
        dataset.set_dataset_date('2013-09', date_format='%Y-%m')
        assert dataset['dataset_date'] == '09/01/2013-09/30/2013'
        dataset.set_dataset_date('2013-09', dataset_end_date='2014-02')
        assert dataset['dataset_date'] == '09/01/2013-02/28/2014'
        dataset.set_dataset_date('2013-09', dataset_end_date='2014-02', date_format='%Y-%m')
        assert dataset['dataset_date'] == '09/01/2013-02/28/2014'
        dataset.set_dataset_date('2013')
        assert dataset['dataset_date'] == '01/01/2013-12/31/2013'
        dataset.set_dataset_date('2013', dataset_end_date='2014')
        assert dataset['dataset_date'] == '01/01/2013-12/31/2014'
        dataset.set_dataset_date('2013', dataset_end_date='2014', date_format='%Y')
        assert dataset['dataset_date'] == '01/01/2013-12/31/2014'
        with pytest.raises(ParserError):
            dataset.set_dataset_date('2013-09', allow_range=False)
        with pytest.raises(ParserError):
            dataset.set_dataset_date('2013-09', date_format='%Y-%m', allow_range=False)
        with pytest.raises(ParserError):
            dataset.set_dataset_date('2013-09', dataset_end_date='2014-02', allow_range=False)
        with pytest.raises(ParserError):
            dataset.set_dataset_date('2013-09', dataset_end_date='2014-02', date_format='%Y-%m', allow_range=False)

    def test_transform_update_frequency(self):
        assert len(Dataset.list_valid_update_frequencies()) == 32
        assert Dataset.transform_update_frequency('-2') == 'As needed'
        assert Dataset.transform_update_frequency('-1') == 'Never'
        assert Dataset.transform_update_frequency('0') == 'Live'
        assert Dataset.transform_update_frequency('1') == 'Every day'
        assert Dataset.transform_update_frequency('Adhoc') == '-2'
        assert Dataset.transform_update_frequency('As needed') == '-2'
        assert Dataset.transform_update_frequency('Never') == '-1'
        assert Dataset.transform_update_frequency('Live') == '0'
        assert Dataset.transform_update_frequency('Every day') == '1'
        assert Dataset.transform_update_frequency('EVERY WEEK') == '7'
        assert Dataset.transform_update_frequency('every month') == '30'
        assert Dataset.transform_update_frequency('LALA') is None
        assert Dataset.transform_update_frequency(-2) == 'As needed'
        assert Dataset.transform_update_frequency(7) == 'Every week'
        assert Dataset.transform_update_frequency('') is None
        assert Dataset.transform_update_frequency(23) is None
        assert Dataset.transform_update_frequency('15') is None
        assert Dataset.transform_update_frequency('Quarterly') == '90'

    def test_get_set_expected_update_frequency(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['data_update_frequency'] == '7'
        assert dataset.get_expected_update_frequency() == 'Every week'
        dataset.set_expected_update_frequency('every two weeks')
        assert dataset['data_update_frequency'] == '14'
        dataset.set_expected_update_frequency(30)
        assert dataset['data_update_frequency'] == '30'
        dataset.set_expected_update_frequency('Fortnightly')
        assert dataset['data_update_frequency'] == '14'
        assert dataset.get_expected_update_frequency() == 'Every two weeks'
        dataset.set_expected_update_frequency('EVERY SIX MONTHS')
        assert dataset['data_update_frequency'] == '180'
        assert dataset.get_expected_update_frequency() == 'Every six months'
        dataset.set_expected_update_frequency('90')
        assert dataset['data_update_frequency'] == '90'
        assert dataset.get_expected_update_frequency() == 'Every three months'
        with pytest.raises(HDXError):
            dataset.set_expected_update_frequency('lalala')
        with pytest.raises(HDXError):
            dataset.set_expected_update_frequency(9)
        del dataset['data_update_frequency']
        assert dataset.get_expected_update_frequency() is None

    def test_get_add_tags(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['tags'] == resulttags
        assert dataset.get_tags() == ['conflict', 'political violence']
        dataset.add_tag('LALA')
        assert dataset['tags'] == resulttags
        assert dataset.get_tags() == ['conflict', 'political violence']
        dataset.add_tag('conflict')
        expected = copy.deepcopy(resulttags)
        expected.append({'name': 'violence and conflict', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'})
        assert dataset['tags'] == expected
        assert dataset.get_tags() == ['conflict', 'political violence', 'violence and conflict']
        dataset.add_tags(['desempleo', 'desocupación', 'desempleo', 'conflict-related deaths'])
        assert dataset.get_tags() == ['conflict', 'political violence', 'violence and conflict', 'unemployment', 'fatalities - deaths']
        dataset.remove_tag('violence and conflict')
        assert dataset.get_tags() == ['conflict', 'political violence', 'unemployment', 'fatalities - deaths']
        del dataset['tags']
        assert dataset.get_tags() == []
        dataset.add_tag('conflict-related deaths')
        assert dataset['tags'] == [{'name': 'violence and conflict', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'}, {'name': 'fatalities - deaths', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'}]
        assert dataset.get_tags() == ['violence and conflict', 'fatalities - deaths']
        dataset.add_tag(u'conflict-related deaths')
        assert dataset.get_tags() == ['violence and conflict', 'fatalities - deaths']
        dataset.add_tag(u'cholera')
        assert dataset.get_tags() == ['violence and conflict', 'fatalities - deaths', 'cholera']
        dataset.remove_tag(u'violence and conflict')
        assert dataset.get_tags() == ['fatalities - deaths', 'cholera']
        dataset.add_tag('cholera')
        assert dataset.get_tags() == ['fatalities - deaths', 'cholera']

    def test_is_set_subnational(self, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['subnational'] == '1'
        assert dataset.is_subnational() is True
        dataset.set_subnational(False)
        assert dataset['subnational'] == '0'
        assert dataset.is_subnational() is False
        dataset.set_subnational(True)
        assert dataset['subnational'] == '1'
        assert dataset.is_subnational() is True

    def test_get_add_location(self, locations, read):
        Country.countriesdata(use_live=False)
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['groups'] == resultgroups
        assert dataset.get_location() == ['Algeria', 'Zimbabwe']
        dataset.add_country_location('sdn')
        expected = copy.deepcopy(resultgroups)
        expected.append({'name': 'sdn'})
        assert dataset['groups'] == expected
        assert dataset.get_location() == ['Algeria', 'Zimbabwe', 'Sudan']
        dataset.add_country_location('dza')
        assert dataset['groups'] == expected
        assert dataset.get_location() == ['Algeria', 'Zimbabwe', 'Sudan']
        dataset.add_country_locations(['KEN', 'Mozambique', 'ken'])
        expected.extend([{'name': 'ken'}, {'name': 'moz'}])
        assert dataset['groups'] == expected
        assert dataset.get_location() == ['Algeria', 'Zimbabwe', 'Sudan', 'Kenya', 'Mozambique']
        dataset.remove_location('sdn')
        assert dataset.get_location() == ['Algeria', 'Zimbabwe', 'Kenya', 'Mozambique']
        with pytest.raises(HDXError):
            dataset.add_region_location('NOTEXIST')
        dataset.add_region_location('Africa')
        assert len(dataset['groups']) == 60
        assert len(dataset.get_location()) == 60
        del dataset['groups']
        assert dataset.get_location() == []
        with pytest.raises(HDXError):
            dataset.add_country_location('abc')
        with pytest.raises(HDXError):
            dataset.add_country_location('lala')
        dataset.add_country_location('Ukrai', exact=False)
        assert dataset['groups'] == [{'name': 'ukr'}]
        assert dataset.get_location() == ['Ukraine']
        dataset.add_country_location('ukr')
        dataset.add_other_location('nepal-earthquake')
        assert dataset['groups'] == [{'name': 'ukr'}, {'name': 'nepal-earthquake'}]
        assert dataset.get_location() == ['Ukraine', 'Nepal Earthquake']
        del dataset['groups']
        dataset.add_other_location('Nepal E', exact=False)
        assert dataset['groups'] == [{'name': 'nepal-earthquake'}]
        dataset.add_other_location('Nepal Earthquake')
        assert dataset['groups'] == [{'name': 'nepal-earthquake'}]
        with pytest.raises(HDXError):
            dataset.add_other_location('lala')
        with pytest.raises(HDXError):
            dataset.add_other_location('lala', alterror='nana')
        dataset['groups'] = [{'name': 'ken'}, {'name': 'MOZ'}, {'name': 'dza'}]
        dataset.remove_location('moz')
        assert dataset['groups'] == [{'name': 'ken'}, {'name': 'dza'}]
        dataset.remove_location('KEN')
        assert dataset['groups'] == [{'name': 'dza'}]

    def test_maintainer(self, configuration, user_read):
        dataset = Dataset(TestDataset.dataset_data)
        dataset.set_maintainer('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        maintainer = dataset.get_maintainer()
        assert maintainer['name'] == 'MyUser1'
        user = User(user_data)
        dataset.set_maintainer(user)
        maintainer = dataset.get_maintainer()
        assert maintainer['name'] == 'MyUser1'
        with pytest.raises(HDXError):
            dataset.set_maintainer('jpsmith')
        with pytest.raises(HDXError):
            dataset.set_maintainer(123)

    def test_organization(self, configuration, organization_read):
        dataset = Dataset(TestDataset.dataset_data)
        dataset.set_organization('b67e6c74-c185-4f43-b561-0e114a736f19')
        organization = dataset.get_organization()
        assert organization['name'] == 'acled'
        organization = Organization(organization_data)
        organization['name'] = 'TEST1'
        dataset.set_organization(organization)
        organization = dataset.get_organization()
        assert organization['name'] == 'acled'
        with pytest.raises(HDXError):
            dataset.set_organization('123')
        with pytest.raises(HDXError):
            dataset.set_organization(123)

    def test_add_update_delete_showcase(self, configuration, showcase_read):
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        dataset['id'] = 'dataset123'
        showcases = dataset.get_showcases()
        assert len(showcases) == 1
        TestDataset.association = None
        showcases[0]['id'] = '05e392bf-04e0-4ca6-848c-4e87bba10746'
        dataset.remove_showcase(showcases[0])
        assert TestDataset.association == 'delete'
        TestDataset.association = None
        assert dataset.add_showcase('15e392bf-04e0-4ca6-848c-4e87bba10745') is True
        assert TestDataset.association == 'create'
        TestDataset.association = None
        dataset.add_showcases([{'id': '15e392bf-04e0-4ca6-848c-4e87bba10745'}])
        assert TestDataset.association == 'create'
        TestDataset.association = None
        assert dataset.add_showcases([{'id': '15e392bf-04e0-4ca6-848c-4e87bba10745'}, {'id': '05e392bf-04e0-4ca6-848c-4e87bba10746'}]) is False
        assert TestDataset.association == 'create'
        TestDataset.association = None
        assert dataset.add_showcase({'name': 'TEST1'}) is True
        assert TestDataset.association == 'create'
        TestDataset.association = None
        with pytest.raises(HDXError):
            dataset.add_showcase('123')
        with pytest.raises(HDXError):
            dataset.add_showcase(123)

    def test_hdxconnect(self, configuration, post_create):
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        dataset['private'] = True
        dataset.set_requestable()
        assert dataset['is_requestdata_type'] is True
        assert dataset.is_requestable() is True
        assert dataset['private'] is False
        dataset['private'] = True
        dataset.set_requestable(False)
        assert dataset['is_requestdata_type'] is False
        assert dataset['private'] is True
        dataset.set_requestable()
        assert dataset.get('field_name') is None
        assert dataset.get_fieldnames() == list()
        assert dataset.add_fieldname('myfield1') is True
        assert dataset.add_fieldnames(['myfield1', 'myfield2']) is False
        assert dataset.remove_fieldname('myfield1') is True
        assert dataset.remove_fieldname('myfield1') is False
        assert dataset.add_fieldnames(['myfield3', 'myfield4']) is True
        assert dataset.get_fieldnames() == ['myfield2', 'myfield3', 'myfield4']
        assert dataset.get('fiele_types') is None
        assert dataset.get_filetypes() == list()
        assert dataset.add_filetype('mytype1') is True
        assert dataset.add_filetypes(['mytype1', 'mytype2']) is False
        assert dataset.remove_filetype('mytype1') is True
        assert dataset.remove_filetype('mytype1') is False
        assert dataset.add_filetypes(['mytype3', 'mytype4']) is True
        assert dataset.get_filetypes() == ['mytype2', 'mytype3', 'mytype4']
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset['num_of_rows'] = 100
        dataset.create_in_hdx()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        resources_data = copy.deepcopy(TestDataset.resources_data)
        dataset.add_update_resources(resources_data)
        with pytest.raises(NotRequestableError):
            dataset.get_fieldnames()
        with pytest.raises(NotRequestableError):
            dataset.add_fieldname('LALA')
        with pytest.raises(NotRequestableError):
            dataset.add_fieldnames(['LALA'])
        with pytest.raises(NotRequestableError):
            dataset.remove_fieldname('LALA')
        with pytest.raises(NotRequestableError):
            dataset.add_filetype('csv')
        with pytest.raises(NotRequestableError):
            dataset.add_filetypes(['csv'])
        with pytest.raises(NotRequestableError):
            dataset.remove_filetype('csv')
        assert dataset.get_filetypes() == ['xlsx', 'csv']

    def test_chainrule_error(self, configuration, read):
        with pytest.raises(ChainRuleError):
            Vocabulary.set_tagsdict(None)
            url = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/Tag_Mapping_ChainRuleError.csv'
            Vocabulary.read_tags_mappings(url=url, failchained=True)

    def test_add_clean_tags(self, configuration, read):
        Vocabulary.set_tagsdict(None)
        Vocabulary.read_tags_mappings(failchained=False)
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset.get_tags() == ['conflict', 'political violence']
        assert dataset.clean_tags() == (['violence and conflict'], ['political violence'])
        dataset.add_tags(['nodeid123', 'transportation'])
        assert dataset.get_tags() == ['violence and conflict', 'transportation']
        dataset['tags'].append({'name': 'nodeid123', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'})
        assert dataset.clean_tags() == (['violence and conflict', 'transportation'], ['nodeid123'])
        assert dataset.get_tags() == ['violence and conflict', 'transportation']
        dataset.add_tags(['geodata', 'points'])
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata'], [])
        dataset.add_tag('financial')
        assert dataset.get_tags() == ['violence and conflict', 'transportation', 'geodata']
        dataset['tags'].append({'name': 'financial', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'})
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata'], ['financial'])
        dataset.add_tag('addresses')
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata', '3-word addresses'], [])
        dataset.remove_tag('3-word addresses')
        assert dataset.get_tags() == ['violence and conflict', 'transportation', 'geodata']
        dataset.add_tag('cultivos coca')
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata', 'food production'], [])
        dataset.remove_tag('food production')
        dataset.add_tag('atentados')
        assert dataset.get_tags() == ['violence and conflict', 'transportation', 'geodata', 'security incidents']
        dataset['tags'].append({'name': 'atentados', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'})
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata', 'security incidents'], [])
        dataset.remove_tag('security incidents')
        dataset.add_tag('windspeeds')
        assert dataset.clean_tags() == (['violence and conflict', 'transportation', 'geodata', 'wind speed'], [])
        dataset.add_tag('conservancies')
        assert dataset.get_tags() == ['violence and conflict', 'transportation', 'geodata', 'wind speed', 'protected areas']
        dataset.remove_tag('transportation')
        dataset.remove_tag('protected areas')
        assert dataset.get_tags() == ['violence and conflict', 'geodata', 'wind speed']

    def test_set_quickchart_resource(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert 'dataset_preview' not in dataset
        assert dataset.set_quickchart_resource('3d777226-96aa-4239-860a-703389d16d1f')['id'] == '3d777226-96aa-4239-860a-703389d16d1f'
        assert dataset['dataset_preview'] == 'resource_id'
        resources = dataset.get_resources()
        assert resources[0]['dataset_preview_enabled'] == 'False'
        assert resources[1]['dataset_preview_enabled'] == 'True'
        assert dataset.set_quickchart_resource(resources[0])['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resources[0]['dataset_preview_enabled'] == 'True'
        assert resources[1]['dataset_preview_enabled'] == 'False'
        assert dataset.set_quickchart_resource(resources[1].data)['id'] == '3d777226-96aa-4239-860a-703389d16d1f'
        assert resources[0]['dataset_preview_enabled'] == 'False'
        assert resources[1]['dataset_preview_enabled'] == 'True'
        assert dataset.set_quickchart_resource(0)['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert resources[0]['dataset_preview_enabled'] == 'True'
        assert resources[1]['dataset_preview_enabled'] == 'False'
        assert dataset.set_quickchart_resource('12345') is None
        with pytest.raises(HDXError):
            dataset.set_quickchart_resource(True)
        dataset.preview_off()
        assert dataset['dataset_preview'] == 'no_preview'
        assert resources[0]['dataset_preview_enabled'] == 'False'
        assert resources[1]['dataset_preview_enabled'] == 'False'
        assert dataset.set_quickchart_resource('Resource2')['id'] == '3d777226-96aa-4239-860a-703389d16d1f'
        assert dataset['dataset_preview'] == 'resource_id'
        assert resources[0]['dataset_preview_enabled'] == 'False'
        assert resources[1]['dataset_preview_enabled'] == 'True'
        assert dataset.set_quickchart_resource({'name': 'Resource1'})['id'] == 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5'
        assert dataset['dataset_preview'] == 'resource_id'
        assert resources[0]['dataset_preview_enabled'] == 'True'
        assert resources[1]['dataset_preview_enabled'] == 'False'

    def test_quickcharts_resource_last(self):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset.quickcharts_resource_last() is False
        resource = {'name': 'QuickCharts-resource'}
        dataset.resources.insert(1, resource)
        assert dataset.quickcharts_resource_last() is True
        assert dataset.resources[2]['name'] == resource['name']
        assert dataset.quickcharts_resource_last() is True

    def test_generate_resource_view(self, configuration, post_update, static_resource_view_yaml):
        dataset = Dataset.read_from_hdx('TEST1')
        assert 'dataset_preview' not in dataset
        resourceview = dataset.generate_resource_view(path=static_resource_view_yaml)
        hxl_preview_config = json.loads(resourceview['hxl_preview_config'])
        assert resourceview['id'] == 'c06b5a0d-1d41-4a74-a196-41c251c76023'
        assert hxl_preview_config['bites'][0]['title'] == 'Sum of fatalities'
        assert hxl_preview_config['bites'][1]['title'] == 'Sum of fatalities grouped by admin1'
        assert hxl_preview_config['bites'][2]['title'] == 'Sum of fatalities grouped by admin2'
        resourceview = dataset.generate_resource_view(path=static_resource_view_yaml, bites_disabled=[False, True, False])
        hxl_preview_config = json.loads(resourceview['hxl_preview_config'])
        assert resourceview['id'] == 'c06b5a0d-1d41-4a74-a196-41c251c76023'
        assert hxl_preview_config['bites'][0]['title'] == 'Sum of fatalities'
        assert hxl_preview_config['bites'][1]['title'] == 'Sum of fatalities grouped by admin2'
        resourceview = dataset.generate_resource_view(path=static_resource_view_yaml, bites_disabled=[True, True, True])
        assert resourceview is None
        indicators = [{'code': '1', 'title': 'My1', 'unit': 'ones'}, {'code': '2', 'title': 'My2', 'unit': 'twos'},
                      {'code': '3', 'title': 'My3', 'unit': 'threes'}]
        resourceview = dataset.generate_resource_view(indicators=indicators)
        hxl_preview_config = json.loads(resourceview['hxl_preview_config'])
        assert resourceview['id'] == 'c06b5a0d-1d41-4a74-a196-41c251c76023'
        assert hxl_preview_config['bites'][0]['ingredient']['filters']['filterWith'][0]['#indicator+code'] == '1'
        assert hxl_preview_config['bites'][0]['uiProperties']['title'] == 'My1'
        assert hxl_preview_config['bites'][0]['computedProperties']['dataTitle'] == 'ones'
        assert hxl_preview_config['bites'][1]['ingredient']['filters']['filterWith'][0]['#indicator+code'] == '2'
        assert hxl_preview_config['bites'][1]['uiProperties']['title'] == 'My2'
        assert hxl_preview_config['bites'][1]['computedProperties']['dataTitle'] == 'twos'
        assert hxl_preview_config['bites'][2]['ingredient']['filters']['filterWith'][0]['#indicator+code'] == '3'
        assert hxl_preview_config['bites'][2]['uiProperties']['title'] == 'My3'
        assert hxl_preview_config['bites'][2]['computedProperties']['dataTitle'] == 'threes'
        assert dataset.generate_resource_view(indicators=[]) is None
        assert dataset.generate_resource_view(indicators=[None, None, None]) is None
        assert dataset.generate_resource_view(resource='123', path=static_resource_view_yaml) is None
        del dataset.get_resources()[0]['id']
        resourceview = dataset.generate_resource_view(path=static_resource_view_yaml)
        assert 'id' not in resourceview
        assert 'resource_id' not in resourceview
        assert resourceview['resource_name'] == 'Resource1'
        dataset['id'] = 'TEST1'
        dataset.update_in_hdx()
        assert dataset.preview_resourceview is None
        with pytest.raises(IOError):
            dataset.generate_resource_view()

    def test_get_hdx_url(self, configuration, hdx_config_yaml, project_config_yaml):
        dataset = Dataset()
        assert dataset.get_hdx_url() is None
        dataset_data = copy.deepcopy(TestDataset.dataset_data)
        dataset = Dataset(dataset_data)
        assert dataset.get_hdx_url() == 'https://data.humdata.org/dataset/MyDataset1'
        Configuration.delete()
        Configuration._create(hdx_site='feature', user_agent='test', hdx_config_yaml=hdx_config_yaml,
                              project_config_yaml=project_config_yaml)
        dataset = Dataset(dataset_data)
        assert dataset.get_hdx_url() == 'https://feature-data.humdata.org/dataset/MyDataset1'

    def test_remove_dates_from_title(self):
        dataset = Dataset()
        with pytest.raises(HDXError):
            dataset.remove_dates_from_title()
        assert 'title' not in dataset
        title = 'Title with no dates'
        dataset['title'] = title
        assert dataset.remove_dates_from_title() == list()
        assert dataset['title'] == title
        assert 'dataset_date' not in dataset
        assert dataset.remove_dates_from_title(set_dataset_date=True) == list()
        title = 'ICA Armenia, 2017 - Drought Risk, 1981-2015'
        dataset['title'] = title
        expected = [(datetime.datetime(1981, 1, 1, 0, 0), datetime.datetime(2015, 12, 31, 0, 0)),
                    (datetime.datetime(2017, 1, 1, 0, 0), datetime.datetime(2017, 12, 31, 0, 0))]
        assert dataset.remove_dates_from_title(change_title=False) == expected
        assert dataset['title'] == title
        assert 'dataset_date' not in dataset
        assert dataset.remove_dates_from_title() == expected
        newtitle = 'ICA Armenia - Drought Risk'
        assert dataset['title'] == newtitle
        assert 'dataset_date' not in dataset
        dataset['title'] = title
        assert dataset.remove_dates_from_title(set_dataset_date=True) == expected
        assert dataset['title'] == newtitle
        assert dataset['dataset_date'] == '01/01/1981-12/31/2015'
        assert dataset.remove_dates_from_title() == list()
        dataset['title'] = 'Mon_State_Village_Tract_Boundaries 9999 2001'
        expected = [(datetime.datetime(2001, 1, 1, 0, 0), datetime.datetime(2001, 12, 31, 0, 0))]
        assert dataset.remove_dates_from_title(set_dataset_date=True) == expected
        assert dataset['title'] == 'Mon_State_Village_Tract_Boundaries 9999'
        assert dataset['dataset_date'] == '01/01/2001-12/31/2001'
        dataset['title'] = 'Mon_State_Village_Tract_Boundaries 2001 99'
        assert dataset.remove_dates_from_title(set_dataset_date=True) == expected
        assert dataset['title'] == 'Mon_State_Village_Tract_Boundaries 99'
        assert dataset['dataset_date'] == '01/01/2001-12/31/2001'
        dataset['title'] = 'Mon_State_Village_Tract_Boundaries 9999 2001 99'
        assert dataset.remove_dates_from_title(set_dataset_date=True) == expected
        assert dataset['title'] == 'Mon_State_Village_Tract_Boundaries 9999 99'
        assert dataset['dataset_date'] == '01/01/2001-12/31/2001'

    def test_download_and_generate_resource(self, configuration):
        with temp_dir('test') as folder:
            url = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'
            hxltags = {'EVENT_ID_CNTY': '#event+code', 'EVENT_DATE': '#date+occurred', 'YEAR': '#date+year',
                       'EVENT_TYPE': '#event+type', 'ACTOR1': '#group+name+first', 'ASSOC_ACTOR_1':
                           '#group+name+first+assoc', 'ACTOR2': '#group+name+second', 'ASSOC_ACTOR_2':
                           '#group+name+second+assoc', 'REGION': '#region+name', 'COUNTRY': '#country+name',
                       'ADMIN1': '#adm1+name', 'ADMIN2': '#adm2+name', 'ADMIN3': '#adm3+name', 'LOCATION': '#loc+name',
                       'LATITUDE': '#geo+lat', 'LONGITUDE': '#geo+lon', 'SOURCE': '#meta+source', 'NOTES':
                           '#description', 'FATALITIES': '#affected+killed', 'ISO3': '#country+code'}

            filename = 'conflict_data_alg.csv'
            resourcedata = {
                'name': 'Conflict Data for Algeria',
                'description': 'Conflict data with HXL tags'
            }

            admin1s = set()

            def process_row(headers, row):
                row['lala'] = 'lala'
                admin1 = row.get('ADMIN1')
                if admin1 is not None:
                    admin1s.add(admin1)
                return row

            quickcharts = {'hashtag': '#event+code', 'values': ['1416RTA', 'XXXXRTA', '2231RTA'], 'cutdown': 2}
            dataset = Dataset()
            with Download(user_agent='test') as downloader:
                success, results = dataset.download_and_generate_resource(
                    downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'lala')],
                    row_function=process_row, yearcol='YEAR', quickcharts=quickcharts)
                assert success is True
                assert results == {'startdate': datetime.datetime(2001, 1, 1, 0, 0), 'enddate': datetime.datetime(2002, 12, 31, 0, 0), 'bites_disabled': [False, True, False],
                                   'headers': ['lala', 'GWNO', 'EVENT_ID_CNTY', 'EVENT_ID_NO_CNTY', 'EVENT_DATE', 'YEAR', 'TIME_PRECISION', 'EVENT_TYPE', 'ACTOR1', 'ALLY_ACTOR_1', 'INTER1', 'ACTOR2', 'ALLY_ACTOR_2', 'INTER2', 'INTERACTION', 'COUNTRY', 'ADMIN1', 'ADMIN2', 'ADMIN3', 'LOCATION', 'LATITUDE', 'LONGITUDE', 'GEO_PRECISION', 'SOURCE', 'NOTES', 'FATALITIES'],
                                   'rows': [{'lala': '', 'GWNO': '', 'EVENT_ID_CNTY': '#event+code', 'EVENT_ID_NO_CNTY': '', 'EVENT_DATE': '#date+occurred', 'YEAR': '#date+year', 'TIME_PRECISION': '', 'EVENT_TYPE': '#event+type', 'ACTOR1': '#group+name+first', 'ALLY_ACTOR_1': '', 'INTER1': '', 'ACTOR2': '#group+name+second', 'ALLY_ACTOR_2': '', 'INTER2': '', 'INTERACTION': '', 'COUNTRY': '#country+name', 'ADMIN1': '#adm1+name', 'ADMIN2': '#adm2+name', 'ADMIN3': '#adm3+name', 'LOCATION': '#loc+name', 'LATITUDE': '#geo+lat', 'LONGITUDE': '#geo+lon', 'GEO_PRECISION': '', 'SOURCE': '#meta+source', 'NOTES': '#description', 'FATALITIES': '#affected+killed'},
                                            {'GWNO': '615', 'EVENT_ID_CNTY': '1416RTA', 'EVENT_ID_NO_CNTY': '', 'EVENT_DATE': '18/04/2001', 'YEAR': '2001', 'TIME_PRECISION': '1', 'EVENT_TYPE': 'Violence against civilians', 'ACTOR1': 'Police Forces of Algeria (1999-)', 'ALLY_ACTOR_1': '', 'INTER1': '1', 'ACTOR2': 'Civilians (Algeria)', 'ALLY_ACTOR_2': 'Berber Ethnic Group (Algeria)', 'INTER2': '7', 'INTERACTION': '17', 'COUNTRY': 'Algeria', 'ADMIN1': 'Tizi Ouzou', 'ADMIN2': 'Beni-Douala', 'ADMIN3': '', 'LOCATION': 'Beni Douala', 'LATITUDE': '36.61954', 'LONGITUDE': '4.08282', 'GEO_PRECISION': '1', 'SOURCE': 'Associated Press Online', 'NOTES': 'A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.', 'FATALITIES': '1', 'lala': 'lala'},
                                            {'GWNO': '615', 'EVENT_ID_CNTY': '2229RTA', 'EVENT_ID_NO_CNTY': '', 'EVENT_DATE': '19/04/2001', 'YEAR': '2001', 'TIME_PRECISION': '1', 'EVENT_TYPE': 'Riots/Protests', 'ACTOR1': 'Rioters (Algeria)', 'ALLY_ACTOR_1': 'Berber Ethnic Group (Algeria)', 'INTER1': '5', 'ACTOR2': 'Police Forces of Algeria (1999-)', 'ALLY_ACTOR_2': '', 'INTER2': '1', 'INTERACTION': '15', 'COUNTRY': 'Algeria', 'ADMIN1': 'Tizi Ouzou', 'ADMIN2': 'Tizi Ouzou', 'ADMIN3': '', 'LOCATION': 'Tizi Ouzou', 'LATITUDE': '36.71183', 'LONGITUDE': '4.04591', 'GEO_PRECISION': '3', 'SOURCE': 'Kabylie report', 'NOTES': 'Riots were reported in numerous villages in Kabylie, resulting in dozens wounded in clashes between protesters and police and significant material damage.', 'FATALITIES': '0', 'lala': 'lala'},
                                            {'GWNO': '615', 'EVENT_ID_CNTY': '2230RTA', 'EVENT_ID_NO_CNTY': '', 'EVENT_DATE': '20/04/2001', 'YEAR': '2002', 'TIME_PRECISION': '1', 'EVENT_TYPE': 'Riots/Protests', 'ACTOR1': 'Protesters (Algeria)', 'ALLY_ACTOR_1': 'Students (Algeria)', 'INTER1': '6', 'ACTOR2': '', 'ALLY_ACTOR_2': '', 'INTER2': '0', 'INTERACTION': '60', 'COUNTRY': 'Algeria', 'ADMIN1': 'Bejaia', 'ADMIN2': 'Amizour', 'ADMIN3': '', 'LOCATION': 'Amizour', 'LATITUDE': '36.64022', 'LONGITUDE': '4.90131', 'GEO_PRECISION': '1', 'SOURCE': 'Crisis Group', 'NOTES': 'Students protested in the Amizour area. At least 3 were later arrested for allegedly insulting gendarmes.', 'FATALITIES': '0', 'lala': 'lala'},
                                            {'GWNO': '615', 'EVENT_ID_CNTY': '2231RTA', 'EVENT_ID_NO_CNTY': '', 'EVENT_DATE': '21/04/2001', 'YEAR': '2001', 'TIME_PRECISION': '1', 'EVENT_TYPE': 'Riots/Protests', 'ACTOR1': 'Rioters (Algeria)', 'ALLY_ACTOR_1': 'Berber Ethnic Group (Algeria)', 'INTER1': '5', 'ACTOR2': 'Police Forces of Algeria (1999-)', 'ALLY_ACTOR_2': '', 'INTER2': '1', 'INTERACTION': '15', 'COUNTRY': 'Algeria', 'ADMIN1': 'Bejaia', 'ADMIN2': 'Amizour', 'ADMIN3': '', 'LOCATION': 'Amizour', 'LATITUDE': '36.64022', 'LONGITUDE': '4.90131', 'GEO_PRECISION': '1', 'SOURCE': 'Kabylie report', 'NOTES': 'Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.', 'FATALITIES': '0', 'lala': 'lala'}],
                                   'qcheaders': ['EVENT_ID_CNTY', 'EVENT_DATE', 'YEAR', 'EVENT_TYPE', 'ACTOR1', 'ACTOR2', 'COUNTRY', 'ADMIN1', 'ADMIN2', 'ADMIN3', 'LOCATION', 'LATITUDE', 'LONGITUDE', 'SOURCE', 'NOTES', 'FATALITIES'],
                                   'qcrows': [{'EVENT_ID_CNTY': '#event+code', 'EVENT_DATE': '#date+occurred', 'YEAR': '#date+year', 'EVENT_TYPE': '#event+type', 'ACTOR1': '#group+name+first', 'ACTOR2': '#group+name+second', 'COUNTRY': '#country+name', 'ADMIN1': '#adm1+name', 'ADMIN2': '#adm2+name', 'ADMIN3': '#adm3+name', 'LOCATION': '#loc+name', 'LATITUDE': '#geo+lat', 'LONGITUDE': '#geo+lon', 'SOURCE': '#meta+source', 'NOTES': '#description', 'FATALITIES': '#affected+killed'},
                                              {'EVENT_ID_CNTY': '1416RTA', 'EVENT_DATE': '18/04/2001', 'YEAR': '2001', 'EVENT_TYPE': 'Violence against civilians', 'ACTOR1': 'Police Forces of Algeria (1999-)', 'ACTOR2': 'Civilians (Algeria)', 'COUNTRY': 'Algeria', 'ADMIN1': 'Tizi Ouzou', 'ADMIN2': 'Beni-Douala', 'ADMIN3': '', 'LOCATION': 'Beni Douala', 'LATITUDE': '36.61954', 'LONGITUDE': '4.08282', 'SOURCE': 'Associated Press Online', 'NOTES': 'A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.', 'FATALITIES': '1'}, {'EVENT_ID_CNTY': '2231RTA', 'EVENT_DATE': '21/04/2001', 'YEAR': '2001', 'EVENT_TYPE': 'Riots/Protests', 'ACTOR1': 'Rioters (Algeria)', 'ACTOR2': 'Police Forces of Algeria (1999-)', 'COUNTRY': 'Algeria', 'ADMIN1': 'Bejaia', 'ADMIN2': 'Amizour', 'ADMIN3': '', 'LOCATION': 'Amizour', 'LATITUDE': '36.64022', 'LONGITUDE': '4.90131', 'SOURCE': 'Kabylie report', 'NOTES': 'Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.', 'FATALITIES': '0'}]}
                assert dataset['dataset_date'] == '01/01/2001-12/31/2002'
                assert admin1s == {'Bejaia', 'Tizi Ouzou'}
                resources = dataset.get_resources()
                assert resources == [{'name': 'Conflict Data for Algeria', 'description': 'Conflict data with HXL tags', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                     {'name': 'QuickCharts-Conflict Data for Algeria', 'description': 'Cut down data for QuickCharts', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
                assert_files_same(join('tests', 'fixtures', 'gen_resource', filename), join(folder, filename))
                qc_filename = 'qc_%s' % filename
                assert_files_same(join('tests', 'fixtures', 'gen_resource', qc_filename), join(folder, qc_filename))

                def process_year(row):
                    year = row['YEAR']
                    if year == '2002':
                        return None
                    startdate, enddate = parse_date_range(year)
                    return {'startdate': startdate, 'enddate': enddate}

                quickcharts['cutdownhashtags'] = ['#event+code']
                del quickcharts['hashtag']
                success, results = dataset.download_and_generate_resource(
                    downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'lala')],
                    row_function=process_row, date_function=process_year, quickcharts=quickcharts)
                assert success is True
                assert results['startdate'] == datetime.datetime(2001, 1, 1, 0, 0)
                assert results['enddate'] == datetime.datetime(2001, 12, 31, 0, 0)
                assert dataset['dataset_date'] == '01/01/2001-12/31/2001'
                assert_files_same(join('tests', 'fixtures', 'gen_resource', 'min_%s' % qc_filename), join(folder, qc_filename))

                with pytest.raises(HDXError):
                    dataset.download_and_generate_resource(downloader, url, hxltags, folder, filename, resourcedata,
                                                           yearcol='YEAR', date_function=process_year)
                success, results = dataset.download_and_generate_resource(
                    downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'lala')],
                    row_function=process_row)
                assert success is True
                url = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/empty.csv'
                success, results = dataset.download_and_generate_resource(
                    downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'lala')],
                    row_function=process_row, yearcol='YEAR')
                assert success is False
                url = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/gen_resource/test_data_no_data.csv'
                success, results = dataset.download_and_generate_resource(
                    downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'lala')],
                    row_function=process_row, quickcharts=quickcharts)
                assert success is False
                url = 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/gen_resource/test_data_no_years.csv'
                success, results = dataset.download_and_generate_resource(downloader, url, hxltags, folder, filename,
                                                                 resourcedata, header_insertions=[(0, 'lala')],
                                                                 row_function=process_row, yearcol='YEAR')
                assert success is False
