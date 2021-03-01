# -*- coding: UTF-8 -*-
"""Dataset Tests (core methods)"""
import copy
import json
import re
import tempfile
from os import remove
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml

from hdx.data.dataset import Dataset, NotRequestableError
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.resource_view import ResourceView
from hdx.hdx_configuration import Configuration
from hdx.version import get_api_version
from . import MockResponse, dataset_data, resources_data, dataset_resultdict, dataset_mockshow, resultgroups
from .test_resource_view import resource_view_list, resource_view_mockcreate, resource_view_mocklist, \
    resource_view_mockshow
from .test_vocabulary import vocabulary_mockshow

searchdict = load_yaml(join('tests', 'fixtures', 'dataset_search_results.yml'))
dataset_list = ['acled-conflict-data-for-libya', 'acled-conflict-data-for-liberia', 'acled-conflict-data-for-lesotho',
                'acled-conflict-data-for-kenya', 'acled-conflict-data-for-guinea', 'acled-conflict-data-for-ghana',
                'acled-conflict-data-for-gambia', 'acled-conflict-data-for-gabon', 'acled-conflict-data-for-ethiopia',
                'acled-conflict-data-for-eritrea']
hxlupdate_list = [{'title': 'Quick Charts', 'resource_id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 
                   'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d', 'view_type': 'hdx_hxl_preview', 
                   'description': '', 'id': '29cc5894-4306-4bef-96ce-b7a833e7986a'}]
dataset_autocomplete = [{'match_field': 'name', 'match_displayed': 'acled-conflict-data-for-africa-1997-lastyear', 'name': 'acled-conflict-data-for-africa-1997-lastyear', 'title': 'ACLED Conflict Data for Africa 1997-2016'}]


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
        elif datadict['sort'] == 'metadata_created asc':
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


class TestDatasetCore:
    @pytest.fixture(scope='class')
    def test_file(self):
        return join('tests', 'fixtures', 'test_data.csv')

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_dataset_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_dataset_static.json')

    @pytest.fixture(scope='function')
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return dataset_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_revise(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                if isinstance(data, dict):
                    datadict = {k.decode('utf8'): v.decode('utf8') for k, v in data.items()}
                else:
                    datadict = json.loads(data.decode('utf-8'))
                if datadict['match'] == '{"name":"MyDataset1"}':
                    resultdictcopy = copy.deepcopy(dataset_resultdict)
                    resultdictcopy['resources'][0]['description'] = 'haha'
                    resultdictcopy = {'package': resultdictcopy}
                    result = json.dumps(resultdictcopy)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_revise"}' % result)
                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_revise"}')

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
                    return dataset_mockshow(url, datadict)
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
                    result = json.dumps(resources_data[0])
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_create"}' % result)
                if 'create' not in url and 'revise' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_create"}')
                if 'revise' in url:
                    datadict = json.loads(datadict['update'])
                if datadict['name'] == 'MyDataset1':
                    resultdictcopy = copy.deepcopy(dataset_resultdict)
                    resultdictcopy['state'] = datadict['state']
                    if 'revise' in url:
                        resultdictcopy = {'package': resultdictcopy}
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
                    return dataset_mockshow(url, datadict)
                if 'hxl' in url:
                    return mockhxlupdate(url, datadict)
                if 'resource' in url:
                    resultdictcopy = copy.deepcopy(resources_data[0])
                    merge_two_dictionaries(resultdictcopy, datadict)
                    result = json.dumps(resultdictcopy)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_update"}' % result)
                else:
                    if 'revise' not in url:
                        return MockResponse(404,
                                            '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}')
                    datadict = json.loads(datadict['update'])
                    if datadict['name'] in ['MyDataset1', 'DatasetExist']:
                        resultdictcopy = copy.deepcopy(dataset_resultdict)
                        merge_two_dictionaries(resultdictcopy, datadict)
                        for i, resource in enumerate(resultdictcopy['resources']):
                            for j, resource2 in enumerate(resultdictcopy['resources']):
                                if i != j:
                                    if resource == resource2:
                                        del resultdictcopy['resources'][j]
                                        break
                            resource['package_id'] = resultdictcopy['id']
                        resultdictcopy = {'package': resultdictcopy}
                        result = json.dumps(resultdictcopy)
                        return MockResponse(200,
                                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=dataset_update"}' % result)
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
                    return dataset_mockshow(url, datadict)
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
                    return dataset_mockshow(url, datadict)
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
    def post_autocomplete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'autocomplete' not in url or 'acled' not in datadict['q']:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not autocomplete", "__type": "TEST ERROR: Not Autocomplete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_autocomplete"}')
                result = json.dumps(dataset_autocomplete)
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_autocomplete"}' % result)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read):
        dataset = Dataset.read_from_hdx('TEST1')
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['name'] == 'MyDataset1'
        assert dataset['dataset_date'] == '06/04/2016'
        assert dataset.number_of_resources() == 3
        dataset = Dataset.read_from_hdx('TEST2')
        assert dataset is None
        dataset = Dataset.read_from_hdx('TEST3')
        assert dataset is None

    def test_revise(self, configuration, test_file, post_revise):
        dataset = Dataset.revise({'name': 'MyDataset1'}, filter=['-resources__0__description'], update={'resources': [{'description': 'haha'}]}, files_to_upload={'update__resources__0__upload': test_file})
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['name'] == 'MyDataset1'
        assert dataset['dataset_date'] == '06/04/2016'
        assert dataset.get_resource()['description'] == 'haha'
        with pytest.raises(HDXError):
            Dataset.revise({'name': 'MyDataset1'}, filter=['-resources__0__description'], update={'resources': [{'description': 'haha'}]}, files_to_upload={'update__resources__0__upload': 'NOTEXIST'})
        with pytest.raises(HDXError):
            dataset._write_to_hdx('revise', dict(), id_field_name='', files_to_upload={'update__resources__0__upload': 'NOTEXIST'})

    def test_create_in_hdx(self, configuration, post_create):
        dataset = Dataset()
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset['id'] = 'TEST1'
        dataset['name'] = 'LALA'
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()
        dataset.create_in_hdx(allow_no_resources=True)
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3

        datasetdata['name'] = 'MyDataset2'
        dataset = Dataset(datasetdata)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        datasetdata['name'] = 'MyDataset3'
        dataset = Dataset(datasetdata)
        with pytest.raises(HDXError):
            dataset.create_in_hdx()

        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
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

        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        datasetdata['resources'] = resourcesdata
        dataset = Dataset(datasetdata)
        assert len(dataset.get_resources()) == 3
        del datasetdata['resources']
        uniqueval = 'myconfig2'
        config.unique = uniqueval
        dataset = Dataset(datasetdata, configuration=config)
        del resourcesdata[0]['id']
        del resourcesdata[1]['id']
        dataset.add_update_resources(resourcesdata)
        dataset.create_in_hdx()
        assert dataset['id'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert len(dataset.resources) == 3
        assert dataset.resources[0].configuration.unique == uniqueval
        assert dataset['state'] == 'active'
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        resource = Resource(resourcesdata[0])
        file = tempfile.NamedTemporaryFile(delete=False)
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        dataset.create_in_hdx()
        remove(file.name)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
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
        resourceviewdata = {'title': 'Quick Charts', 'resource_name': 'Resource1', 'view_type': 'hdx_hxl_preview'}
        dataset.preview_resourceview = ResourceView(resourceviewdata)
        dataset.update_in_hdx()
        assert dataset.preview_resourceview is None
        dataset.update_in_hdx(updated_by_script='hi')
        pattern = r'hi \([12]\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d.\d\d\d\d\d\d\)'
        match = re.search(pattern, dataset['updated_by_script'])
        assert match
        dataset.update_in_hdx(updated_by_script='hi', batch='6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d')
        match = re.search(pattern, dataset['updated_by_script'])
        assert match
        assert dataset['batch'] == '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d'
        assert dataset['batch_mode'] == 'DONT_GROUP'
        with pytest.raises(HDXError):
            dataset.update_in_hdx(updated_by_script='hi', batch='1234')

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
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        dataset['id'] = 'NOTEXIST'
        dataset['name'] = 'DatasetExist'
        dataset.create_in_hdx(allow_no_resources=True)
        assert dataset['name'] == 'DatasetExist'
        datasetdata['name'] = 'MyDataset1'
        datasetdata['id'] = 'TEST1'
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3

        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resources([resource, resource])
        dataset.create_in_hdx(match_resources_by_metadata=False)
        assert dataset['id'] == 'TEST1'
        assert dataset['dataset_date'] == '06/04/2016'
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        dataset.update_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        dataset = Dataset.read_from_hdx('TEST4')
        dataset['id'] = 'TEST4'
        assert dataset['state'] == 'active'
        dataset.update_in_hdx()
        assert len(dataset.resources) == 3
        dataset = Dataset.read_from_hdx('TEST4')
        file = tempfile.NamedTemporaryFile(delete=False)
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(batch='6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d')
        assert len(dataset.resources) == 3
        resource['name'] = '123'
        resource.set_file_to_upload(None)
        resource['url'] = 'http://lala'
        dataset.add_update_resource(resource)
        dataset.update_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        del resource['id']
        resource['name'] = 'Resource1'
        dataset.add_update_resource(resource)
        dataset.update_in_hdx()
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 4

        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resource(resource)
        resource = Resource(resourcesdata[1])
        dataset.add_update_resource(resource)
        resource = Resource(resourcesdata[0])
        resource['name'] = 'ResourcePosition'
        resource['id'] = '123'
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        resource = Resource(resourcesdata[0])
        resource['name'] = 'changed name'
        resource['id'] = '456'
        resource.set_file_to_upload(file.name)
        dataset.update_in_hdx(match_resources_by_metadata=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 4

        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resource(resource)
        resource = Resource(resourcesdata[1])
        dataset.add_update_resource(resource)
        resource = Resource(resourcesdata[0])
        resource['name'] = 'ResourcePosition'
        resource['id'] = '123'
        resource.set_file_to_upload(file.name)
        dataset.add_update_resource(resource)
        resource = dataset.get_resources()[0]
        resource['name'] = 'changed name'
        resource['id'] = '456'
        resource.set_file_to_upload(file.name)
        dataset.update_in_hdx(match_resources_by_metadata=False)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        remove(file.name)
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(remove_additional_resources=False)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 3
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(remove_additional_resources=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 1
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        dataset.add_update_resource(resource)
        dataset.update_in_hdx(match_resources_by_metadata=False, remove_additional_resources=True)
        assert dataset['state'] == 'active'
        assert len(dataset.resources) == 1

    def test_delete_from_hdx(self, configuration, post_delete):
        dataset = Dataset.read_from_hdx('TEST1')
        dataset.delete_from_hdx()
        del dataset['id']
        with pytest.raises(HDXError):
            dataset.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
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
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
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
        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        dataset = Dataset(datasetdata)
        dataset.add_update_resources(resourcesdata)
        dataset.add_update_resources(resourcesdata)
        assert len(dataset.resources) == 3
        dataset.delete_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d6')
        assert len(dataset.resources) == 3
        dataset.delete_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d5')
        assert len(dataset.resources) == 2
        resourcesdata = copy.deepcopy(resources_data)
        resource = Resource(resourcesdata[0])
        resource.set_file_to_upload('lala')
        dataset.add_update_resource(resource)
        assert dataset.resources[2].get_file_to_upload() == 'lala'
        dataset.add_update_resource('de6549d8-268b-4dfe-adaf-a4ae5c8510d5')
        assert len(dataset.resources) == 3
        with pytest.raises(HDXError):
            dataset.add_update_resource(123)
        with pytest.raises(HDXError):
            dataset.add_update_resource('123')
        resourcesdata[0]['package_id'] = '123'
        with pytest.raises(HDXError):
            dataset.add_update_resources(resourcesdata)
        with pytest.raises(HDXError):
            dataset.add_update_resources(123)
        with pytest.raises(HDXError):
            dataset.delete_resource('NOTEXIST')
        datasetdata['resources'] = resourcesdata
        dataset = Dataset(datasetdata)
        assert len(dataset.resources) == 3
        resource = copy.deepcopy(resources_data[0])
        del resource['id']
        dataset.add_update_resource(resource)
        assert len(dataset.resources) == 3
        dataset.add_update_resources([resource])
        assert len(dataset.resources) == 3
        resource = copy.deepcopy(resources_data[2])
        del resource['id']
        dataset.add_update_resource(resource)
        assert len(dataset.resources) == 3
        dataset.add_update_resources([resource])
        assert len(dataset.resources) == 3
        resource['format'] = 'mdb'
        dataset.add_update_resource(resource)
        assert len(dataset.resources) == 4
        resource = copy.deepcopy(resources_data[2])
        del resource['id']
        resource['format'] = 'doc'
        dataset.add_update_resources([resource])
        assert len(dataset.resources) == 5
        resource['format'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            dataset.add_update_resource(resource)

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

    def test_hdxconnect(self, configuration, post_create):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
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
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        resourcesdata = copy.deepcopy(resources_data)
        dataset.add_update_resources(resourcesdata)
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
        assert dataset.get_filetypes() == ['xlsx', 'csv', 'xls']

    def test_autocomplete(self, configuration, post_autocomplete):
        assert Dataset.autocomplete('acled') == dataset_autocomplete
