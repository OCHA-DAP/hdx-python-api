# -*- coding: UTF-8 -*-
"""Showcase Tests"""
import copy
import json
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml

from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.hdx_configuration import Configuration
from .test_vocabulary import vocabulary_mockshow
from . import MockResponse

showcase_resultdict = {
    'relationships_as_object': [],
    'num_tags': 2,
    'id': '05e392bf-04e0-4ca6-848c-4e87bba10746',
    'metadata_created': '2017-07-03T07:50:49.474517',
    'metadata_modified': '2017-07-03T08:12:43.726624',
    'author': 'Dan Mihaila',
    'author_email': 'dan@gmail.com',
    'state': 'active',
    'creator_user_id': '060468e4-2f33-4488-8504-c4b10cc34821',
    'type': 'showcase',
    'resources': [],
    'tags': [
        {
            'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b',
            'state': 'active',
            'display_name': 'economics',
            'id': 'e1ff4b78-efcf-40e7-888b-7342169068e1',
            'name': 'economics'
        },
        {
            'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b',
            'state': 'active',
            'display_name': 'health',
            'id': '8a43b269-78cb-4a85-a4ce-7e3e928bc487',
            'name': 'health'
        }
    ],
    'groups': [],
    'relationships_as_subject': [],
    'total_res_downloads': 0,
    'num_datasets': 2,
    'showcase_notes_formatted': '<p>lalala</p>',
    'image_url': 'visual.png',
    'revision_id': 'cc64364b-ede2-400a-bb9f-8e585a4f6399',
    'notes': 'My Showcase',
    'url': 'http://visualisation/url/',
    'title': 'MyShowcase1',
    'image_display_url': 'http://myvisual/visual.png',
    'name': 'showcase-1'
}

datasetsdict = load_yaml(join('tests', 'fixtures', 'dataset_all_results.yml'))
allsearchdict = load_yaml(join('tests', 'fixtures', 'showcase_all_search_results.yml'))


def mockshow(url, datadict):
    if 'package_list' in url:
        result = json.dumps(datasetsdict)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_list"}' % result)
    if '_show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}')
    result = json.dumps(showcase_resultdict)
    if datadict['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746' or datadict['id'] == 'TEST1':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}' % result)
    if datadict['id'] == 'TEST2':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}')
    if datadict['id'] == 'TEST3':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}')


def mockallsearch(url, datadict):
    if 'search' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not Search Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    if 'metadata_modified' in datadict['fq']:
        newsearchdict = copy.deepcopy(allsearchdict)
        newsearchdict['results'] = [newsearchdict['results'][0]]
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                newsearchdict))
    if datadict['q'] == 'ACLED':
        newsearchdict = copy.deepcopy(allsearchdict)
        if datadict['rows'] == 11:
            newsearchdict['results'].append(newsearchdict['results'][0])
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                    newsearchdict))
        elif datadict['rows'] == 6:
            if datadict['start'] == 2:
                newsearchdict['results'] = newsearchdict['results'][2:8]
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                        newsearchdict))
        elif datadict['rows'] == 5:
            if datadict['start'] == 0:
                newsearchdict['count'] = 5
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                        newsearchdict[:5]))
            elif datadict['start'] == 5:
                newsearchdict['count'] = 6  # return wrong count
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                        newsearchdict[:5]))
            else:
                newsearchdict['count'] = 0
                newsearchdict['results'] = list()
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                        newsearchdict))
        else:
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                    allsearchdict))
    if datadict['q'] == '*:*':
        newsearchdict = copy.deepcopy(allsearchdict)
        newsearchdict['results'].extend(copy.deepcopy(newsearchdict['results']))
        for i, x in enumerate(newsearchdict['results']):
            x['id'] = '%s%d' % (x['id'], i)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                newsearchdict))
    if datadict['q'] == '"':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Validation Error", "__type": "Validation Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    if datadict['q'] == 'ajyhgr':
        return MockResponse(200,
                            '{"success": true, "result": {"count": 0, "results": []}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')


class TestShowcase:
    showcase_data = {
        'title': 'MyShowcase1',
        'notes': 'My Showcase',
        'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
        'image_url': 'http://myvisual/visual.png',
        'name': 'showcase-1',
        'url': 'http://visualisation/url/',
        'tags': [{'name': 'economy'}, {'name': 'health'}],
        'dataset_ids': ['a89c6260-6392-416e-bcbc-eb2c5f1d7add']
    }
    association = None

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_showcase_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_showcase_static.json')

    @pytest.fixture(scope='function')
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'association_delete' in url:
                    TestShowcase.association = 'delete'
                    return MockResponse(200,
                                        '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_delete"}')
                elif 'association_create' in url:
                    TestShowcase.association = 'create'
                    result = json.dumps(datadict)
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_create"}' % result)
                return mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_create(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'vocabulary' in url:
                    return vocabulary_mockshow(url, datadict)
                if url.endswith('show') or 'list' in url:
                    return mockshow(url, datadict)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

                resultdictcopy = copy.deepcopy(showcase_resultdict)
                resultdictcopy['state'] = datadict['state']
                result = json.dumps(resultdictcopy)
                if datadict['title'] == 'MyShowcase1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}' % result)
                if datadict['title'] == 'MyShowcase2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')
                if datadict['title'] == 'MyShowcase3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_update(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'vocabulary' in url:
                    return vocabulary_mockshow(url, datadict)
                if url.endswith('show') or 'list' in url:
                    return mockshow(url, datadict)
                if 'update' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')
                resultdictcopy = copy.deepcopy(showcase_resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict['title'] == 'MyShowcase1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}' % result)
                if datadict['title'] == 'MyShowcase2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')
                if datadict['title'] == 'MyShowcase3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_delete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if url.endswith('show') or 'list' in url:
                    return mockshow(url, datadict)
                if 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_delete"}')
                if datadict['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_delete"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def allsearch(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mockallsearch(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read):
        showcase = Showcase.read_from_hdx('05e392bf-04e0-4ca6-848c-4e87bba10746')
        assert showcase['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'
        assert showcase['title'] == 'MyShowcase1'
        showcase = Showcase.read_from_hdx('TEST2')
        assert showcase is None
        showcase = Showcase.read_from_hdx('TEST3')
        assert showcase is None

    def test_create_in_hdx(self, configuration, post_create):
        showcase = Showcase()
        with pytest.raises(HDXError):
            showcase.create_in_hdx()
        showcase['id'] = '05e392bf-04e0-4ca6-848c-4e87bba10746'
        showcase['title'] = 'LALA'
        with pytest.raises(HDXError):
            showcase.create_in_hdx()

        showcase_data = copy.deepcopy(TestShowcase.showcase_data)
        showcase = Showcase(showcase_data)
        showcase.create_in_hdx()
        assert showcase['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'
        assert showcase['state'] == 'active'

        showcase_data['title'] = 'MyShowcase2'
        showcase = Showcase(showcase_data)
        with pytest.raises(HDXError):
            showcase.create_in_hdx()

        showcase_data['title'] = 'MyShowcase3'
        showcase = Showcase(showcase_data)
        with pytest.raises(HDXError):
            showcase.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        showcase = Showcase()
        showcase['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            showcase.update_in_hdx()
        showcase['title'] = 'LALA'
        with pytest.raises(HDXError):
            showcase.update_in_hdx()

        showcase = Showcase.read_from_hdx('05e392bf-04e0-4ca6-848c-4e87bba10746')
        assert showcase['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'
        assert showcase['title'] == 'MyShowcase1'

        showcase['name'] = 'TEST1'
        showcase['notes'] = 'lalalala'
        showcase.update_in_hdx()
        assert showcase['name'] == 'TEST1'
        assert showcase['notes'] == 'lalalala'
        assert showcase['state'] == 'active'
        expected = copy.deepcopy(showcase_resultdict)
        expected['notes'] = 'lalalala'
        expected['name'] = 'TEST1'
        expected['tags'] = [{'name': 'economics', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'}, {'name': 'health', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b'}]
        assert showcase.get_old_data_dict() == expected

        showcase['name'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            showcase.update_in_hdx()

        del showcase['name']
        with pytest.raises(HDXError):
            showcase.update_in_hdx()

        showcase_data = copy.deepcopy(TestShowcase.showcase_data)
        showcase_data['title'] = 'MyShowcase1'
        showcase_data['name'] = 'TEST1'
        showcase = Showcase(showcase_data)
        showcase.create_in_hdx()
        assert showcase['name'] == 'TEST1'
        assert showcase['notes'] == 'My Showcase'
        assert showcase['state'] == 'active'

    def test_delete_from_hdx(self, configuration, post_delete):
        showcase = Showcase.read_from_hdx('05e392bf-04e0-4ca6-848c-4e87bba10746')
        showcase.delete_from_hdx()
        del showcase['id']
        with pytest.raises(HDXError):
            showcase.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        showcase_data = copy.deepcopy(TestShowcase.showcase_data)
        showcase = Showcase(showcase_data)
        assert showcase['title'] == 'MyShowcase1'
        assert showcase['name'] == 'showcase-1'
        showcase.update_from_yaml(static_yaml)
        assert showcase['title'] == 'MyShowcase1'
        assert showcase['name'] == 'my-showcase-1'

    def test_update_json(self, configuration, static_json):
        showcase_data = copy.deepcopy(TestShowcase.showcase_data)
        showcase = Showcase(showcase_data)
        assert showcase['title'] == 'MyShowcase1'
        assert showcase['name'] == 'showcase-1'
        showcase.update_from_json(static_json)
        assert showcase['title'] == 'MyShowcase1'
        assert showcase['name'] == 'new-showcase-1'

    def test_tags(self, configuration):
        showcase_data = copy.deepcopy(TestShowcase.showcase_data)
        showcase = Showcase(showcase_data)
        assert showcase.get_tags() == ['economy', 'health']
        showcase.add_tag('wash')
        assert showcase.get_tags() == ['economy', 'health', 'water sanitation and hygiene - wash']
        showcase.add_tags(['sanitation'])
        assert showcase.get_tags() == ['economy', 'health', 'water sanitation and hygiene - wash']
        result = showcase.remove_tag('water sanitation and hygiene - wash')
        assert result is True
        assert showcase.get_tags() == ['economy', 'health']
        showcase['tags'] = None
        result = showcase.remove_tag('wash')
        assert result is False

    def test_datasets(self, configuration, read):
        showcase = Showcase.read_from_hdx('05e392bf-04e0-4ca6-848c-4e87bba10746')
        datasets = showcase.get_datasets()
        assert len(datasets) == 10
        assert datasets[0].data == datasetsdict[0]
        dict4 = copy.deepcopy(datasetsdict[4])
        del dict4['resources']
        assert datasets[4].data == dict4
        TestShowcase.association = None
        showcase.remove_dataset(datasets[0])
        assert TestShowcase.association == 'delete'
        TestShowcase.association = None
        assert showcase.add_dataset('a2f32edd-bac2-4940-aa58-49e565041055') is True
        assert TestShowcase.association == 'create'
        TestShowcase.association = None
        assert showcase.add_datasets([{'id': 'a2f32edd-bac2-4940-aa58-49e565041055'}, {'id': '6a5aebc1-f5a9-4842-8183-b8118228e71e'}]) is False
        assert TestShowcase.association == 'create'
        TestShowcase.association = None
        assert showcase.add_dataset({'name': 'TEST1'}) is True
        assert TestShowcase.association == 'create'
        TestShowcase.association = None
        with pytest.raises(HDXError):
            showcase.add_dataset('123')
        with pytest.raises(HDXError):
            showcase.add_dataset(123)

    def test_search_in_hdx(self, configuration, allsearch):
        showcases = Showcase.search_in_hdx('ACLED')
        assert len(showcases) == 10
        showcases = Showcase.search_in_hdx('ACLED', offset=2, limit=6)
        assert len(showcases) == 6
        showcases = Showcase.search_in_hdx(fq='metadata_modified:[2018-01-01T00:00:00.000Z TO NOW]')
        assert len(showcases) == 1
        showcases = Showcase.search_in_hdx('ajyhgr')
        assert len(showcases) == 0
        with pytest.raises(HDXError):
            Showcase.search_in_hdx('"')
        with pytest.raises(HDXError):
            Showcase.search_in_hdx('ACLED', rows=11)
        with pytest.raises(HDXError):
            # Test returned row counts per page mismatch (wrong count of 6 purposely in mocksearch)
            Showcase.search_in_hdx('ACLED', page_size=5)

    def test_get_all_showcases(self, configuration, allsearch):
        showcases = Showcase.get_all_showcases()
        assert len(showcases) == 20
