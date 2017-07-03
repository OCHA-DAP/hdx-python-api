# -*- coding: UTF-8 -*-
"""ShowcaseItem Tests"""
import copy
import json
from os.path import join

import pytest
import requests

from hdx.data.hdxobject import HDXError
from hdx.data.showcaseitem import ShowcaseItem
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml
from . import MockResponse

resultdict = {
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
            'vocabulary_id': '57f71f5f-adb0-48fd-ab2c-6b93b9d30332',
            'state': 'active',
            'display_name': 'economy',
            'id': 'c69c6fa1-7b2d-403e-a386-c57272da505d',
            'name': 'economy'
        },
        {
            'vocabulary_id': '57f71f5f-adb0-48fd-ab2c-6b93b9d30332',
            'state': 'active',
            'display_name': 'health',
            'id': 'cfc3fb43-78a5-45e0-8bac-fbb81d00d211',
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
    'notes': 'My ShowcaseItem',
    'url': 'http://visualisation/url/',
    'title': 'MyShowcaseItem1',
    'image_display_url': 'http://myvisual/visual.png',
    'name': 'showcase-item-1'
}

datasetsdict = load_yaml(join('tests', 'fixtures', 'dataset_all_results.yml'))


def mockshow(url, datadict):
    if 'list' in url:
        result = json.dumps(datasetsdict)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_list"}' % result)
    if '_show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == 'TEST1':
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


def mockassociate(url, datadict):
    if 'association_delete' in url:
        return MockResponse(200,
                            '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_delete"}')
    elif 'association_create' in url:
        result = json.dumps(datadict)
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_create"}' % result)


class TestShowcaseItem:
    showcaseitem_data = {
        'title': 'MyShowcaseItem1',
        'notes': 'My ShowcaseItem',
        'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
        'image_display_url': 'http://myvisual/visual.png',
        'name': 'showcase-item-1',
        'url': 'http://visualisation/url/',
        'tags': [{'name': 'economy'}, {'name': 'health'}],
        'dataset_ids': ['a89c6260-6392-416e-bcbc-eb2c5f1d7add']
    }

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_showcaseitem_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_showcaseitem_static.json')

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
                datadict = json.loads(data.decode('utf-8'))
                if url.endswith('show') or 'list' in url:
                    return mockshow(url, datadict)
                if 'association' in url:
                    return mockassociate(url, datadict)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

                result = json.dumps(resultdict)
                if datadict['title'] == 'MyShowcaseItem1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}' % result)
                if datadict['title'] == 'MyShowcaseItem2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')
                if datadict['title'] == 'MyShowcaseItem3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_create"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_update(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                datadict = json.loads(data.decode('utf-8'))
                if url.endswith('show') or 'list' in url:
                    return mockshow(url, datadict)
                if 'association' in url:
                    return mockassociate(url, datadict)
                if 'update' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict['title'] == 'MyShowcaseItem1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}' % result)
                if datadict['title'] == 'MyShowcaseItem2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')
                if datadict['title'] == 'MyShowcaseItem3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_update"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_delete(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
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

        monkeypatch.setattr(requests, 'Session', MockSession)

    def test_read_from_hdx(self, configuration, read):
        showcaseitem = ShowcaseItem.read_from_hdx('TEST1')
        assert showcaseitem['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'
        assert showcaseitem['title'] == 'MyShowcaseItem1'
        assert showcaseitem['dataset_ids'][7] == 'a89c6260-6392-416e-bcbc-eb2c5f1d7add'
        showcaseitem = ShowcaseItem.read_from_hdx('TEST2')
        assert showcaseitem is None
        showcaseitem = ShowcaseItem.read_from_hdx('TEST3')
        assert showcaseitem is None

    def test_create_in_hdx(self, configuration, post_create):
        showcaseitem = ShowcaseItem()
        with pytest.raises(HDXError):
            showcaseitem.create_in_hdx()
        showcaseitem['id'] = 'TEST1'
        showcaseitem['title'] = 'LALA'
        with pytest.raises(HDXError):
            showcaseitem.create_in_hdx()

        showcaseitem_data = copy.deepcopy(TestShowcaseItem.showcaseitem_data)
        showcaseitem = ShowcaseItem(showcaseitem_data)
        showcaseitem.create_in_hdx()
        assert showcaseitem['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'

        showcaseitem_data['title'] = 'MyShowcaseItem2'
        showcaseitem = ShowcaseItem(showcaseitem_data)
        with pytest.raises(HDXError):
            showcaseitem.create_in_hdx()

        showcaseitem_data['title'] = 'MyShowcaseItem3'
        showcaseitem = ShowcaseItem(showcaseitem_data)
        with pytest.raises(HDXError):
            showcaseitem.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        showcaseitem = ShowcaseItem()
        showcaseitem['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            showcaseitem.update_in_hdx()
        showcaseitem['title'] = 'LALA'
        with pytest.raises(HDXError):
            showcaseitem.update_in_hdx()

        showcaseitem = ShowcaseItem.read_from_hdx('TEST1')
        assert showcaseitem['id'] == '05e392bf-04e0-4ca6-848c-4e87bba10746'
        assert showcaseitem['title'] == 'MyShowcaseItem1'

        showcaseitem['id'] = 'TEST1'
        showcaseitem['notes'] = 'lalalala'
        showcaseitem.update_in_hdx()
        assert showcaseitem['id'] == 'TEST1'
        assert showcaseitem['notes'] == 'lalalala'
        expected = copy.deepcopy(resultdict)
        expected['notes'] = 'lalalala'
        expected['id'] = 'TEST1'
        expected['dataset_ids'] = ['6a5aebc1-f5a9-4842-8183-b8118228e71e', 'b2f32edd-bac2-4940-aa58-49e565041056',
                                   '86e6d416-d2e8-499d-b2aa-6c75ea931f19', '8fc7bcd9-5daa-44a8-b219-e707af2cd4a8',
                                   '87a5dbbc-db76-4a0f-a20f-5210a20a3bc9', '7ba76fc6-22aa-4295-be20-39ccaa1d0c0c',
                                   '13398455-5826-4420-b7e4-af22ff9b4061', 'a89c6260-6392-416e-bcbc-eb2c5f1d7add',
                                   '5ea80d40-ef69-43b7-9baf-4bd4cafb5965', 'd80ef63d-6b5e-4188-9e33-654155e03013']
        assert showcaseitem.get_old_data_dict() == expected

        showcaseitem['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            showcaseitem.update_in_hdx()

        del showcaseitem['id']
        with pytest.raises(HDXError):
            showcaseitem.update_in_hdx()

        showcaseitem_data = copy.deepcopy(TestShowcaseItem.showcaseitem_data)
        showcaseitem_data['title'] = 'MyShowcaseItem1'
        showcaseitem_data['id'] = 'TEST1'
        showcaseitem = ShowcaseItem(showcaseitem_data)
        showcaseitem.create_in_hdx()
        assert showcaseitem['id'] == 'TEST1'
        assert showcaseitem['notes'] == 'My ShowcaseItem'

    def test_delete_from_hdx(self, configuration, post_delete):
        showcaseitem = ShowcaseItem.read_from_hdx('TEST1')
        showcaseitem.delete_from_hdx()
        del showcaseitem['id']
        with pytest.raises(HDXError):
            showcaseitem.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        showcaseitem_data = copy.deepcopy(TestShowcaseItem.showcaseitem_data)
        showcaseitem = ShowcaseItem(showcaseitem_data)
        assert showcaseitem['title'] == 'MyShowcaseItem1'
        assert showcaseitem['name'] == 'showcase-item-1'
        showcaseitem.update_from_yaml(static_yaml)
        assert showcaseitem['title'] == 'MyShowcaseItem1'
        assert showcaseitem['name'] == 'my-showcase-item-1'

    def test_update_json(self, configuration, static_json):
        showcaseitem_data = copy.deepcopy(TestShowcaseItem.showcaseitem_data)
        showcaseitem = ShowcaseItem(showcaseitem_data)
        assert showcaseitem['title'] == 'MyShowcaseItem1'
        assert showcaseitem['name'] == 'showcase-item-1'
        showcaseitem.update_from_json(static_json)
        assert showcaseitem['title'] == 'MyShowcaseItem1'
        assert showcaseitem['name'] == 'new-showcase-item-1'
