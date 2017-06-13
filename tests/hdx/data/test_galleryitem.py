# -*- coding: UTF-8 -*-
"""GalleryItem Tests"""
import copy
import json
from os.path import join

import pytest
import requests

from hdx.data.galleryitem import GalleryItem
from hdx.data.hdxobject import HDXError
from hdx.utilities.dictandlist import merge_two_dictionaries


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


resultdict = {
    'description': 'My GalleryItem',
    '__extras': {
        'view_count': 1
    },
    'url': 'http://visualisation/url/',
    'title': 'MyGalleryItem1',
    'featured': 0,
    'image_url': 'http://myvisual/visual.png',
    'type': 'visualization',
    'id': '2f90d964-f980-4513-ad1b-5df6b2d044ff',
    'owner_id': '196196be-6037-4488-8b71-d786adf4c081'
}


def mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == 'TEST1':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_show"}' % result)
    if datadict['id'] == 'TEST2':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_show"}')
    if datadict['id'] == 'TEST3':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_show"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_show"}')


class TestGalleryItem:
    galleryitem_data = {
        'title': 'MyGalleryItem1',
        'description': 'My GalleryItem',
        'dataset_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
        'image_url': 'http://myvisual/visual.png',
        'type': 'visualization',
        'url': 'http://visualisation/url/'
    }

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_galleryitem_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_galleryitem_static.json')

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
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_create"}')

                result = json.dumps(resultdict)
                if datadict['title'] == 'MyGalleryItem1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_create"}' % result)
                if datadict['title'] == 'MyGalleryItem2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_create"}')
                if datadict['title'] == 'MyGalleryItem3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_create"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_create"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_update(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return mockshow(url, datadict)
                if 'update' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_update"}')
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict['title'] == 'MyGalleryItem1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_update"}' % result)
                if datadict['title'] == 'MyGalleryItem2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_update"}')
                if datadict['title'] == 'MyGalleryItem3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_update"}')

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
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_delete"}')
                if datadict['id'] == '2f90d964-f980-4513-ad1b-5df6b2d044ff':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=related_delete"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    def test_read_from_hdx(self, configuration, read):
        galleryitem = GalleryItem.read_from_hdx('TEST1')
        assert galleryitem['id'] == '2f90d964-f980-4513-ad1b-5df6b2d044ff'
        assert galleryitem['title'] == 'MyGalleryItem1'
        galleryitem = GalleryItem.read_from_hdx('TEST2')
        assert galleryitem is None
        galleryitem = GalleryItem.read_from_hdx('TEST3')
        assert galleryitem is None

    def test_create_in_hdx(self, configuration, post_create):
        galleryitem = GalleryItem()
        with pytest.raises(HDXError):
            galleryitem.create_in_hdx()
        galleryitem['id'] = 'TEST1'
        galleryitem['title'] = 'LALA'
        with pytest.raises(HDXError):
            galleryitem.create_in_hdx()

        galleryitem_data = copy.deepcopy(TestGalleryItem.galleryitem_data)
        galleryitem = GalleryItem(galleryitem_data)
        galleryitem.create_in_hdx()
        assert galleryitem['id'] == '2f90d964-f980-4513-ad1b-5df6b2d044ff'

        galleryitem_data['title'] = 'MyGalleryItem2'
        galleryitem = GalleryItem(galleryitem_data)
        with pytest.raises(HDXError):
            galleryitem.create_in_hdx()

        galleryitem_data['title'] = 'MyGalleryItem3'
        galleryitem = GalleryItem(galleryitem_data)
        with pytest.raises(HDXError):
            galleryitem.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        galleryitem = GalleryItem()
        galleryitem['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            galleryitem.update_in_hdx()
        galleryitem['title'] = 'LALA'
        with pytest.raises(HDXError):
            galleryitem.update_in_hdx()

        galleryitem = GalleryItem.read_from_hdx('TEST1')
        assert galleryitem['id'] == '2f90d964-f980-4513-ad1b-5df6b2d044ff'
        assert galleryitem['type'] == 'visualization'

        galleryitem['type'] = 'paper'
        galleryitem['id'] = 'TEST1'
        galleryitem['title'] = 'MyGalleryItem1'
        galleryitem.update_in_hdx()
        assert galleryitem['id'] == 'TEST1'
        assert galleryitem['type'] == 'paper'
        assert galleryitem.get_old_data_dict() == {'__extras': {'view_count': 1},
                                                   'description': 'My GalleryItem',
                                                   'featured': 0, 'id': 'TEST1',
                                                   'image_url': 'http://myvisual/visual.png',
                                                   'owner_id': '196196be-6037-4488-8b71-d786adf4c081',
                                                   'title': 'MyGalleryItem1', 'type': 'paper',
                                                   'url': 'http://visualisation/url/'}

        galleryitem['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            galleryitem.update_in_hdx()

        del galleryitem['id']
        with pytest.raises(HDXError):
            galleryitem.update_in_hdx()

        galleryitem_data = copy.deepcopy(TestGalleryItem.galleryitem_data)
        galleryitem_data['title'] = 'MyGalleryItem1'
        galleryitem_data['id'] = 'TEST1'
        galleryitem = GalleryItem(galleryitem_data)
        galleryitem.create_in_hdx()
        assert galleryitem['id'] == 'TEST1'
        assert galleryitem['type'] == 'visualization'

    def test_delete_from_hdx(self, configuration, post_delete):
        galleryitem = GalleryItem.read_from_hdx('TEST1')
        galleryitem.delete_from_hdx()
        del galleryitem['id']
        with pytest.raises(HDXError):
            galleryitem.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        galleryitem_data = copy.deepcopy(TestGalleryItem.galleryitem_data)
        galleryitem = GalleryItem(galleryitem_data)
        assert galleryitem['title'] == 'MyGalleryItem1'
        assert galleryitem['type'] == 'visualization'
        galleryitem.update_from_yaml(static_yaml)
        assert galleryitem['title'] == 'MyGalleryItem1'
        assert galleryitem['type'] == 'paper'

    def test_update_json(self, configuration, static_json):
        galleryitem_data = copy.deepcopy(TestGalleryItem.galleryitem_data)
        galleryitem = GalleryItem(galleryitem_data)
        assert galleryitem['title'] == 'MyGalleryItem1'
        assert galleryitem['type'] == 'visualization'
        galleryitem.update_from_json(static_json)
        assert galleryitem['title'] == 'MyGalleryItem1'
        assert galleryitem['type'] == 'other'
