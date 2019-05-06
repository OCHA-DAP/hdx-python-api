# -*- coding: UTF-8 -*-
"""Organization Tests"""
import copy
import json
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml

from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from . import MockResponse, organization_data, user_data
from .test_user import user_mockshow

resultdict = load_yaml(join('tests', 'fixtures', 'organization_show_results.yml'))

organization_list = ['acaps', 'accion-contra-el-hambre-acf-spain', 'acf-west-africa', 'acled', 'acted',
                     'action-contre-la-faim', 'adeso', 'afd', 'afdb', 'afghanistan-protection-cluster']
searchdict = load_yaml(join('tests', 'fixtures', 'dataset_search_results.yml'))


def organization_mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19' or datadict['id'] == 'TEST1':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_show"}' % result)
    if datadict['id'] == 'TEST2':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_show"}')
    if datadict['id'] == 'TEST3':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_show"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_show"}')


def mocklist(url):
    if 'list' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_list"}')
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_list"}' % json.dumps(
                            organization_list))

def mockgetdatasets(url, datadict):
    if 'search' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not search", "__type": "TEST ERROR: Not Search Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}')
    if datadict['fq'] == 'organization:acled':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_search"}' % json.dumps(
                                searchdict))

class TestOrganization:
    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_organization_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_organization_static.json')

    @pytest.fixture(scope='function')
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return organization_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_create(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return organization_mockshow(url, datadict)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_create"}')

                resultdictcopy = copy.deepcopy(resultdict)
                resultdictcopy['state'] = datadict['state']
                result = json.dumps(resultdictcopy)
                if datadict['name'] == 'MyOrganization1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_create"}' % result)
                if datadict['name'] == 'MyOrganization2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_create"}')
                if datadict['name'] == 'MyOrganization3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_create"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_create"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_update(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return organization_mockshow(url, datadict)
                if 'update' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_update"}')
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict['name'] == 'MyOrganization1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_update"}' % result)
                if datadict['name'] == 'MyOrganization2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_update"}')
                if datadict['name'] == 'MyOrganization3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_update"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_delete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return organization_mockshow(url, datadict)
                if 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_delete"}')
                if datadict['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_delete"}')

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_list(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mocklist(url)

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
    def datasets_get(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return mockgetdatasets(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read, mocksmtp):
        organization = Organization.read_from_hdx('b67e6c74-c185-4f43-b561-0e114a736f19')
        assert organization['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'
        assert organization['name'] == 'acled'
        organization = Organization.read_from_hdx('TEST2')
        assert organization is None
        organization = Organization.read_from_hdx('TEST3')
        assert organization is None

    def test_create_in_hdx(self, configuration, post_create):
        organization = Organization()
        with pytest.raises(HDXError):
            organization.create_in_hdx()
        organization['id'] = 'b67e6c74-c185-4f43-b561-0e114a736f19'
        organization['name'] = 'LALA'
        with pytest.raises(HDXError):
            organization.create_in_hdx()

        org_data = copy.deepcopy(organization_data)
        organization = Organization(org_data)
        organization.create_in_hdx()
        assert organization['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'
        assert organization['state'] == 'active'

        org_data['name'] = 'MyOrganization2'
        organization = Organization(org_data)
        with pytest.raises(HDXError):
            organization.create_in_hdx()

        org_data['name'] = 'MyOrganization3'
        organization = Organization(org_data)
        with pytest.raises(HDXError):
            organization.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        organization = Organization()
        organization['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            organization.update_in_hdx()
        organization['name'] = 'LALA'
        with pytest.raises(HDXError):
            organization.update_in_hdx()

        organization = Organization.read_from_hdx('b67e6c74-c185-4f43-b561-0e114a736f19')
        assert organization['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'
        assert organization['name'] == 'acled'

        organization['description'] = 'Humanitarian work'
        organization['id'] = 'b67e6c74-c185-4f43-b561-0e114a736f19'
        organization['name'] = 'MyOrganization1'
        organization.update_in_hdx()
        assert organization['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'
        assert organization['description'] == 'Humanitarian work'
        assert organization['state'] == 'active'

        organization['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            organization.update_in_hdx()

        del organization['id']
        with pytest.raises(HDXError):
            organization.update_in_hdx()

        org_data = copy.deepcopy(organization_data)
        org_data['name'] = 'MyOrganization1'
        org_data['id'] = 'b67e6c74-c185-4f43-b561-0e114a736f19'
        organization = Organization(org_data)
        organization.create_in_hdx()
        assert organization['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'
        assert organization['name'] == 'MyOrganization1'
        assert organization['state'] == 'active'

    def test_delete_from_hdx(self, configuration, post_delete):
        organization = Organization.read_from_hdx('b67e6c74-c185-4f43-b561-0e114a736f19')
        organization.delete_from_hdx()
        del organization['id']
        with pytest.raises(HDXError):
            organization.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        org_data = copy.deepcopy(organization_data)
        organization = Organization(org_data)
        assert organization['name'] == 'MyOrganization1'
        assert organization['description'] == 'We do humanitarian work'
        organization.update_from_yaml(static_yaml)
        assert organization['name'] == 'MyOrg1'
        assert organization['description'] == 'Humanitarian organisation'

    def test_update_json(self, configuration, static_json):
        org_data = copy.deepcopy(organization_data)
        organization = Organization(org_data)
        assert organization['name'] == 'MyOrganization1'
        assert organization['description'] == 'We do humanitarian work'
        organization.update_from_json(static_json)
        assert organization['name'] == 'MyOrg1'
        assert organization['description'] == 'other'

    def test_get_all_organizations(self, configuration, post_list):
        organizations = Organization.get_all_organization_names()
        assert len(organizations) == 10

    def test_users(self, configuration, user_read):
        org_data = copy.deepcopy(resultdict)
        organization = Organization(org_data)
        organization['users'][0]['name'] = 'MyUser1'
        users = organization.get_users()
        assert len(users) == 1
        assert users[0]['name'] == 'MyUser1'
        organization.remove_user('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        users = organization.get_users()
        assert len(users) == 0
        userdata_copy = copy.deepcopy(user_data)
        userdata_copy['id'] = '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        user = User(userdata_copy)
        user['name'] = 'TEST1'
        user['capacity'] = 'member'
        del organization['users']
        organization.add_update_users([user])
        users = organization.get_users('member')
        assert len(users) == 1
        organization.remove_user(users[0])
        users = organization.get_users()
        assert len(users) == 0
        organization.add_update_users([user])
        users = organization.get_users('member')
        assert len(users) == 1
        organization.add_update_users(['MyUser1'], 'member')
        users = organization.get_users('member')
        assert len(users) == 2
        assert users[0]['name'] == 'MyUser1'
        organization.remove_user(users[0].data)
        users = organization.get_users('member')
        assert len(users) == 1
        organization.add_update_users([user], 'editor')
        organization.remove_user('')
        users = organization.get_users('editor')
        assert len(users) == 1
        organization.remove_user('NOT_EXIST')
        users = organization.get_users('editor')
        assert len(users) == 1
        users = organization.get_users('admin')
        assert len(users) == 0
        del organization['users'][0]['id']
        users = organization.get_users('member')
        assert len(users) == 1
        organization.add_update_user({'name': 'TEST1'}, 'member')
        users = organization.get_users('member')
        assert len(users) == 2
        with pytest.raises(HDXError):
            organization.add_update_users(123)
        with pytest.raises(HDXError):
            organization.add_update_user(123)
        with pytest.raises(HDXError):
            organization.remove_user(123)

    def test_get_datasets(self, configuration, datasets_get):
        org_data = copy.deepcopy(resultdict)
        organization = Organization(org_data)
        datasets = organization.get_datasets()
        assert len(datasets) == 10
