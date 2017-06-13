# -*- coding: UTF-8 -*-
"""User Tests"""
import copy
import json
from os.path import join

import pytest
import requests

from hdx.configuration import Configuration
from hdx.data.hdxobject import HDXError
from hdx.data.user import User
from hdx.utilities.dictandlist import merge_two_dictionaries


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


resultdict = {
    'openid': None,
    'about': 'Data Scientist',
    'apikey': '31e86726-2993-4d82-be93-3d2133c81d94',
    'display_name': 'xxx',
    'name': 'MyUser1',
    'created': '2017-03-22T09:55:55.871573',
    'email_hash': '9e2fc15d8cc65f43136f9dcef05fa184',
    'email': 'xxx@yyy.com',
    'sysadmin': False,
    'activity_streams_email_notifications': False,
    'state': 'active',
    'number_of_edits': 0,
    'fullname': 'xxx xxx',
    'id': '9f3e9973-7dbe-4c65-8820-f48578e3ffea',
    'number_created_packages': 0
}

result2dict = copy.deepcopy(resultdict)
result2dict['email'] = 'aaa@bbb.com'
user_list = [resultdict, result2dict]


def mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == 'TEST1':
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}' % result)
    if datadict['id'] == 'TEST2':
        return MockResponse(404,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}')
    if datadict['id'] == 'TEST3':
        return MockResponse(200,
                            '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}')
    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}')


def mocklist(url):
    if 'list' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not all", "__type": "TEST ERROR: Not All Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_list"}')
    return MockResponse(200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_list"}' % json.dumps(
                            user_list))


class TestUser:
    user_data = {
        'name': 'MyUser1',
        'email': 'xxx@yyy.com',
        'password': 'xxx',
        'fullname': 'xxx xxx',
        'about': 'Data Scientist',
    }

    smtp_initargs = {
        'host': 'localhost',
        'port': 123,
        'local_hostname': 'mycomputer.fqdn.com',
        'timeout': 3,
        'source_address': ('machine', 456),
    }
    username = 'user'
    password = 'pass'
    email_config_dict = {
        'connection_type': 'ssl',
        'username': username,
        'password': password
    }
    subject = 'hello'
    body = 'hello there'
    sender = 'me@gmail.com'
    mail_options = ['a', 'b']
    rcpt_options = [1, 2]
    email_config_dict.update(smtp_initargs)

    @pytest.fixture(scope='class')
    def static_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_user_static.yml')

    @pytest.fixture(scope='class')
    def static_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_user_static.json')

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
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}')

                result = json.dumps(resultdict)
                if datadict['name'] == 'MyUser1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}' % result)
                if datadict['name'] == 'MyUser2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}')
                if datadict['name'] == 'MyUser3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}')

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
                                        '{"success": false, "error": {"message": "TEST ERROR: Not update", "__type": "TEST ERROR: Not Update Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_update"}')
                resultdictcopy = copy.deepcopy(resultdict)
                merge_two_dictionaries(resultdictcopy, datadict)

                result = json.dumps(resultdictcopy)
                if datadict['name'] == 'MyUser1':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_update"}' % result)
                if datadict['name'] == 'MyUser2':
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_update"}')
                if datadict['name'] == 'MyUser3':
                    return MockResponse(200,
                                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_update"}')

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_update"}')

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
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}')
                if datadict['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}')

        monkeypatch.setattr(requests, 'Session', MockSession)

    @pytest.fixture(scope='function')
    def post_list(self, monkeypatch):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth):
                datadict = json.loads(data.decode('utf-8'))
                return mocklist(url)

        monkeypatch.setattr(requests, 'Session', MockSession)

    def test_read_from_hdx(self, configuration, read, mocksmtp):
        user = User.read_from_hdx('TEST1')
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['name'] == 'MyUser1'
        user = User.read_from_hdx('TEST2')
        assert user is None
        user = User.read_from_hdx('TEST3')
        assert user is None
        config = Configuration.read()
        config.setup_emailer(email_config_dict=TestUser.email_config_dict)
        user = User.read_from_hdx('TEST1')
        user.email(TestUser.subject, TestUser.body, sender=TestUser.sender, mail_options=TestUser.mail_options,
                   rcpt_options=TestUser.rcpt_options)
        email = config.emailer()
        assert email.server.type == 'smtpssl'
        assert email.server.initargs == TestUser.smtp_initargs
        assert email.server.username == TestUser.username
        assert email.server.password == TestUser.password
        assert email.server.sender == TestUser.sender
        assert email.server.recipients == ['xxx@yyy.com']
        assert email.server.msg == '''Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Subject: hello
From: me@gmail.com
To: xxx@yyy.com

hello there'''
        assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}

    def test_create_in_hdx(self, configuration, post_create):
        user = User()
        with pytest.raises(HDXError):
            user.create_in_hdx()
        user['id'] = 'TEST1'
        user['name'] = 'LALA'
        with pytest.raises(HDXError):
            user.create_in_hdx()

        user_data = copy.deepcopy(TestUser.user_data)
        user = User(user_data)
        user.create_in_hdx()
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'

        user_data['name'] = 'MyUser2'
        user = User(user_data)
        with pytest.raises(HDXError):
            user.create_in_hdx()

        user_data['name'] = 'MyUser3'
        user = User(user_data)
        with pytest.raises(HDXError):
            user.create_in_hdx()

    def test_update_in_hdx(self, configuration, post_update):
        user = User()
        user['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            user.update_in_hdx()
        user['name'] = 'LALA'
        with pytest.raises(HDXError):
            user.update_in_hdx()

        user = User.read_from_hdx('TEST1')
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['about'] == 'Data Scientist'

        user['about'] = 'IMO'
        user['id'] = 'TEST1'
        user['name'] = 'MyUser1'
        user.update_in_hdx()
        assert user['id'] == 'TEST1'
        assert user['about'] == 'IMO'

        user['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            user.update_in_hdx()

        del user['id']
        with pytest.raises(HDXError):
            user.update_in_hdx()

        user_data = copy.deepcopy(TestUser.user_data)
        user_data['name'] = 'MyUser1'
        user_data['id'] = 'TEST1'
        user = User(user_data)
        user.create_in_hdx()
        assert user['id'] == 'TEST1'
        assert user['about'] == 'Data Scientist'

    def test_delete_from_hdx(self, configuration, post_delete):
        user = User.read_from_hdx('TEST1')
        user.delete_from_hdx()
        del user['id']
        with pytest.raises(HDXError):
            user.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        user_data = copy.deepcopy(TestUser.user_data)
        user = User(user_data)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'Data Scientist'
        user.update_from_yaml(static_yaml)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'IMO'

    def test_update_json(self, configuration, static_json):
        user_data = copy.deepcopy(TestUser.user_data)
        user = User(user_data)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'Data Scientist'
        user.update_from_json(static_json)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'other'

    def test_get_all_users(self, configuration, post_list, mocksmtp):
        users = User.get_all_users()
        assert len(users) == 2
        config = Configuration.read()
        config.setup_emailer(email_config_dict=TestUser.email_config_dict)
        User.email_users(users, TestUser.subject, TestUser.body, sender=TestUser.sender, mail_options=TestUser.mail_options,
                   rcpt_options=TestUser.rcpt_options)
        email = config.emailer()
        assert email.server.type == 'smtpssl'
        assert email.server.initargs == TestUser.smtp_initargs
        assert email.server.username == TestUser.username
        assert email.server.password == TestUser.password
        assert email.server.sender == TestUser.sender
        assert email.server.recipients == ['xxx@yyy.com', 'aaa@bbb.com']
        assert email.server.msg == '''Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Subject: hello
From: me@gmail.com
To: xxx@yyy.com, aaa@bbb.com

hello there'''
        assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}
        with pytest.raises(ValueError):
            User.email_users(list(), TestUser.subject, TestUser.body, sender=TestUser.sender,
                             mail_options=TestUser.mail_options, rcpt_options=TestUser.rcpt_options)
