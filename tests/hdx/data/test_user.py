# -*- coding: UTF-8 -*-
"""User Tests"""
import copy
import json
from os.path import join

import pytest
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml

from hdx.data.hdxobject import HDXError
from hdx.data.user import User
from hdx.hdx_configuration import Configuration
from . import MockResponse, user_data

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
orgdict = load_yaml(join('tests', 'fixtures', 'organization_show_results.yml'))
orgdict2 = copy.deepcopy(orgdict)
del orgdict2['users']
del orgdict2['packages']
del orgdict2['extras']
orglist = [orgdict2]

result2dict = copy.deepcopy(resultdict)
result2dict['email'] = 'aaa@bbb.com'
user_list = [resultdict, result2dict]

user_autocomplete = [{'fullname': 'Fake Test', 'id': '2ea425bf-130d-4b63-b755-5cc5435b4b75', 'name': 'another-fake-tester'}]


def user_mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}')
    result = json.dumps(resultdict)
    if datadict['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea' or datadict['id'] == 'MyUser1':
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
    text_body = 'hello there'
    html_body = '''\
<html>
  <head></head>
  <body>
    <p>Hi!<br>
    </p>
  </body>
</html>
'''
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
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return user_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_create(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return user_mockshow(url, datadict)
                if 'create' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_create"}')

                resultdictcopy = copy.deepcopy(resultdict)
                resultdictcopy['state'] = datadict['state']
                result = json.dumps(resultdictcopy)
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

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_update(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                if 'show' in url:
                    return user_mockshow(url, datadict)
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

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_delete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'show' in url:
                    return user_mockshow(url, datadict)
                if 'delete' not in url:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not delete", "__type": "TEST ERROR: Not Delete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}')
                if datadict['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea':
                    return MockResponse(200,
                                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}' % decodedata)

                return MockResponse(404,
                                    '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_delete"}')

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
    def post_listorgs(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'user' in url:
                    if 'show' in url:
                        return user_mockshow(url, datadict)
                    elif 'list' in url:
                        return MockResponse(200,
                                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=organization_list"}' % json.dumps(orglist))
                elif 'organization' in url:
                    if 'show' in url:
                        result = json.dumps(orgdict)
                        if datadict['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19' or datadict['id'] == 'TEST1':
                            return MockResponse(200,
                                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_show"}' % result)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_autocomplete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                decodedata = data.decode('utf-8')
                datadict = json.loads(decodedata)
                if 'autocomplete' not in url or 'fake' not in datadict['q']:
                    return MockResponse(404,
                                        '{"success": false, "error": {"message": "TEST ERROR: Not autocomplete", "__type": "TEST ERROR: Not Autocomplete Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_autocomplete"}')
                result = json.dumps(user_autocomplete)
                return MockResponse(200,
                                    '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=user_autocomplete"}' % result)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, read, mocksmtp):
        user = User.read_from_hdx('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['name'] == 'MyUser1'
        user = User.read_from_hdx('TEST2')
        assert user is None
        user = User.read_from_hdx('TEST3')
        assert user is None
        config = Configuration.read()
        config.setup_emailer(email_config_dict=TestUser.email_config_dict)
        user = User.read_from_hdx('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        user.email(TestUser.subject, TestUser.text_body, html_body=TestUser.html_body, sender=TestUser.sender,
                   mail_options=TestUser.mail_options, rcpt_options=TestUser.rcpt_options)
        email = config.emailer()
        assert email.server.type == 'smtpssl'
        assert email.server.initargs == TestUser.smtp_initargs
        assert email.server.username == TestUser.username
        assert email.server.password == TestUser.password
        assert email.server.sender == TestUser.sender
        assert email.server.recipients == ['xxx@yyy.com']
        assert 'Content-Type: multipart/alternative;' in email.server.msg
        assert '''\
MIME-Version: 1.0
Subject: hello
From: me@gmail.com
To: xxx@yyy.com''' in email.server.msg
        assert '''\
Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

hello there''' in email.server.msg
        assert '''\
Content-Type: text/html; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

<html>
  <head></head>
  <body>
    <p>Hi!<br>
    </p>
  </body>
</html>''' in email.server.msg
        assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}

    def test_create_in_hdx(self, configuration, post_create):
        user = User()
        with pytest.raises(HDXError):
            user.create_in_hdx()
        user['id'] = '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        user['name'] = 'LALA'
        with pytest.raises(HDXError):
            user.create_in_hdx()

        data = copy.deepcopy(user_data)
        user = User(data)
        user.create_in_hdx()
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['capacity'] == 'admin'
        assert user['state'] == 'active'

        data['name'] = 'MyUser2'
        user = User(data)
        with pytest.raises(HDXError):
            user.create_in_hdx()

        data['name'] = 'MyUser3'
        user = User(data)
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

        user = User.read_from_hdx('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['about'] == 'Data Scientist'

        user['about'] = 'IMO'
        user['id'] = '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        user['name'] = 'MyUser1'
        user['capacity'] = 'member'
        user.update_in_hdx()
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['about'] == 'IMO'
        assert user['capacity'] == 'member'
        assert user['state'] == 'active'

        user['id'] = 'NOTEXIST'
        with pytest.raises(HDXError):
            user.update_in_hdx()

        del user['id']
        with pytest.raises(HDXError):
            user.update_in_hdx()

        data = copy.deepcopy(user_data)
        data['name'] = 'MyUser1'
        data['id'] = '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        user = User(data)
        user.create_in_hdx()
        assert user['id'] == '9f3e9973-7dbe-4c65-8820-f48578e3ffea'
        assert user['about'] == 'Data Scientist'
        assert user['state'] == 'active'

    def test_delete_from_hdx(self, configuration, post_delete):
        user = User.read_from_hdx('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        user.delete_from_hdx()
        del user['id']
        with pytest.raises(HDXError):
            user.delete_from_hdx()

    def test_update_yaml(self, configuration, static_yaml):
        data = copy.deepcopy(user_data)
        user = User(data)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'Data Scientist'
        user.update_from_yaml(static_yaml)
        assert user['name'] == 'MyUser1'
        assert user['about'] == 'IMO'

    def test_update_json(self, configuration, static_json):
        data = copy.deepcopy(user_data)
        user = User(data)
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
        User.email_users(users, TestUser.subject, TestUser.text_body, html_body=TestUser.html_body,
                         sender=TestUser.sender, cc=users, bcc=users, mail_options=TestUser.mail_options,
                         rcpt_options=TestUser.rcpt_options)
        email = config.emailer()
        assert email.server.type == 'smtpssl'
        assert email.server.initargs == TestUser.smtp_initargs
        assert email.server.username == TestUser.username
        assert email.server.password == TestUser.password
        assert email.server.sender == TestUser.sender
        assert email.server.recipients == ['xxx@yyy.com', 'aaa@bbb.com', 'xxx@yyy.com', 'aaa@bbb.com', 'xxx@yyy.com',
                                           'aaa@bbb.com']
        assert 'Content-Type: multipart/alternative;' in email.server.msg
        assert '''\
MIME-Version: 1.0
Subject: hello
From: me@gmail.com
To: xxx@yyy.com, aaa@bbb.com
Cc: xxx@yyy.com, aaa@bbb.com''' in email.server.msg
        assert '''\
Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

hello there''' in email.server.msg
        assert '''\
Content-Type: text/html; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit

<html>
  <head></head>
  <body>
    <p>Hi!<br>
    </p>
  </body>
</html>''' in email.server.msg
        assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}
        with pytest.raises(ValueError):
            User.email_users(list(), TestUser.subject, TestUser.text_body, sender=TestUser.sender,
                             mail_options=TestUser.mail_options, rcpt_options=TestUser.rcpt_options)

    def test_get_organizations(self, configuration, post_listorgs):
        user = User.read_from_hdx('9f3e9973-7dbe-4c65-8820-f48578e3ffea')
        organizations = user.get_organizations()
        assert organizations[0]['id'] == 'b67e6c74-c185-4f43-b561-0e114a736f19'

    def test_autocomplete(self, configuration, post_autocomplete):
        assert User.autocomplete('fake%20test') == user_autocomplete
