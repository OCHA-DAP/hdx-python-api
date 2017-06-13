# -*- coding: UTF-8 -*-
"""Configuration Tests"""
from os.path import join

import ckanapi
import pytest

from hdx.configuration import Configuration, ConfigurationError


class TestConfiguration:
    @pytest.fixture(scope='class')
    def hdx_key_file(self):
        return join('tests', 'fixtures', '.hdxkey')

    @pytest.fixture(scope='class')
    def empty_hdx_key_file(self):
        return join('tests', 'fixtures', '.emptyhdxkey')

    @pytest.fixture(scope='class')
    def project_config_yaml(self):
        return join('tests', 'fixtures', 'config', 'project_configuration.yml')

    @pytest.fixture(scope='class')
    def project_config_json(self):
        return join('tests', 'fixtures', 'config', 'project_configuration.json')

    def test_init(self, hdx_key_file, project_config_json, project_config_yaml):
        default_key_file = Configuration.default_hdx_key_file
        Configuration.default_hdx_key_file = 'NOT EXIST'
        with pytest.raises(IOError):
            Configuration()
        Configuration.default_hdx_key_file = default_key_file

        with pytest.raises(IOError):
            Configuration(hdx_key_file='NOT_EXIST', project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_key_file=hdx_key_file, hdx_config_yaml='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_key_file=hdx_key_file, hdx_config_json='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_key_file=hdx_key_file, project_config_yaml='NOT_EXIST')

        with pytest.raises(IOError):
            Configuration(hdx_key_file=hdx_key_file, project_config_json='NOT_EXIST')

        with pytest.raises(ConfigurationError):
            Configuration(hdx_site='NOT_EXIST', hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_key_file=hdx_key_file, project_config_json=project_config_json,
                          project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_key_file=hdx_key_file, project_config_dict={'la': 'la'},
                          project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_key_file=hdx_key_file, project_config_dict={'la': 'la'},
                          project_config_json=project_config_json)

    def test_hdx_configuration_dict(self, hdx_key_file, project_config_yaml, mocksmtp):
        Configuration._create(hdx_site='prod', hdx_key_file=hdx_key_file,
                             hdx_config_dict={
                                                 'hdx_prod_site': {
                                                     'url': 'https://data.humdata.org/',
                                                     'username': None,
                                                     'password': None
                                                 },
                                                 'XYZ': {'567': 987}
                                             },
                             project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'param_1': 'ABC',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'XYZ': {'567': 987}
        }

        configuration = Configuration.read()
        assert configuration == expected_configuration

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
        email_config_dict.update(smtp_initargs)

        recipients = ['larry@gmail.com', 'moe@gmail.com', 'curly@gmail.com']
        subject = 'hello'
        body = 'hello there'
        sender = 'me@gmail.com'
        mail_options = ['a', 'b']
        rcpt_options = [1, 2]

        configuration.setup_emailer(email_config_dict=email_config_dict)
        email = configuration.emailer()
        email.send(recipients, subject, body, sender=sender, mail_options=mail_options,
                                     rcpt_options=rcpt_options)
        assert email.server.type == 'smtpssl'
        assert email.server.initargs == smtp_initargs
        assert email.server.username == username
        assert email.server.password == password
        assert email.server.sender == sender
        assert email.server.recipients == recipients
        assert email.server.msg == '''Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Subject: hello
From: me@gmail.com
To: larry@gmail.com, moe@gmail.com, curly@gmail.com

hello there'''
        assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}

    def test_hdx_configuration_json(self, hdx_key_file, project_config_yaml, locations):
        hdx_config_json = join('tests', 'fixtures', 'config', 'hdx_config.json')
        Configuration._create(validlocations=locations, hdx_key_file=hdx_key_file, hdx_config_json=hdx_config_json,
                             project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'param_1': 'ABC',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'hdx_test_site': {
                'url': 'https://test-data.humdata.org/',
                'username': 'tumteetum',
                'password': 'tumteetumteetum'
            },
            'dataset': {'required_fields': [
                'name',
                'dataset_date',
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['name', 'description'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
            ],},
        }
        assert Configuration.read() == expected_configuration

    def test_hdx_configuration_yaml(self, hdx_key_file, project_config_yaml, locations):
        hdx_configuration_yaml = join('tests', 'fixtures', 'config', 'hdx_config.yml')
        Configuration._create(validlocations=locations, hdx_key_file=hdx_key_file,
                             hdx_config_yaml=hdx_configuration_yaml, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'param_1': 'ABC',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'hdx_test_site': {
                'url': 'https://test-data.humdata.org/',
                'username': 'lala',
                'password': 'lalala'
            },
            'dataset': {'required_fields': [
                'name',
                'title',
                'dataset_date',
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'description'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
            ], 'ignore_on_update': ['dataset_id']},
        }
        assert Configuration.read() == expected_configuration

    def test_project_configuration_dict(self, hdx_key_file):
        Configuration._create(hdx_key_file=hdx_key_file)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'hdx_demo_site': {
                'url': 'https://demo-data.humdata.org/',
                'username': 'ZGVtbzE3OQ==',
                'password': 'ZnVud2l0aGhkeA=='
            },
            'hdx_test_site': {
                'url': 'https://test-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'hdx_feature_site': {
                'url': 'https://feature-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'dataset': {'required_fields': [
                'name',
                'private',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'dataset_date',
                'groups',
                'license_id',
                'methodology',
                'data_update_frequency'
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description',
                                             'url_type', 'resource_type'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': 'dataset_id'},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
        }
        assert Configuration.read() == expected_configuration
        Configuration._create(hdx_key_file=hdx_key_file, project_config_dict={'abc': '123'})
        expected_configuration['abc'] = '123'
        assert Configuration.read() == expected_configuration

    def test_project_configuration_json(self, hdx_key_file, project_config_json):
        Configuration._create(hdx_key_file=hdx_key_file, project_config_json=project_config_json)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'hdx_demo_site': {
                'url': 'https://demo-data.humdata.org/',
                'username': 'ZGVtbzE3OQ==',
                'password': 'ZnVud2l0aGhkeA=='
            },
            'hdx_test_site': {
                'url': 'https://test-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'hdx_feature_site': {
                'url': 'https://feature-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'my_param': 'abc',
            'dataset': {'required_fields': [
                'name',
                'private',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'dataset_date',
                'groups',
                'license_id',
                'methodology',
                'data_update_frequency'
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description',
                                             'url_type', 'resource_type'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': 'dataset_id'},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
        }
        assert Configuration.read() == expected_configuration

    def test_project_configuration_yaml(self, hdx_key_file, project_config_yaml):
        Configuration._create(hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'param_1': 'ABC',
            'hdx_prod_site': {
                'url': 'https://data.humdata.org/',
                'username': None,
                'password': None
            },
            'hdx_demo_site': {
                'url': 'https://demo-data.humdata.org/',
                'username': 'ZGVtbzE3OQ==',
                'password': 'ZnVud2l0aGhkeA=='
            },
            'hdx_test_site': {
                'url': 'https://test-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'hdx_feature_site': {
                'url': 'https://feature-data.humdata.org/',
                'username': 'ZGF0YXByb2plY3Q=',
                'password': 'aHVtZGF0YQ=='
            },
            'dataset': {'required_fields': [
                'name',
                'private',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'dataset_date',
                'groups',
                'license_id',
                'methodology',
                'data_update_frequency'
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description',
                                             'url_type', 'resource_type'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': 'dataset_id'},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
        }
        assert Configuration.read() == expected_configuration

    def test_get_hdx_key_site(self, hdx_key_file, project_config_yaml):
        Configuration._create(hdx_site='prod', hdx_key_file=hdx_key_file,
                             hdx_config_dict={},
                             project_config_yaml=project_config_yaml)
        actual_configuration = Configuration.read()
        assert actual_configuration.get_api_key() == '12345'
        assert actual_configuration.get_hdx_site_url() == 'https://data.humdata.org/'
        assert actual_configuration._get_credentials() == ('', '')

    def test_set_hdx_key_value(self, empty_hdx_key_file, project_config_yaml):
        with pytest.raises(ConfigurationError):
            Configuration.load_api_key(empty_hdx_key_file)
        Configuration._create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                             hdx_config_dict={},
                             project_config_yaml=project_config_yaml)
        assert Configuration.read().get_api_key() == 'TEST_HDX_KEY'
        Configuration._create(hdx_site='prod', hdx_read_only=True,
                              hdx_config_dict={},
                              project_config_yaml=project_config_yaml)
        assert Configuration.read().get_api_key() is None

    def test_create_set_configuration(self, project_config_yaml):
        Configuration._create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                             hdx_config_dict={},
                             project_config_yaml=project_config_yaml)
        with pytest.raises(ConfigurationError):
            Configuration.create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                 hdx_config_dict={},
                                 project_config_yaml=project_config_yaml)
        configuration = Configuration(hdx_site='test', hdx_key='OTHER_TEST_HDX_KEY',
                                      hdx_config_dict={},
                                      project_config_yaml=project_config_yaml)
        Configuration.setup(configuration)
        assert Configuration.read() == configuration
        Configuration._configuration = None
        with pytest.raises(ConfigurationError):
            Configuration.read()
        Configuration.create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                             hdx_config_dict={},
                             project_config_yaml=project_config_yaml)
        assert Configuration.read().get_api_key() == 'TEST_HDX_KEY'

    def test_remoteckan_validlocations(self, project_config_yaml):
        Configuration._create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                             hdx_config_dict={},
                             project_config_yaml=project_config_yaml)
        remoteckan = ckanapi.RemoteCKAN('http://lalala', apikey='12345',
                                        user_agent='HDXPythonLibrary/1.0')
        Configuration.read().setup_remoteckan(remoteckan)
        assert Configuration.read().remoteckan() == remoteckan
        validlocations = [{'a': '1', 'b': '2'}]
        Configuration.read()._validlocations = validlocations
        assert Configuration.read().validlocations() == validlocations
        validlocations = [{'z': '25', 'x': '32'}]
        validlocationsfn = lambda: validlocations
        Configuration.read().setup_validlocations(validlocationsfn)
        assert Configuration.read().validlocations() == validlocations
        remoteckan = ckanapi.RemoteCKAN('http://hahaha', apikey='54321',
                                        user_agent='HDXPythonLibrary/0.5')
        validlocations = [{'1': 'a', '3': 'd'}]
        validlocationsfn = lambda: validlocations
        Configuration._create(remoteckan=remoteckan,
                              validlocationsfn=validlocationsfn,
                              hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_config_dict={},
                              project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan() == remoteckan
        assert Configuration.read().validlocations() == validlocations
        Configuration.read()._remoteckan = None
        with pytest.raises(ConfigurationError):
            Configuration.read().remoteckan()
        Configuration.read()._validlocationsfn = None
        with pytest.raises(ConfigurationError):
            Configuration.read().validlocations()
        Configuration._configuration = None
        with pytest.raises(ConfigurationError):
            Configuration.read().remoteckan()
        with pytest.raises(ConfigurationError):
            Configuration.read().validlocations()
