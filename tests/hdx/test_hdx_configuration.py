# -*'coding: UTF-8 -*-
"""Configuration Tests"""
from os.path import join

import ckanapi
import pytest
from hdx.utilities.loader import LoadError

from hdx.hdx_configuration import Configuration, ConfigurationError


class TestConfiguration:
    @pytest.fixture(scope='class')
    def hdx_base_config_yaml(self, configfolder):
        return join(configfolder, 'hdx_base_config.yml')

    @pytest.fixture(scope='class')
    def hdx_base_config_json(self, configfolder):
        return join(configfolder, 'hdx_base_config.json')

    @pytest.fixture(scope='class')
    def hdx_config_json(self, configfolder):
        return join(configfolder, 'hdx_config.json')

    @pytest.fixture(scope='class')
    def hdx_missing_site_config_json(self, configfolder):
        return join(configfolder, 'hdx_missing_site_config.json')

    @pytest.fixture(scope='class')
    def project_config_json(self, configfolder):
        return join(configfolder, 'project_configuration.json')

    @pytest.fixture(scope='class')
    def user_agent_config_yaml(self, configfolder):
        return join(configfolder, 'user_agent_config.yml')

    @pytest.fixture(scope='class')
    def user_agent_config2_yaml(self, configfolder):
        return join(configfolder, 'user_agent_config2.yml')

    @pytest.fixture(scope='class')
    def user_agent_config3_yaml(self, configfolder):
        return join(configfolder, 'user_agent_config3.yml')

    @pytest.fixture(scope='class')
    def empty_yaml(self, configfolder):
        return join(configfolder, 'empty.yml')

    @pytest.fixture(scope='class')
    def user_agent_config_wrong_yaml(self, configfolder):
        return join(configfolder, 'user_agent_config_wrong.yml')

    def test_init(self, hdx_config_json, hdx_config_yaml, hdx_base_config_yaml, hdx_base_config_json, hdx_missing_site_config_json, project_config_json, project_config_yaml):
        default_config_file = Configuration.default_hdx_config_yaml
        Configuration.default_hdx_config_yaml = 'NOT EXIST'
        with pytest.raises(ConfigurationError):
            Configuration()
        Configuration.default_hdx_config_yaml = default_config_file

        Configuration.default_hdx_config_yaml = hdx_config_yaml
        assert Configuration().get_api_key() == '12345'
        Configuration.default_hdx_config_yaml = default_config_file

        with pytest.raises(IOError):
            Configuration(hdx_config_yaml='NOT_EXIST', project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_config_yaml=hdx_config_yaml, hdx_base_config_yaml='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_config_yaml=hdx_config_yaml, hdx_base_config_json='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_dict={'a': 1}, hdx_config_yaml=hdx_base_config_yaml,
                          hdx_base_config_json=hdx_base_config_json, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_dict={'a': 1}, hdx_config_json=hdx_config_json,
                          hdx_base_config_json=hdx_base_config_json, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_json=hdx_config_json, hdx_config_yaml=hdx_config_yaml,
                          hdx_base_config_json = hdx_base_config_json, project_config_yaml = project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, hdx_base_config_dict={'a': 1},
                          hdx_base_config_yaml=hdx_base_config_yaml, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, hdx_base_config_dict={'a': 1},
                          hdx_base_config_json=hdx_base_config_json, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, hdx_base_config_json=hdx_base_config_json,
                          hdx_base_config_yaml=hdx_base_config_yaml, project_config_yaml=project_config_yaml)

        with pytest.raises(IOError):
            Configuration(hdx_config_yaml=hdx_config_yaml, project_config_yaml='NOT_EXIST')

        with pytest.raises(IOError):
            Configuration(hdx_config_yaml=hdx_config_yaml, project_config_json='NOT_EXIST')

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_json=hdx_missing_site_config_json, project_config_json=project_config_json, hdx_base_config_json=hdx_base_config_json)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_site='NOT_EXIST', hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, project_config_json=project_config_json,
                          project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, project_config_dict={'la': 'la'},
                          project_config_yaml=project_config_yaml)

        with pytest.raises(ConfigurationError):
            Configuration(hdx_config_yaml=hdx_config_yaml, project_config_dict={'la': 'la'},
                          project_config_json=project_config_json)

    def test_hdx_configuration_dict(self, project_config_yaml, mocksmtp):
        Configuration._create(user_agent='test', hdx_config_dict={'hdx_site': 'prod', 'hdx_read_only': True, 'hdx_key': 'abcde'},
                              hdx_base_config_dict={
                                                 'hdx_prod_site': {
                                                     'url': 'https://data.humdata.org/',
                                                     'username': None,
                                                     'password': None
                                                 },
                                                 'XYZ': {'567': 987}
                                             },
                              project_config_yaml=project_config_yaml)
        expected_configuration = {
            'hdx_site': 'prod',
            'hdx_read_only': True,
            'hdx_key': 'abcde',
            'tags_cleanup_url': 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/HDX_Tags_Cleaning.csv',
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

        with pytest.raises(ConfigurationError):
            configuration.emailer()
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

    def test_hdx_configuration_json(self, hdx_config_json, hdx_base_config_json, project_config_yaml):
        Configuration._create(user_agent='test', hdx_config_json=hdx_config_json,
                              hdx_base_config_json=hdx_base_config_json, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'hdx_site': 'test',
            'hdx_read_only': False,
            'hdx_key': '54321',
            'tags_cleanup_url': 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/HDX_Tags_Cleaning.csv',
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
            'resource': {'required_fields': ['name', 'description']},
            'showcase': {'required_fields': ['name']},
        }
        assert Configuration.read() == expected_configuration

    def test_hdx_configuration_yaml(self, hdx_config_yaml, hdx_base_config_yaml, project_config_yaml):
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml,
                              hdx_base_config_yaml=hdx_base_config_yaml, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'hdx_site': 'prod',
            'hdx_read_only': False,
            'hdx_key': '12345',
            'tags_cleanup_url': 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/HDX_Tags_Cleaning.csv',
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
            'resource': {'required_fields': ['package_id', 'name', 'description']},
            'showcase': {'required_fields': ['name', 'title']},
        }
        assert Configuration.read() == expected_configuration

    def test_project_configuration_dict(self, hdx_config_yaml):
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml)
        expected_configuration = {
            'hdx_site': 'prod',
            'hdx_read_only': False,
            'hdx_key': '12345',
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
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'license_id',
                'methodology',
                'tags'
            ]},
            'dataset-requestable': {'required_fields': [
                'name',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'tags',
                'field_names',
                'file_types',
                'num_of_rows'
            ]},
            'resource': {'required_fields': [
                'package_id',
                'name',
                'format',
                'description',
                'url_type',
                'resource_type']},
            'showcase': {'required_fields': [
                'name',
                'title',
                'notes',
                'url',
                'image_url',
                'tags'
            ]},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
            'organization': {'required_fields': [
                'name',
                'title',
                'description',
            ]},
            'resource view': {'required_fields': [
                'resource_id',
                'title',
                'view_type',
            ]},
            'tags_cleanup_url': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTyUP_oS878LihWM7iR0qry0OZP44BKyVXc1P_SoM0FEvbVGdOQxxlQvBWT_vwwGNaAbmuSeTg1FwuP/pub?gid=346948259&single=true&output=csv'
        }
        assert Configuration.read() == expected_configuration
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_dict={'abc': '123'})
        expected_configuration['abc'] = '123'
        assert Configuration.read() == expected_configuration

    def test_project_configuration_json(self, hdx_config_yaml, project_config_json):
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_json=project_config_json)
        expected_configuration = {
            'hdx_site': 'prod',
            'hdx_read_only': False,
            'hdx_key': '12345',
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
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'license_id',
                'methodology',
                'tags'
            ]},
            'dataset-requestable': {'required_fields': [
                'name',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'tags',
                'field_names',
                'file_types',
                'num_of_rows'
            ]},
            'resource': {'required_fields': [
                'package_id',
                'name',
                'format',
                'description',
                'url_type',
                'resource_type']},
            'showcase': {'required_fields': [
                'name',
                'title',
                'notes',
                'url',
                'image_url',
                'tags'
            ]},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
            'organization': {'required_fields': [
                'name',
                'title',
                'description',
            ]},
            'resource view': {'required_fields': [
                'resource_id',
                'title',
                'view_type',
            ]},
            'tags_cleanup_url': 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTyUP_oS878LihWM7iR0qry0OZP44BKyVXc1P_SoM0FEvbVGdOQxxlQvBWT_vwwGNaAbmuSeTg1FwuP/pub?gid=346948259&single=true&output=csv'
        }
        assert Configuration.read() == expected_configuration

    def test_project_configuration_yaml(self, hdx_config_yaml, project_config_yaml):
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'hdx_site': 'prod',
            'hdx_read_only': False,
            'hdx_key': '12345',
            'tags_cleanup_url': 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/HDX_Tags_Cleaning.csv',
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
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'license_id',
                'methodology',
                'tags'
            ]},
            'dataset-requestable': {'required_fields': [
                'name',
                'title',
                'notes',
                'dataset_source',
                'owner_org',
                'maintainer',
                'dataset_date',
                'data_update_frequency',
                'groups',
                'tags',
                'field_names',
                'file_types',
                'num_of_rows'
            ]},
            'resource': {'required_fields': [
                'package_id',
                'name',
                'format',
                'description',
                'url_type',
                'resource_type']},
            'showcase': {'required_fields': [
                'name',
                'title',
                'notes',
                'url',
                'image_url',
                'tags'
            ]},
            'user': {'required_fields': [
                'name',
                'email',
                'password',
                'fullname',
                'about',
            ], 'ignore_on_update': 'password'},
            'organization': {'required_fields': [
                'name',
                'title',
                'description',
            ]},
            'resource view': {'required_fields': [
                'resource_id',
                'title',
                'view_type',
            ]},
        }
        assert Configuration.read() == expected_configuration

    def test_get_hdx_key_site(self, hdx_config_yaml, project_config_yaml):
        Configuration._create(user_agent='test', hdx_config_yaml=hdx_config_yaml,
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        actual_configuration = Configuration.read()
        assert actual_configuration.get_api_key() == '12345'
        assert actual_configuration.get_hdx_site_url() == 'https://data.humdata.org/'
        assert actual_configuration._get_credentials() == ('', '')

    def test_set_hdx_key_value(self, project_config_yaml):
        Configuration._create(user_agent='test', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        configuration = Configuration.read()
        assert configuration.get_api_key() == 'TEST_HDX_KEY'
        configuration.set_api_key('NEW API KEY')
        assert configuration.get_api_key() == 'NEW API KEY'
        Configuration._create(user_agent='test', hdx_site='prod', hdx_read_only=True,
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().get_api_key() is None
        configuration = Configuration.read()
        configuration.set_api_key('TEST API KEY')
        assert configuration.get_api_key() is None
        configuration.set_read_only(False)
        assert configuration.get_api_key() == 'TEST API KEY'
        configuration.set_read_only(True)
        assert configuration.get_api_key() is None
        configuration.set_api_key('NEW API KEY')
        configuration.set_read_only(False)
        assert configuration.get_api_key() == 'NEW API KEY'

    def test_create_set_configuration(self, project_config_yaml):
        Configuration._create(user_agent='test', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        with pytest.raises(ConfigurationError):
            Configuration.create(user_agent='test', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                 hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        configuration = Configuration(user_agent='test', hdx_site='test', hdx_key='OTHER_TEST_HDX_KEY',
                                      hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        Configuration.setup(configuration)
        assert Configuration.read() == configuration
        Configuration.delete()
        with pytest.raises(ConfigurationError):
            Configuration.read()
        Configuration.create(user_agent='test', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                             hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().get_api_key() == 'TEST_HDX_KEY'

    def test_remoteckan_validlocations(self, project_config_yaml):
        Configuration._create(user_agent='test', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        remoteckan = ckanapi.RemoteCKAN('http://lalala', apikey='12345',
                                        user_agent='HDXPythonLibrary/1.0')
        Configuration.read().setup_remoteckan(remoteckan=remoteckan)
        assert Configuration.read().remoteckan() == remoteckan
        remoteckan = ckanapi.RemoteCKAN('http://hahaha', apikey='54321',
                                        user_agent='HDXPythonLibrary/0.5')
        Configuration._create(remoteckan=remoteckan,
                              hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={},
                              project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan() == remoteckan
        Configuration.read()._remoteckan = None
        with pytest.raises(ConfigurationError):
            Configuration.read().remoteckan()
        Configuration.delete()
        with pytest.raises(ConfigurationError):
            Configuration.read().remoteckan()

    def test_user_agent(self, user_agent_config_yaml, user_agent_config2_yaml, user_agent_config3_yaml,
                        empty_yaml, user_agent_config_wrong_yaml, project_config_yaml):
        Configuration._create(user_agent_config_yaml=user_agent_config_yaml, hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        version = Configuration.get_version()
        assert Configuration.read().remoteckan().user_agent == 'lala:HDXPythonLibrary/%s-myua' % version
        Configuration._create(user_agent_config_yaml=user_agent_config2_yaml, hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan().user_agent == 'HDXPythonLibrary/%s-myuseragent' % version
        Configuration._create(user_agent_config_yaml=user_agent_config3_yaml, user_agent_lookup='lookup',  hdx_site='prod',
                              hdx_key='TEST_HDX_KEY', hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan().user_agent == 'HDXPythonLibrary/%s-mylookupagent' % version
        Configuration._create(user_agent_config_yaml=user_agent_config3_yaml, user_agent_lookup='lookup2',  hdx_site='prod',
                              hdx_key='TEST_HDX_KEY', hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan().user_agent == 'HDXPythonLibrary/%s-mylookupagent2' % version
        Configuration._create(user_agent='my_ua', preprefix='papa', hdx_site='prod', hdx_key='TEST_HDX_KEY',
                              hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        assert Configuration.read().remoteckan().user_agent == 'papa:HDXPythonLibrary/%s-my_ua' % version
        with pytest.raises(ConfigurationError):
            Configuration._create(user_agent_config_yaml=user_agent_config3_yaml, user_agent_lookup='fail',
                                  hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                  hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        with pytest.raises(LoadError):
            Configuration._create(user_agent_config_yaml=empty_yaml, hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                  hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        with pytest.raises(ConfigurationError):
            Configuration._create(user_agent_config_yaml=user_agent_config_wrong_yaml, hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                  hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
        with pytest.raises(ConfigurationError):
            Configuration._create(hdx_site='prod', hdx_key='TEST_HDX_KEY',
                                  hdx_base_config_dict={}, project_config_yaml=project_config_yaml)
