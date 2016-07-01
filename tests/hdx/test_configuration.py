#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Configuration Tests"""
from os.path import join

import pytest

from hdx.configuration import Configuration, ConfigurationError


class TestConfiguration():
    @pytest.fixture(scope='class')
    def hdx_key_file(self):
        return join('fixtures', '.hdxkey')

    @pytest.fixture(scope='class')
    def project_config_yaml(self):
        return join('fixtures', 'config', 'project_configuration.yml')

    @pytest.fixture(scope='class')
    def project_config_json(self):
        return join('fixtures', 'config', 'project_configuration.json')

    def test_init(self, hdx_key_file, project_config_json, project_config_yaml):
        with pytest.raises(FileNotFoundError):
            Configuration()

        with pytest.raises(FileNotFoundError):
            Configuration(hdx_key_file='NOT_EXIST', project_config_yaml=project_config_yaml)

        with pytest.raises(FileNotFoundError):
            Configuration(hdx_key_file=hdx_key_file, hdx_config_yaml='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(FileNotFoundError):
            Configuration(hdx_key_file=hdx_key_file, hdx_config_json='NOT_EXIST',
                          project_config_yaml=project_config_yaml)

        with pytest.raises(FileNotFoundError):
            Configuration(hdx_key_file=hdx_key_file, project_config_yaml='NOT_EXIST')

        with pytest.raises(FileNotFoundError):
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

    def test_hdx_configuration_dict(self, hdx_key_file, project_config_yaml):
        actual_configuration = Configuration(hdx_site='uat', hdx_key_file=hdx_key_file,
                                             hdx_config_dict={'XYZ': {'567': 987}},
                                             project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'hdx_uat_site': 'https://uat-data.humdata.org/',
            'XYZ': {'567': 987}
        }
        assert actual_configuration == expected_configuration

    def test_hdx_configuration_json(self, hdx_key_file, project_config_yaml):
        hdx_config_json = join('fixtures', 'config', 'hdx_config.json')
        actual_configuration = Configuration(hdx_key_file=hdx_key_file, hdx_config_json=hdx_config_json,
                                             project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': 'https://data.humdata.org/',
            'hdx_uat_site': 'https://uat-data.humdata.org/',
            'hdx_test_site': 'https://test-data.humdata.org/',
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
        assert actual_configuration == expected_configuration

    def test_hdx_configuration_yaml(self, hdx_key_file, project_config_yaml):
        hdx_configuration_yaml = join('fixtures', 'config', 'hdx_config.yml')
        actual_configuration = Configuration(hdx_key_file=hdx_key_file, hdx_config_yaml=hdx_configuration_yaml,
                                             project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': 'https://data.humdata.org/',
            'hdx_uat_site': 'https://uat-data.humdata.org/',
            'hdx_test_site': 'https://test-data.humdata.org/',
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
        assert actual_configuration == expected_configuration

    def test_project_configuration_dict(self, hdx_key_file):
        actual_configuration = Configuration(hdx_key_file=hdx_key_file, project_config_dict={'abc': '123'})
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': 'https://data.humdata.org/',
            'hdx_test_site': 'https://test-data.humdata.org/',
            'abc': '123',
            'dataset': {'required_fields': [
                'name',
                'title',
                'dataset_date',
                'groups',
                'owner_org',
                'author',
                'author_email',
                'maintainer',
                'maintainer_email',
                'license_id',
                'subnational',
                'notes',
                'caveats',
                'data_update_frequency',
                'methodology',
                'methodology_other',
                'dataset_source',
                'package_creator',
                'private',
                'url',
                'state',
                'tags',
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': ['dataset_id']},
        }
        assert actual_configuration == expected_configuration

    def test_project_configuration_json(self, hdx_key_file, project_config_json):
        actual_configuration = Configuration(hdx_key_file=hdx_key_file, project_config_json=project_config_json)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': 'https://data.humdata.org/',
            'hdx_test_site': 'https://test-data.humdata.org/',
            'my_param': 'abc',
            'dataset': {'required_fields': [
                'name',
                'title',
                'dataset_date',
                'groups',
                'owner_org',
                'author',
                'author_email',
                'maintainer',
                'maintainer_email',
                'license_id',
                'subnational',
                'notes',
                'caveats',
                'data_update_frequency',
                'methodology',
                'methodology_other',
                'dataset_source',
                'package_creator',
                'private',
                'url',
                'state',
                'tags',
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': ['dataset_id']},
        }
        assert actual_configuration == expected_configuration

    def test_project_configuration_yaml(self, hdx_key_file, project_config_yaml):
        actual_configuration = Configuration(hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)
        expected_configuration = {
            'api_key': '12345',
            'hdx_prod_site': 'https://data.humdata.org/',
            'hdx_uat_site': 'https://uat-data.humdata.org/',
            'hdx_test_site': 'https://test-data.humdata.org/',
            'dataset': {'required_fields': [
                'name',
                'title',
                'dataset_date',
                'groups',
                'owner_org',
                'author',
                'author_email',
                'maintainer',
                'maintainer_email',
                'license_id',
                'subnational',
                'notes',
                'caveats',
                'data_update_frequency',
                'methodology',
                'methodology_other',
                'dataset_source',
                'package_creator',
                'private',
                'url',
                'state',
                'tags',
            ]},
            'resource': {'dataset_id': 'package_id',
                         'required_fields': ['package_id', 'name', 'format', 'url', 'description'
                                             ]},
            'galleryitem': {'dataset_id': 'dataset_id', 'required_fields': [
                'dataset_id',
                'title',
                'type',
                'description',
                'url',
                'image_url',
            ], 'ignore_on_update': ['dataset_id']},
        }
        assert actual_configuration == expected_configuration

    def test_get_hdx_key_site(self, hdx_key_file, project_config_yaml):
        actual_configuration = Configuration(hdx_site='uat', hdx_key_file=hdx_key_file,
                                             hdx_config_dict={},
                                             project_config_yaml=project_config_yaml)
        assert actual_configuration.get_api_key() == '12345'
        assert actual_configuration.get_hdx_site() == 'https://uat-data.humdata.org/'
