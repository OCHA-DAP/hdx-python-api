# -*- coding: UTF-8 -*-
"""Simple Facade Tests"""
from os.path import join

from hdx.facades import logging_kwargs

logging_kwargs.update({
    'smtp_config_yaml': join('tests', 'fixtures', 'config', 'smtp_config.yml'),
})

import pytest

from . import my_testfn, my_excfn, testresult
from hdx.facades.simple import facade


class TestSimple:
    @pytest.fixture(scope='class')
    def hdx_key_file(self):
        return join('tests', 'fixtures', '.hdxkey')

    @pytest.fixture(scope='class')
    def project_config_yaml(self):
        return join('tests', 'fixtures', 'config', 'project_configuration.yml')

    def test_facade(self, hdx_key_file, project_config_yaml):
        testresult.actual_result = None
        facade(my_testfn, hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'https://test-data.humdata.org/'

    def test_exception(self, hdx_key_file, project_config_yaml):
        testresult.actual_result = None
        with pytest.raises(ValueError):
            facade(my_excfn, hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)
