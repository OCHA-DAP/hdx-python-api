#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Simple Wrapper Tests"""
from os.path import join

from hdx.collector import logging_kwargs

logging_kwargs.update({
    'smtp_config_yaml': join('fixtures', 'config', 'smtp_config.yml'),
})

import pytest

from . import my_testfn, my_excfn, testresult
from hdx.collector.simple import wrapper


class TestSimple():
    @pytest.fixture(scope='class')
    def hdx_key_file(self):
        return join('fixtures', '.hdxkey')

    @pytest.fixture(scope='class')
    def collector_config_yaml(self):
        return join('fixtures', 'config', 'collector_configuration.yml')

    def test_wrapper(self, hdx_key_file, collector_config_yaml):
        testresult.actual_result = None
        wrapper(my_testfn, hdx_key_file=hdx_key_file, collector_config_yaml=collector_config_yaml)
        assert testresult.actual_result == 'https://test-data.humdata.org/'

    def test_exception(self, hdx_key_file, collector_config_yaml):
        testresult.actual_result = None
        with pytest.raises(ValueError):
            wrapper(my_excfn, hdx_key_file=hdx_key_file, collector_config_yaml=collector_config_yaml)
