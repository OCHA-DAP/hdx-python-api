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
    def test_wrapper(self):
        testresult.actual_result = None
        wrapper(my_testfn, collector_config_yaml=join('fixtures', 'config', 'collector_configuration.yml'))
        assert testresult.actual_result == 'https://test-data.humdata.org/'

    def test_exception(self):
        testresult.actual_result = None
        with pytest.raises(ValueError):
            wrapper(my_excfn, collector_config_yaml=join('fixtures', 'config', 'collector_configuration.yml'))
