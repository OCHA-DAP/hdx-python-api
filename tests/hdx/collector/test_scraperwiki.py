#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""ScraperWiki Wrapper Tests"""
from os.path import join

from hdx.collector import logging_kwargs

logging_kwargs.update({
    'smtp_config_yaml': join('fixtures', 'config', 'smtp_config.yml'),
})

from . import my_testfn, my_excfn, testresult
from hdx.collector.scraperwiki import wrapper


class TestScraperWiki():
    def test_wrapper(self):
        testresult.actual_result = None
        wrapper(my_testfn, collector_config_yaml=join('fixtures', 'config', 'collector_configuration.yml'))
        assert testresult.actual_result == 'https://test-data.humdata.org/'

    def test_exception(self):
        testresult.actual_result = None
        wrapper(my_excfn, collector_config_yaml=join('fixtures', 'config', 'collector_configuration.yml'))
        assert testresult.actual_result == 'https://test-data.humdata.org/'
