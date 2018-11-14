# -*- coding: UTF-8 -*-
"""Simple Facade Tests"""
from os.path import join

from hdx.facades import logging_kwargs
from hdx.hdx_configuration import ConfigurationError, Configuration

logging_kwargs.update({
    'smtp_config_yaml': join('tests', 'fixtures', 'config', 'smtp_config.yml'),
})

import pytest

from . import my_testfn, my_excfn, testresult, my_testuafn, my_testkeyfn
from hdx.facades.simple import facade


class TestSimple:
    def test_facade(self, monkeypatch, hdx_config_yaml, project_config_yaml):
        my_user_agent = 'test'
        testresult.actual_result = None
        facade(my_testfn, user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'https://data.humdata.org/'
        testresult.actual_result = None
        facade(my_testuafn, user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'HDXPythonLibrary/%s-%s' % (Configuration.get_version(), my_user_agent)
        testresult.actual_result = None
        my_user_agent = 'lala'
        monkeypatch.setenv('USER_AGENT', my_user_agent)
        facade(my_testuafn, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'HDXPythonLibrary/%s-%s' % (Configuration.get_version(), my_user_agent)
        testresult.actual_result = None
        facade(my_testuafn, user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'HDXPythonLibrary/%s-%s' % (Configuration.get_version(), my_user_agent)
        testresult.actual_result = None
        my_preprefix = 'haha'
        monkeypatch.setenv('PREPREFIX', my_preprefix)
        facade(my_testuafn, user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == '%s:HDXPythonLibrary/%s-%s' % (my_preprefix, Configuration.get_version(), my_user_agent)
        testresult.actual_result = None
        my_test_key = '1234'
        facade(my_testkeyfn, hdx_key=my_test_key, user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == my_test_key
        testresult.actual_result = None
        monkeypatch.setenv('HDX_KEY', my_test_key)
        facade(my_testkeyfn, hdx_key='aaaa', user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == my_test_key
        testresult.actual_result = None
        my_test_hdxsite = 'test'
        facade(my_testfn, hdx_site=my_test_hdxsite, user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'https://%s-data.humdata.org/' % my_test_hdxsite
        testresult.actual_result = None
        monkeypatch.setenv('HDX_SITE', my_test_hdxsite)
        facade(my_testfn, hdx_site='feature', user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == 'https://%s-data.humdata.org/' % my_test_hdxsite
        my_test_hdxurl = 'http://other-data.humdata.org'
        monkeypatch.setenv('HDX_URL', my_test_hdxurl)
        facade(my_testfn, hdx_site='feature', user_agent=my_user_agent, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        assert testresult.actual_result == my_test_hdxurl

    def test_exception(self, hdx_config_yaml, project_config_yaml):
        testresult.actual_result = None
        with pytest.raises(ValueError):
            facade(my_excfn, user_agent='test', hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
        with pytest.raises(ConfigurationError):
            facade(my_testuafn, hdx_config_yaml=hdx_config_yaml, project_config_yaml=project_config_yaml)
