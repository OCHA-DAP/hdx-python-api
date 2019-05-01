# -*- coding: UTF-8 -*-
from hdx.hdx_configuration import Configuration


class TestResult:
    actual_result = None


testresult = TestResult()


def my_testfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()


def my_testkeyfn():
    testresult.actual_result = Configuration.read().get_api_key()


def my_testuafn():
    testresult.actual_result = Configuration.read().user_agent


def my_excfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()
    raise ValueError('Some failure!')


def my_testfnkw(**kwargs):
    fn = kwargs.get('fn')
    if fn == 'site':
        testresult.actual_result = Configuration.read().get_hdx_site_url()
    elif fn == 'api':
        testresult.actual_result = Configuration.read().get_api_key()
    elif fn == 'agent':
        testresult.actual_result = Configuration.read().user_agent
    elif fn == 'exc':
        testresult.actual_result = Configuration.read().get_hdx_site_url()
        raise ValueError('Some failure!')
