# -*- coding: UTF-8 -*-
from hdx.configuration import Configuration


class TestResult:
    actual_result = None


testresult = TestResult()


def my_testfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()


def my_excfn():
    testresult.actual_result = Configuration.read().get_hdx_site_url()
    raise ValueError('Some failure!')
