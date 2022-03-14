"""Simple Facade Tests"""
import sys

import pytest
from hdx.utilities.useragent import UserAgent

from hdx.api.configuration import ConfigurationError
from hdx.facades.infer_arguments import facade

from . import my_testfnia, testresult


class TestInferArguments:
    def test_facade(self, monkeypatch, hdx_config_yaml, project_config_yaml):
        UserAgent.clear_global()
        mydata = "hello"
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "test",
                "--mydata",
                mydata,
                "--hdx-site",
                "prod",
                "--hdx-key",
                "123",
                "--user-agent",
                "test",
            ],
        )
        testresult.actual_result = None
        facade(my_testfnia)
        assert testresult.actual_result == mydata

        UserAgent.clear_global()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "test",
                "--mydata",
                mydata,
                "--hdx-site",
                "prod",
                "--user-agent",
                "test",
            ],
        )
        with pytest.raises(ConfigurationError):
            facade(my_testfnia)

        UserAgent.clear_global()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "test",
                "--mydata",
                mydata,
                "--hdx-site",
                "prod",
                "--hdx-key",
                "123",
                "--user-agent",
                "test",
                "--lala",
                "what",
            ],
        )
        with pytest.raises(SystemExit):
            facade(my_testfnia)
        UserAgent.clear_global()
