"""Simple Facade Tests"""

import sys

import pytest

from . import my_testfnia, testresult
from hdx.api.configuration import ConfigurationError
from hdx.facades.infer_arguments import facade
from hdx.utilities.useragent import UserAgent


class TestInferArguments:
    def test_facade(
        self,
        monkeypatch,
        hdx_config_yaml,
        hdx_config_json,
        project_config_yaml,
    ):
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
                "--user-agent",
                "test",
            ],
        )
        testresult.actual_result = None
        facade(my_testfnia, hdx_site="stage", hdx_key="123")
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
        with pytest.raises(FileNotFoundError):
            facade(my_testfnia, hdx_config_yaml="NOT EXIST")

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
            facade(
                my_testfnia,
                hdx_config_yaml=hdx_config_yaml,
                hdx_config_json=hdx_config_json,
            )

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
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "test",
                "-h",
            ],
        )
        with pytest.raises(SystemExit):
            facade(my_testfnia)
        UserAgent.clear_global()
