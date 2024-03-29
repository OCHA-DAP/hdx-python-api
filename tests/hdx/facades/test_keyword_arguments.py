"""Simple Facade Tests"""

import pytest

from . import my_testfnkw, testresult
from hdx.api import __version__
from hdx.facades.keyword_arguments import facade
from hdx.utilities.useragent import UserAgent, UserAgentError


class TestKeywordArguments:
    def test_facade(self, monkeypatch, hdx_config_yaml, project_config_yaml):
        UserAgent.clear_global()
        my_user_agent = "test"
        testresult.actual_result = None
        facade(
            my_testfnkw,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="site",
        )
        assert testresult.actual_result == "https://data.humdata.org"
        UserAgent.clear_global()
        testresult.actual_result = None
        facade(
            my_testfnkw,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="agent",
        )
        assert (
            testresult.actual_result
            == f"HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        my_user_agent = "lala"
        monkeypatch.setenv("USER_AGENT", my_user_agent)
        facade(
            my_testfnkw,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="agent",
        )
        assert (
            testresult.actual_result
            == f"HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        facade(
            my_testfnkw,
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="agent",
        )
        assert (
            testresult.actual_result
            == f"HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        my_preprefix = "haha"
        monkeypatch.setenv("PREPREFIX", my_preprefix)
        facade(
            my_testfnkw,
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="agent",
        )
        assert (
            testresult.actual_result
            == f"{my_preprefix}:HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        my_test_key = "1234"
        facade(
            my_testfnkw,
            hdx_key=my_test_key,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="api",
        )
        assert testresult.actual_result == my_test_key
        UserAgent.clear_global()
        testresult.actual_result = None
        monkeypatch.setenv("HDX_KEY", "aaaa")
        facade(
            my_testfnkw,
            hdx_key=my_test_key,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="api",
        )
        assert testresult.actual_result == my_test_key
        UserAgent.clear_global()
        testresult.actual_result = None
        my_test_hdxsite = "stage"
        facade(
            my_testfnkw,
            hdx_site=my_test_hdxsite,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="site",
        )
        assert (
            testresult.actual_result
            == f"https://{my_test_hdxsite}.data-humdata-org.ahconu.org"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        monkeypatch.setenv("HDX_SITE", "feature")
        facade(
            my_testfnkw,
            hdx_site=my_test_hdxsite,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="site",
        )
        assert (
            testresult.actual_result
            == f"https://{my_test_hdxsite}.data-humdata-org.ahconu.org"
        )
        UserAgent.clear_global()
        my_test_hdxurl = "http://other-data.humdata.org"
        monkeypatch.setenv("HDX_URL", my_test_hdxurl)
        facade(
            my_testfnkw,
            hdx_site="feature",
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="site",
        )
        assert testresult.actual_result == my_test_hdxurl
        UserAgent.clear_global()
        my_test_hdxurl2 = "http://other2-data.humdata.org/"
        monkeypatch.setenv("HDX_URL", my_test_hdxurl2)
        facade(
            my_testfnkw,
            hdx_url=my_test_hdxurl,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
            fn="site",
        )
        assert testresult.actual_result == my_test_hdxurl
        UserAgent.clear_global()

    def test_exception(self, hdx_config_yaml, project_config_yaml):
        UserAgent.clear_global()
        testresult.actual_result = None
        with pytest.raises(ValueError):
            facade(
                my_testfnkw,
                user_agent="test",
                hdx_config_yaml=hdx_config_yaml,
                project_config_yaml=project_config_yaml,
                fn="exc",
            )
        UserAgent.clear_global()
        with pytest.raises(UserAgentError):
            facade(
                my_testfnkw,
                hdx_config_yaml=hdx_config_yaml,
                project_config_yaml=project_config_yaml,
                fn="agent",
            )
        UserAgent.clear_global()
