"""Simple Facade Tests"""
import pytest
from hdx.utilities.useragent import UserAgent, UserAgentError

from hdx.api import __version__
from hdx.facades.simple import facade

from . import my_excfn, my_testfn, my_testkeyfn, my_testuafn, testresult


class TestSimple:
    def test_facade(self, monkeypatch, hdx_config_yaml, project_config_yaml):
        UserAgent.clear_global()
        my_user_agent = "test"
        testresult.actual_result = None
        facade(
            my_testfn,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert testresult.actual_result == "https://data.humdata.org"
        UserAgent.clear_global()
        testresult.actual_result = None
        facade(
            my_testuafn,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
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
            my_testuafn,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert (
            testresult.actual_result
            == f"HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        facade(
            my_testuafn,
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
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
            my_testuafn,
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert (
            testresult.actual_result
            == f"{my_preprefix}:HDXPythonLibrary/{__version__}-{my_user_agent}"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        my_test_key = "1234"
        facade(
            my_testkeyfn,
            hdx_key=my_test_key,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert testresult.actual_result == my_test_key
        UserAgent.clear_global()
        testresult.actual_result = None
        monkeypatch.setenv("HDX_KEY", my_test_key)
        facade(
            my_testkeyfn,
            hdx_key="aaaa",
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert testresult.actual_result == my_test_key
        UserAgent.clear_global()
        testresult.actual_result = None
        my_test_hdxsite = "stage"
        facade(
            my_testfn,
            hdx_site=my_test_hdxsite,
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert (
            testresult.actual_result
            == f"https://{my_test_hdxsite}.data-humdata-org.ahconu.org"
        )
        UserAgent.clear_global()
        testresult.actual_result = None
        monkeypatch.setenv("HDX_SITE", my_test_hdxsite)
        facade(
            my_testfn,
            hdx_site="feature",
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert (
            testresult.actual_result
            == f"https://{my_test_hdxsite}.data-humdata-org.ahconu.org"
        )
        UserAgent.clear_global()
        my_test_hdxurl = "http://other-data.humdata.org"
        monkeypatch.setenv("HDX_URL", my_test_hdxurl)
        facade(
            my_testfn,
            hdx_site="feature",
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert testresult.actual_result == my_test_hdxurl
        UserAgent.clear_global()
        my_test_hdxurl2 = "http://other-data.humdata.org/"
        monkeypatch.setenv("HDX_URL", my_test_hdxurl2)
        facade(
            my_testfn,
            hdx_site="feature",
            user_agent=my_user_agent,
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        assert testresult.actual_result == my_test_hdxurl
        UserAgent.clear_global()

    def test_exception(self, hdx_config_yaml, project_config_yaml):
        UserAgent.clear_global()
        testresult.actual_result = None
        with pytest.raises(ValueError):
            facade(
                my_excfn,
                user_agent="test",
                hdx_config_yaml=hdx_config_yaml,
                project_config_yaml=project_config_yaml,
            )
        UserAgent.clear_global()
        with pytest.raises(UserAgentError):
            facade(
                my_testuafn,
                hdx_config_yaml=hdx_config_yaml,
                project_config_yaml=project_config_yaml,
            )
        UserAgent.clear_global()
