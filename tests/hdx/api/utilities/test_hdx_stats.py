"""HDX Stats Tests"""

import json

import pytest

from ... import MockResponse
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_stats import get_general_statistics, push_general_stats
from hdx.data.hdxobject import HDXError

general_statistics = {
    "datasets": {"total": 100, "with_geodata": 20, "with_showcases": 10},
    "organizations": {"total": 30, "with_datasets": 25},
    "users": {"total": 1000},
}

pushed_stats = {"mixpanel_meta": {"datasets total": 100, "orgs total": 30}}


class TestHDXStats:
    succeed = True

    @pytest.fixture(scope="function")
    def post_stats(self):
        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                if TestHDXStats.succeed:
                    if "hdx_general_statistics" in url:
                        result = json.dumps(general_statistics)
                    elif "hdx_push_general_stats" in url:
                        result = json.dumps(pushed_stats)
                    else:
                        result = None
                    if result:
                        return MockResponse(
                            200,
                            f'{{"success": true, "result": {result}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=stats"}}',
                        )
                return MockResponse(
                    403,
                    '{"success": false, "error": {"message": "Access denied", "__type": "Authorization Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=stats"}',
                )

        Configuration.read().remoteckan().session = MockSession()

    def test_get_general_statistics(self, configuration, post_stats):
        assert get_general_statistics() == general_statistics
        assert get_general_statistics(Configuration.read()) == general_statistics

    def test_push_general_stats(self, configuration, post_stats):
        assert push_general_stats() == pushed_stats
        TestHDXStats.succeed = False
        with pytest.raises(HDXError):
            push_general_stats()
        with pytest.raises(HDXError):
            get_general_statistics()
        TestHDXStats.succeed = True
