"""Activity Tests"""

import json
from typing import ClassVar

import pytest

from .. import MockResponse
from hdx.api.configuration import Configuration
from hdx.data.activity import Activity
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.location import Location
from hdx.data.organization import Organization
from hdx.data.user import User

activity_id = "7d18e25e-c004-4e9f-8f6b-a4b4fe8a0159"
activity = {
    "id": activity_id,
    "timestamp": "2026-10-14T01:49:59.740998",
    "user_id": "52c8d2f3-a0d6-4cd9-8765-cf5cb4b67427",
    "object_id": "38772da7-c0b0-477e-a60e-a385a9dd4d64",
    "activity_type": "changed package",
    "permission_labels": ["public"],
}
activity_data = {"package": {"name": "test-dataset"}, "actor": "testuser"}
activity_diff = {"diff": "--- a\n+++ b\n", "activities": []}

total_activities = 250
activities = [
    activity | {"id": f"activity{i}", "timestamp": str(i)}
    for i in range(total_activities)
]


def mockresponse(result, action):
    return MockResponse(
        200,
        f'{{"success": true, "result": {json.dumps(result)}, "help": "http://test-data.humdata.org/api/3/action/help_show?name={action}"}}',
    )


def mocknotfound(action):
    return MockResponse(
        404,
        f'{{"success": false, "error": {{"message": "Not found", "__type": "Not Found Error"}}, "help": "http://test-data.humdata.org/api/3/action/help_show?name={action}"}}',
    )


class TestActivity:
    calls: ClassVar[list] = []

    @pytest.fixture(scope="function")
    def post_activity(self):
        TestActivity.calls = []

        class MockSession:
            @staticmethod
            def post(url, data, **kwargs):
                datadict = json.loads(data.decode("utf-8"))
                action = url.split("/")[-1]
                TestActivity.calls.append((action, datadict))
                if action.endswith("_activity_list"):
                    if (
                        action != "recently_changed_packages_activity_list"
                        and datadict["id"] != "TEST1"
                    ):
                        return mocknotfound(action)
                    offset = datadict.get("offset", 0)
                    limit = min(datadict.get("limit", 31), 100)
                    return mockresponse(activities[offset : offset + limit], action)
                if datadict["id"] != activity_id:
                    return mocknotfound(action)
                if action == "activity_show":
                    return mockresponse(activity, action)
                if action == "activity_data_show":
                    return mockresponse(activity_data, action)
                if action == "activity_diff":
                    return mockresponse(activity_diff, action)
                return mocknotfound(action)

        Configuration.read().remoteckan().session = MockSession()

    def test_read_from_hdx(self, configuration, post_activity):
        result = Activity.read_from_hdx(activity_id)
        assert result.data == activity
        assert Activity.read_from_hdx("TEST2") is None

    def test_get_object_type(self, configuration):
        assert Activity(activity).get_object_type() == "package"
        result = Activity({"activity_type": "new organization"}).get_object_type()
        assert result == "organization"
        result = Activity({"activity_type": "follow dataset"}).get_object_type()
        assert result == "package"

    def test_get_data_and_diff(self, configuration, post_activity):
        result = Activity(activity)
        assert result.get_data() == activity_data
        assert result.get_diff() == activity_diff
        assert TestActivity.calls[-1] == (
            "activity_diff",
            {"id": activity_id, "object_type": "package", "diff_type": "unified"},
        )
        result.get_diff("html")
        assert TestActivity.calls[-1][1]["diff_type"] == "html"
        result = Activity({"id": "TEST2", "activity_type": "changed package"})
        with pytest.raises(HDXError):
            result.get_data()
        with pytest.raises(HDXError):
            result.get_diff()

    def test_get_activities(self, configuration, post_activity):
        result = Activity.get_activities("package", "TEST1")
        assert len(result) == 31
        assert isinstance(result[0], Activity)
        assert result[0]["id"] == "activity0"
        result = Activity.get_activities("package", "TEST1", offset=10, limit=5)
        assert [x["id"] for x in result] == [f"activity{i}" for i in range(10, 15)]
        assert TestActivity.calls[-1] == (
            "package_activity_list",
            {"id": "TEST1", "offset": 10, "limit": 5},
        )
        with pytest.raises(HDXError):
            Activity.get_activities("package", "TEST2")
        with pytest.raises(HDXError):
            Activity.get_activities("package", None)

    def test_get_activities_all_pages(self, configuration, post_activity):
        result = Activity.get_activities("package", "TEST1", all_pages=True)
        assert [x["id"] for x in result] == [x["id"] for x in activities]
        assert [x[1]["offset"] for x in TestActivity.calls] == [0, 100, 200]
        TestActivity.calls = []
        result = Activity.get_activities(
            "package", "TEST1", all_pages=True, offset=150, limit=50
        )
        assert [x["id"] for x in result] == [x["id"] for x in activities[150:]]
        assert [x[1]["offset"] for x in TestActivity.calls] == [150, 200, 250]
        TestActivity.calls = []
        result = Activity.get_activities("package", "TEST1", all_pages=True, limit=500)
        assert [x["id"] for x in result] == [x["id"] for x in activities]
        assert [x[1]["limit"] for x in TestActivity.calls] == [100, 100, 100]
        assert [x[1]["offset"] for x in TestActivity.calls] == [0, 100, 200]

    def test_get_recently_changed_packages(self, configuration, post_activity):
        result = Activity.get_recently_changed_packages(limit=3)
        assert [x["id"] for x in result] == ["activity0", "activity1", "activity2"]
        assert TestActivity.calls[-1] == (
            "recently_changed_packages_activity_list",
            {"limit": 3},
        )
        TestActivity.calls = []
        result = Activity.get_recently_changed_packages(all_pages=True)
        assert len(result) == total_activities
        assert [x[1]["offset"] for x in TestActivity.calls] == [0, 100, 200]
        TestActivity.calls = []
        result = Activity.get_recently_changed_packages(
            all_pages=True, max_activities=120
        )
        assert [x["id"] for x in result] == [x["id"] for x in activities[:120]]
        assert [x[1]["offset"] for x in TestActivity.calls] == [0, 100]
        result = Activity.get_recently_changed_packages(
            all_pages=True, max_activities=100
        )
        assert len(result) == 100
        result = Activity.get_recently_changed_packages(
            all_pages=True, max_activities=None, limit=50
        )
        assert len(result) == total_activities

    @pytest.mark.parametrize(
        "hdxobjectclass,action",
        [
            (Dataset, "package_activity_list"),
            (Organization, "organization_activity_list"),
            (Location, "group_activity_list"),
            (User, "user_activity_list"),
        ],
    )
    def test_hdxobject_get_activities(
        self, configuration, post_activity, hdxobjectclass, action
    ):
        hdxobject = hdxobjectclass({"name": "TEST1"})
        result = hdxobject.get_activities(limit=2)
        assert [x["id"] for x in result] == ["activity0", "activity1"]
        assert isinstance(result[0], Activity)
        assert TestActivity.calls[-1] == (action, {"id": "TEST1", "limit": 2})
        hdxobject = hdxobjectclass({"id": "TEST1", "name": "TEST2"})
        hdxobject.get_activities()
        assert TestActivity.calls[-1] == (action, {"id": "TEST1"})
        with pytest.raises(HDXError):
            hdxobjectclass().get_activities()

    def test_read_only(self, configuration):
        result = Activity(activity)
        with pytest.raises(HDXError):
            result.check_required_fields()
        with pytest.raises(HDXError):
            result.create_in_hdx()
        with pytest.raises(HDXError):
            result.update_in_hdx()
        with pytest.raises(HDXError):
            result.delete_from_hdx()
