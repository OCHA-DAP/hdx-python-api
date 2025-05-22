"""HDXState Utility Tests"""

import json
from copy import deepcopy
from datetime import datetime, timezone
from os import makedirs
from os.path import exists, join
from shutil import copyfile, rmtree
from tempfile import gettempdir

import pytest

from ... import MockResponse, dataset_resultdict
from ...data.test_resource import resultdict
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_state import HDXState
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date


class TestState:
    @pytest.fixture(scope="class")
    def tempfolder(self):
        return join(gettempdir(), "test_state")

    @pytest.fixture(scope="class")
    def statefolder(self, fixturesfolder):
        return join(fixturesfolder, "state")

    @pytest.fixture(scope="class")
    def statefile(self):
        return "last_build_date.txt"

    @pytest.fixture(scope="class")
    def multidatestatefile(self):
        return "analysis_dates.txt"

    @pytest.fixture(scope="class")
    def date1(self):
        return datetime(2020, 9, 23, 0, 0, tzinfo=timezone.utc)

    @pytest.fixture(scope="class")
    def date2(self):
        return datetime(2022, 5, 12, 10, 15, tzinfo=timezone.utc)

    @pytest.fixture(scope="function")
    def do_state(self, tempfolder, statefile):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                if "resource" in url:
                    result = json.dumps(resultdict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}'
                        % result,
                    )
                else:
                    myresultdict = deepcopy(dataset_resultdict)
                    resource = myresultdict["resources"][0]
                    resource["name"] = statefile
                    resource["url"] = join(tempfolder, statefile)
                    result = json.dumps(myresultdict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}'
                        % result,
                    )

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def do_state_multi(self, tempfolder, multidatestatefile):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                if "resource" in url:
                    result = json.dumps(resultdict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}'
                        % result,
                    )
                else:
                    myresultdict = deepcopy(dataset_resultdict)
                    resource = myresultdict["resources"][0]
                    resource["name"] = multidatestatefile
                    resource["url"] = join(tempfolder, multidatestatefile)
                    result = json.dumps(myresultdict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}'
                        % result,
                    )

        Configuration.read().remoteckan().session = MockSession()

    def test_state(
        self, tempfolder, statefolder, statefile, date1, date2, configuration, do_state
    ):
        if not exists(tempfolder):
            makedirs(tempfolder)
        statepath = join(tempfolder, statefile)
        copyfile(join(statefolder, statefile), statepath)
        with HDXState(
            "test_dataset", tempfolder, parse_date, iso_string_from_datetime
        ) as state:
            assert state.get() == date1
        with HDXState(
            "test_dataset", tempfolder, parse_date, iso_string_from_datetime
        ) as state:
            assert state.get() == date1
            state.set(date2)
        with HDXState(
            "test_dataset", tempfolder, parse_date, iso_string_from_datetime
        ) as state:
            assert state.get() == date2.replace(hour=0, minute=0)
        rmtree(tempfolder)

    def test_multi_date_state(
        self,
        tempfolder,
        statefolder,
        multidatestatefile,
        date1,
        date2,
        configuration,
        do_state_multi,
    ):
        if not exists(tempfolder):
            makedirs(tempfolder)
        statepath = join(tempfolder, multidatestatefile)
        copyfile(join(statefolder, multidatestatefile), statepath)
        with HDXState(
            "test_dataset",
            tempfolder,
            HDXState.dates_str_to_country_date_dict,
            HDXState.country_date_dict_to_dates_str,
        ) as state:
            state_dict = state.get()
            assert state_dict == {"DEFAULT": date1}
        with HDXState(
            "test_dataset",
            tempfolder,
            HDXState.dates_str_to_country_date_dict,
            HDXState.country_date_dict_to_dates_str,
        ) as state:
            state_dict = state.get()
            assert state_dict == {"DEFAULT": date1}
            state_dict["AFG"] = date2
            state.set(state_dict)
        with HDXState(
            "test_dataset",
            tempfolder,
            HDXState.dates_str_to_country_date_dict,
            HDXState.country_date_dict_to_dates_str,
        ) as state:
            state_dict = state.get()
            assert state_dict == {
                "DEFAULT": date1,
                "AFG": date2.replace(hour=0, minute=0),
            }
        rmtree(tempfolder)
