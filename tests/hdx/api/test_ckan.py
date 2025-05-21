"""
Unit tests for the freshness class.

"""

import json
import logging
import os
import random
from os import getenv
from os.path import join
from time import sleep

import gspread
import pytest
from gspread.urls import DRIVE_FILES_API_V3_URL

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.user import User
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.dateparse import now_utc

logger = logging.getLogger(__name__)


class TestCKAN:
    @pytest.fixture(scope="class")
    def configuration(self):
        hdx_key = getenv("HDX_KEY_TEST")
        Configuration._create(
            hdx_site="stage",
            user_agent="test",
            hdx_key=hdx_key,
        )
        User.check_current_user_write_access("5a63012e-6c41-420c-8c33-e84b277fdc90")
        Locations._validlocations = None
        Country.countriesdata(use_live=False)
        Vocabulary._approved_vocabulary = None
        Vocabulary._tags_dict = None

    @pytest.fixture(scope="function")
    def datasetmetadata(self):
        return join("tests", "fixtures", "CKAN", "hdx_dataset_static.yaml")

    @pytest.fixture(scope="function")
    def testdata(self):
        return join("tests", "fixtures", "test_data.csv")

    @pytest.fixture(scope="class")
    def params(self):
        return {
            "corpora": "teamDrive",
            "teamDriveId": "0AKCBfHI3H-hcUk9PVA",
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }

    @pytest.fixture(scope="function")
    def gclient(self):
        gsheet_auth = getenv("GSHEET_AUTH")
        if not gsheet_auth:
            auth_file_path = os.path.join(os.path.expanduser("~"), ".gsheet_auth.json")
            if os.path.exists(auth_file_path):
                with open(auth_file_path, encoding="utf-8") as auth_file:
                    gsheet_auth = auth_file.read()
            else:
                raise ValueError("No gsheet authorisation supplied!")
        info = json.loads(gsheet_auth)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        gclient = gspread.service_account_from_dict(info, scopes=scopes)
        return gclient

    @pytest.fixture(scope="function")
    def setup_teardown_folder(self, configuration, gclient, params):
        try:
            payload = {
                "name": "hdx_python_api_test_tmp",
                "mimeType": "application/vnd.google-apps.folder",
                "parents": ["1dvx0H0RG5ZfM9QL148uymWpbuxAqmOzD"],
            }
            r = gclient.http_client.request(
                "post", DRIVE_FILES_API_V3_URL, json=payload, params=params
            )
            folderid = r.json()["id"]
            yield gclient, folderid
        finally:
            payload = {"trashed": True}
            url = f"{DRIVE_FILES_API_V3_URL}/{folderid}"
            gclient.http_client.request("patch", url, json=payload, params=params)
            Vocabulary._approved_vocabulary = None
            Vocabulary._tags_dict = None
            Configuration.delete()

    def test_create_dataset(
        self,
        datasetmetadata,
        testdata,
        setup_teardown_folder,
        params,
    ):
        today = now_utc()
        gclient, folderid = setup_teardown_folder

        def create_gsheet(name, update):
            payload = {
                "name": name,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folderid],
            }
            r = gclient.http_client.request(
                "post", DRIVE_FILES_API_V3_URL, json=payload, params=params
            )
            spreadsheetid = r.json()["id"]
            gsheet = gclient.open_by_key(spreadsheetid)
            wks = gsheet.sheet1
            wks.update(update, "A1")
            gsheet.share("", role="reader", perm_type="anyone")
            return wks, f"{gsheet.url}/export?format=xlsx"

        name = "hdx_python_api_test"
        dataset = Dataset.read_from_hdx(name)
        if dataset:
            dataset.delete_from_hdx()
        title = "HDX Python API test"
        dataset = Dataset({"name": name, "title": title})
        dataset.update_from_yaml(datasetmetadata)
        maintainer_id = "196196be-6037-4488-8b71-d786adf4c081"
        dataset.set_maintainer(maintainer_id)
        dataset.set_organization("5a63012e-6c41-420c-8c33-e84b277fdc90")
        dataset.set_time_period(today)
        dataset.set_expected_update_frequency("Every week")
        dataset.set_subnational(True)
        countryiso3s = ["AFG", "PSE", "SYR", "YEM"]
        dataset.add_country_locations(countryiso3s)
        tags = ["conflict-violence", "displacement", "hxl"]
        dataset.add_tags(tags)
        resource_no = 0

        def create_resource():
            nonlocal resource_no

            resource = Resource(
                {
                    "name": f"test_resource_{resource_no}",
                    "description": f"Test Resource {resource_no}",
                    "last_modified": today.replace(tzinfo=None).isoformat(),
                }
            )
            filestore = resource_no % 2 == 0
            if filestore:
                resource.set_format("csv")
                resource.set_file_to_upload(testdata)
            else:
                wks, url = create_gsheet(
                    "resource1",
                    [[random.random() for i in range(10)] for j in range(10)],
                )
                resource.set_format("xlsx")
                resource["url"] = url

            resource_no += 1
            dataset.add_update_resource(resource)

        # add resources
        for i in range(10):
            create_resource()

        dataset.create_in_hdx(
            hxl_update=False, updated_by_script="hdx_python_api_ignore"
        )

        # check created dataset
        dataset = Dataset.read_from_hdx(name)
        assert dataset["name"] == name
        assert dataset["title"] == title
        assert dataset.get_tags() == tags
        assert dataset.get_maintainer()["id"] == maintainer_id
        assert dataset.get_organization()["display_name"] == "INNAGO (inactive)"
        resources = dataset.get_resources()
        for i, resource in enumerate(resources):
            assert resource["name"] == f"test_resource_{i}"
            if i % 2 == 0:
                assert resource.get_format() == "csv"
                assert resource["url_type"] == "upload"
                assert "humdata" in resource["url"]
            else:
                assert resource.get_format() == "xlsx"
                assert resource["url_type"] == "api"
                assert "humdata" not in resource["url"]

        # modify dataset
        dataset_id = dataset["id"]
        title = "HDX Python API test changed"
        notes = "added some notes"
        caveats = "added some caveats"
        # starting from a newly created Dataset object
        dataset = Dataset(
            {
                "id": dataset_id,
                "title": title,
                "notes": notes,
                "caveats": caveats,
            }
        )
        tags.remove("displacement")
        dataset.add_tags(tags)
        countryiso3s.remove("YEM")
        dataset.add_country_locations(countryiso3s)
        resources.pop(3)
        resources.pop()
        gsheet_resource = resources[5]
        gsheet_resource.set_format("csv")
        gsheet_resource.set_file_to_upload(testdata)
        for resource in resources:
            del resource["package_id"]
        dataset.add_update_resources(resources)

        sleep(2)
        dataset.update_in_hdx(hxl_update=False, remove_additional_resources=True)
        sleep(2)

        # check updated dataset
        dataset = Dataset.read_from_hdx(name)
        assert dataset["name"] == name
        assert dataset["title"] == title
        assert dataset["notes"] == notes
        assert dataset["caveats"] == caveats
        assert dataset.get_tags() == tags
        assert dataset.get_maintainer()["id"] == maintainer_id
        assert dataset.get_organization()["display_name"] == "INNAGO (inactive)"
        updated_resources = dataset.get_resources()
        for i, updated_resource in enumerate(updated_resources):
            resource = resources[i]
            assert updated_resource["name"] == resource["name"]
            assert updated_resource.get_format() == resource.get_format()
            assert updated_resource["url_type"].lower() == resource["url_type"]
            url = resource.get("url")
            if url:
                if "humdata" in url:
                    assert "humdata" in updated_resource["url"]
                else:
                    assert "humdata" not in updated_resource["url"]
            else:
                assert "humdata" in updated_resource["url"]

        # modify dataset again starting with existing dataset
        title = "HDX Python API test changed again"
        dataset["title"] = title
        del dataset["caveats"]
        tags = ["agriculture-livestock", "climate-weather", "hxl"]
        dataset["tags"] = []
        dataset.add_tags(tags)
        countryiso3s.append("YEM")
        dataset.add_country_location("YEM")
        dataset.delete_resource(updated_resources[5])
        updated_resources[0].set_file_to_upload(testdata)
        create_resource()
        resources = dataset.get_resources()

        sleep(2)
        dataset.create_in_hdx(
            hxl_update=False,
            remove_additional_resources=True,
            keys_to_delete=("caveats",),
        )
        sleep(2)

        # check dataset updated for second time
        dataset = Dataset.read_from_hdx(name)
        assert dataset["name"] == name
        assert dataset["title"] == title
        assert "caveats" not in dataset
        assert dataset.get_tags() == tags
        assert dataset.get_maintainer()["id"] == maintainer_id
        assert dataset.get_organization()["display_name"] == "INNAGO (inactive)"
        updated_resources = dataset.get_resources()
        for i, updated_resource in enumerate(updated_resources):
            resource = resources[i]
            assert updated_resource["name"] == resource["name"]
            assert updated_resource.get_format() == resource.get_format()
            assert updated_resource["url_type"].lower() == resource["url_type"]
            url = resource.get("url")
            if url:
                if "humdata" in url:
                    assert "humdata" in updated_resource["url"]
                else:
                    assert "humdata" not in updated_resource["url"]
            else:
                assert "humdata" in updated_resource["url"]

        # tear down
        dataset.delete_from_hdx()
