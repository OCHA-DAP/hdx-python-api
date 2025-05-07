from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.loader import load_json


class TestUpdateDatasetResourcesLogic:
    file_mapping = {
        "SDG 4 Global and Thematic data": "sdg_data_zwe.csv",
        "SDG 4 Global and Thematic indicator list": "sdg_indicatorlist_zwe.csv",
        "SDG 4 Global and Thematic metadata": "sdg_metadata_zwe.csv",
        "Other Policy Relevant Indicators data": "opri_data_zwe.csv",
        "Other Policy Relevant Indicators indicator list": "opri_indicatorlist_zwe.csv",
        "Other Policy Relevant Indicators metadata": "opri_metadata_zwe.csv",
        "Demographic and Socio-economic data": "dem_data_zwe.csv",
        "Demographic and Socio-economic indicator list": "dem_indicatorlist_zwe.csv",
        "QuickCharts-SDG 4 Global and Thematic data": "qc_sdg_data_zwe.csv",
    }

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join(
                "tests", "fixtures", "config", "project_configuration.yaml"
            ),
        )
        Locations.set_validlocations([{"name": "zmb", "title": "Zambia"}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = {}
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": "hxl"},
                {"name": "indicators"},
                {"name": "socioeconomics"},
                {"name": "demographics"},
                {"name": "education"},
                {"name": "sustainable development"},
                {"name": "sustainable development goals-sdg"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }

    @pytest.fixture(scope="class")
    def fixture_path(self):
        return join("tests", "fixtures", "update_dataset_resources")

    @pytest.fixture(scope="class")
    def new_dataset_json(self, fixture_path):
        return join(fixture_path, "unesco_update_dataset.json")

    @pytest.fixture(scope="class")
    def dataset_json(self, fixture_path):
        return join(fixture_path, "unesco_dataset.json")

    @pytest.fixture(scope="class")
    def expected_resources_to_update_json(self, fixture_path):
        return join(fixture_path, "expected_resources_to_update.json")

    @pytest.fixture(scope="function")
    def dataset(self, dataset_json):
        return Dataset.load_from_json(dataset_json)

    @pytest.fixture(scope="function")
    def new_dataset(self, fixture_path, new_dataset_json):
        jsonobj = load_json(new_dataset_json)
        resourceobjs = jsonobj["resources"]
        del jsonobj["resources"]
        dataset = Dataset(jsonobj)
        for resourceobj in resourceobjs:
            resource = Resource(resourceobj)
            filename = self.file_mapping[resourceobj["name"]]
            resource.set_file_to_upload(join(fixture_path, filename))
            dataset.add_update_resource(resource)
        return dataset

    @pytest.fixture(scope="class")
    def expected_resources_to_update(self, expected_resources_to_update_json):
        return load_json(expected_resources_to_update_json)

    def test_dataset_update_resources(
        self,
        fixture_path,
        configuration,
        dataset,
        new_dataset,
        expected_resources_to_update,
    ):
        dataset.old_data = new_dataset.data
        dataset.old_data["resources"] = new_dataset._copy_hdxobjects(
            new_dataset.resources, Resource, ("file_to_upload", "data_updated")
        )
        (
            resources_to_update,
            resources_to_delete,
            filestore_resources,
            new_resource_order,
        ) = dataset._dataset_update_resources(True, True, True, True)
        assert resources_to_update == expected_resources_to_update
        assert resources_to_delete == [8, 2, 1, 0]
        assert filestore_resources == {
            3: join(fixture_path, "sdg_data_zwe.csv"),
            4: join(fixture_path, "sdg_indicatorlist_zwe.csv"),
            5: join(fixture_path, "sdg_metadata_zwe.csv"),
            6: join(fixture_path, "dem_data_zwe.csv"),
            7: join(fixture_path, "dem_indicatorlist_zwe.csv"),
            9: join(fixture_path, "opri_data_zwe.csv"),
            10: join(fixture_path, "opri_indicatorlist_zwe.csv"),
            11: join(fixture_path, "opri_metadata_zwe.csv"),
            12: join(fixture_path, "qc_sdg_data_zwe.csv"),
        }
        assert new_resource_order == [
            ("SDG 4 Global and Thematic data", "csv"),
            ("SDG 4 Global and Thematic indicator list", "csv"),
            ("SDG 4 Global and Thematic metadata", "csv"),
            ("Other Policy Relevant Indicators data", "csv"),
            ("Other Policy Relevant Indicators indicator list", "csv"),
            ("Other Policy Relevant Indicators metadata", "csv"),
            ("Demographic and Socio-economic data", "csv"),
            ("Demographic and Socio-economic indicator list", "csv"),
            ("QuickCharts-SDG 4 Global and Thematic data", "csv"),
        ]
        dataset._prepare_hdx_call(dataset.old_data, {})
        assert (
            dataset["updated_by_script"]
            == "HDX Scraper: UNESCO (2022-12-19T12:51:30.579185)"
        )
        results = dataset._revise_dataset(
            tuple(),
            resources_to_update,
            resources_to_delete,
            filestore_resources,
            new_resource_order,
            hxl_update=False,
            create_default_views=False,
            test=True,
        )
        assert results["files_to_upload"] == {
            "update__resources__0__upload": join(fixture_path, "sdg_data_zwe.csv"),
            "update__resources__1__upload": join(
                fixture_path, "sdg_indicatorlist_zwe.csv"
            ),
            "update__resources__2__upload": join(fixture_path, "sdg_metadata_zwe.csv"),
            "update__resources__3__upload": join(fixture_path, "dem_data_zwe.csv"),
            "update__resources__4__upload": join(
                fixture_path, "dem_indicatorlist_zwe.csv"
            ),
            "update__resources__5__upload": join(fixture_path, "opri_data_zwe.csv"),
            "update__resources__6__upload": join(
                fixture_path, "opri_indicatorlist_zwe.csv"
            ),
            "update__resources__7__upload": join(fixture_path, "opri_metadata_zwe.csv"),
            "update__resources__8__upload": join(fixture_path, "qc_sdg_data_zwe.csv"),
        }
        resources = results["update"]["resources"]
        cutdown_resources = []
        for resource in resources:
            cutdown_resource = {}
            for key, value in resource.items():
                if key in (
                    "dataset_preview_enabled",
                    "format",
                    "name",
                    "resource_type",
                    "url",
                    "url_type",
                ):
                    cutdown_resource[key] = value
            cutdown_resources.append(cutdown_resource)
        assert cutdown_resources == [
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "SDG 4 Global and Thematic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "SDG 4 Global and Thematic indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "SDG 4 Global and Thematic metadata",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Demographic and Socio-economic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Demographic and Socio-economic indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Other Policy Relevant Indicators data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Other Policy Relevant Indicators indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Other Policy Relevant Indicators metadata",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "True",
                "format": "csv",
                "name": "QuickCharts-SDG 4 Global and Thematic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
        ]
