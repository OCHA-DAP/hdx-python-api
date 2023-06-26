import json
from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country


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
        Vocabulary._tags_dict = dict()
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

    @pytest.fixture(scope="function")
    def dataset(self, dataset_json):
        return Dataset.load_from_json(dataset_json)

    @pytest.fixture(scope="function")
    def new_dataset(self, fixture_path, new_dataset_json):
        with open(new_dataset_json) as f:
            jsonobj = json.loads(f.read())
            resourceobjs = jsonobj["resources"]
            del jsonobj["resources"]
            dataset = Dataset(jsonobj)
            for resourceobj in resourceobjs:
                resource = Resource(resourceobj)
                filename = self.file_mapping[resourceobj["name"]]
                resource.set_file_to_upload(join(fixture_path, filename))
                dataset.add_update_resource(resource)
            return dataset

    def test_dataset_update_resources(
        self, configuration, dataset, new_dataset
    ):
        dataset.old_data = new_dataset.data
        dataset.old_data["resources"] = new_dataset._copy_hdxobjects(
            new_dataset.resources, Resource, "file_to_upload"
        )
        (
            resources_to_delete,
            new_resource_order,
            filestore_resources,
        ) = dataset._dataset_merge_update_resources(True, True, True, True)
        assert resources_to_delete == [8, 2, 1, 0]
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
        assert filestore_resources == {
            3: "tests/fixtures/update_dataset_resources/sdg_data_zwe.csv",
            4: "tests/fixtures/update_dataset_resources/sdg_indicatorlist_zwe.csv",
            5: "tests/fixtures/update_dataset_resources/sdg_metadata_zwe.csv",
            6: "tests/fixtures/update_dataset_resources/dem_data_zwe.csv",
            7: "tests/fixtures/update_dataset_resources/dem_indicatorlist_zwe.csv",
            9: "tests/fixtures/update_dataset_resources/opri_data_zwe.csv",
            10: "tests/fixtures/update_dataset_resources/opri_indicatorlist_zwe.csv",
            11: "tests/fixtures/update_dataset_resources/opri_metadata_zwe.csv",
            12: "tests/fixtures/update_dataset_resources/qc_sdg_data_zwe.csv",
        }
        results = dataset._save_dataset_add_filestore_resources(
            "update",
            "id",
            tuple(),
            resources_to_delete,
            new_resource_order,
            filestore_resources,
            hxl_update=False,
            create_default_views=False,
            test=True,
        )
        assert results["files_to_upload"] == {
            "update__resources__0__upload": "tests/fixtures/update_dataset_resources/sdg_data_zwe.csv",
            "update__resources__1__upload": "tests/fixtures/update_dataset_resources/sdg_indicatorlist_zwe.csv",
            "update__resources__2__upload": "tests/fixtures/update_dataset_resources/sdg_metadata_zwe.csv",
            "update__resources__3__upload": "tests/fixtures/update_dataset_resources/dem_data_zwe.csv",
            "update__resources__4__upload": "tests/fixtures/update_dataset_resources/dem_indicatorlist_zwe.csv",
            "update__resources__5__upload": "tests/fixtures/update_dataset_resources/opri_data_zwe.csv",
            "update__resources__6__upload": "tests/fixtures/update_dataset_resources/opri_indicatorlist_zwe.csv",
            "update__resources__7__upload": "tests/fixtures/update_dataset_resources/opri_metadata_zwe.csv",
            "update__resources__8__upload": "tests/fixtures/update_dataset_resources/qc_sdg_data_zwe.csv",
        }
        resources = results["update"]["resources"]
        cutdown_resources = list()
        for resource in resources:
            cutdown_resource = dict()
            for key, value in resource.items():
                if key in (
                    "dataset_preview_enabled",
                    "format",
                    "name",
                    "position",
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
                "position": 3,
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "SDG 4 Global and Thematic indicator list",
                "position": 4,
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "SDG 4 Global and Thematic metadata",
                "position": 5,
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Demographic and Socio-economic data",
                "position": 6,
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "format": "csv",
                "name": "Demographic and Socio-economic indicator list",
                "position": 7,
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
