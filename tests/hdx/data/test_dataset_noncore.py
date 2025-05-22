"""Dataset Tests (noncore methods)"""

import copy
import json
from datetime import datetime, timezone
from os.path import join

import pytest

from .. import (
    MockResponse,
    dataset_data,
    dataset_mockshow,
    dataset_resultdict,
    organization_data,
    resources_data,
    resultgroups,
    resulttags,
    user_data,
)
from .test_organization import organization_mockshow
from .test_resource_view import (
    resource_view_list,
    resource_view_mockcreate,
    resource_view_mocklist,
    resource_view_mockshow,
)
from .test_showcase import showcase_resultdict
from .test_user import user_mockshow
from .test_vocabulary import vocabulary_mockshow
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.path import temp_dir
from hdx.utilities.saver import save_text


class TestDatasetNoncore:
    association = None

    @pytest.fixture(scope="class")
    def static_resource_view_yaml(self):
        return join("tests", "fixtures", "config", "hdx_resource_view_static.yaml")

    @pytest.fixture(scope="function")
    def vocabulary_read(self):
        Vocabulary._approved_vocabulary = None
        Vocabulary._tags_dict = None

        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return vocabulary_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def user_read(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return user_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def organization_read(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                return organization_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def showcase_read(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode("utf-8"))
                if "showcase_list" in url:
                    result = json.dumps([showcase_resultdict])
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_package_showcase_list"}'
                        % result,
                    )
                if "association_delete" in url:
                    TestDatasetNoncore.association = "delete"
                    return MockResponse(
                        200,
                        '{"success": true, "result": null, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_delete"}',
                    )
                elif "association_create" in url:
                    TestDatasetNoncore.association = "create"
                    result = json.dumps(datadict)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=ckanext_showcase_package_association_create"}'
                        % result,
                    )
                return dataset_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope="function")
    def vocabulary_update(self):
        class MockSession:
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                if isinstance(data, dict):
                    datadict = {
                        k.decode("utf8"): v.decode("utf8") for k, v in data.items()
                    }
                else:
                    datadict = json.loads(data.decode("utf-8"))
                if "default" in url:
                    result = json.dumps(resource_view_list)
                    return MockResponse(
                        200,
                        '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_create_default_resource_views"}'
                        % result,
                    )
                if "resource_view" in url:
                    if "show" in url:
                        return resource_view_mockshow(url, datadict)
                    if "list" in url:
                        return resource_view_mocklist(url, datadict)
                    if "create" in url:
                        if datadict["title"] == "Quick Charts":
                            return resource_view_mockcreate(url, datadict)
                    return MockResponse(
                        404,
                        '{"success": false, "error": {"message": "TEST ERROR: Not create", "__type": "TEST ERROR: Not Create Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_view_create"}',
                    )

        Configuration.read().remoteckan().session = MockSession()

    def test_get_name_or_id(self, configuration, hdx_config_yaml, project_config_yaml):
        dataset = Dataset()
        assert dataset.get_name_or_id() is None
        datasetdata = copy.deepcopy(dataset_resultdict)
        dataset = Dataset(datasetdata)
        assert dataset.get_name_or_id() == "MyDataset1"
        assert (
            dataset.get_name_or_id(prefer_name=False)
            == "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d"
        )
        del dataset["name"]
        assert dataset.get_name_or_id() == "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d"
        assert (
            dataset.get_name_or_id(prefer_name=False)
            == "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d"
        )
        datasetdata = copy.deepcopy(dataset_resultdict)
        dataset = Dataset(datasetdata)
        del dataset["id"]
        assert dataset.get_name_or_id() == "MyDataset1"
        assert dataset.get_name_or_id(prefer_name=False) == "MyDataset1"

    def test_get_hdx_url(self, configuration, hdx_config_yaml, project_config_yaml):
        dataset = Dataset()
        assert dataset.get_hdx_url() is None
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset.get_hdx_url() == "https://data.humdata.org/dataset/MyDataset1"
        Configuration.delete()
        Configuration._create(
            hdx_site="feature",
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        dataset = Dataset(datasetdata)
        assert (
            dataset.get_hdx_url()
            == "https://feature.data-humdata-org.ahconu.org/dataset/MyDataset1"
        )

    def test_get_api_url(self, configuration, hdx_config_yaml, project_config_yaml):
        dataset = Dataset()
        assert dataset.get_api_url() is None
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert (
            dataset.get_api_url()
            == "https://data.humdata.org/api/3/action/package_show?id=MyDataset1"
        )
        Configuration.delete()
        Configuration._create(
            hdx_site="feature",
            user_agent="test",
            hdx_config_yaml=hdx_config_yaml,
            project_config_yaml=project_config_yaml,
        )
        dataset = Dataset(datasetdata)
        assert (
            dataset.get_api_url()
            == "https://feature.data-humdata-org.ahconu.org/api/3/action/package_show?id=MyDataset1"
        )

    def test_get_set_date_of_dataset(self):
        dataset = Dataset({"dataset_date": "[2020-01-07T00:00:00 TO *]"})
        result = dataset.get_time_period(today=datetime(2020, 11, 17))
        assert result == {
            "startdate": datetime(2020, 1, 7, 0, 0, tzinfo=timezone.utc),
            "enddate": datetime(2020, 11, 17, 23, 59, 59, tzinfo=timezone.utc),
            "startdate_str": "2020-01-07T00:00:00+00:00",
            "enddate_str": "2020-11-17T23:59:59+00:00",
            "ongoing": True,
        }
        dataset.set_time_period("2020-02-09")
        result = dataset.get_time_period("%d/%m/%Y")
        assert result == {
            "startdate": datetime(2020, 2, 9, 0, 0, tzinfo=timezone.utc),
            "enddate": datetime(2020, 2, 9, 23, 59, 59, tzinfo=timezone.utc),
            "startdate_str": "09/02/2020",
            "enddate_str": "09/02/2020",
            "ongoing": False,
        }
        dataset.set_time_period("2020-02-09", "2020-10-20")
        result = dataset.get_time_period("%d/%m/%Y")
        assert result == {
            "startdate": datetime(2020, 2, 9, 0, 0, tzinfo=timezone.utc),
            "enddate": datetime(2020, 10, 20, 23, 59, 59, tzinfo=timezone.utc),
            "startdate_str": "09/02/2020",
            "enddate_str": "20/10/2020",
            "ongoing": False,
        }
        dataset.set_time_period("2020-02-09", ongoing=True)
        result = dataset.get_time_period("%d/%m/%Y", today=datetime(2020, 3, 9, 0, 0))
        assert result == {
            "startdate": datetime(2020, 2, 9, 0, 0, tzinfo=timezone.utc),
            "enddate": datetime(2020, 3, 9, 23, 59, 59, tzinfo=timezone.utc),
            "startdate_str": "09/02/2020",
            "enddate_str": "09/03/2020",
            "ongoing": True,
        }

    def test_set_dataset_year_range(self, configuration):
        dataset = Dataset()
        retval = dataset.set_time_period_year_range(2001, 2015)
        assert retval == [2001, 2015]
        retval = dataset.set_time_period_year_range("2010", "2017")
        assert retval == [2010, 2017]
        retval = dataset.set_time_period_year_range("2013")
        assert retval == [2013]
        retval = dataset.set_time_period_year_range({2005, 2002, 2003})
        assert retval == [2002, 2003, 2005]
        retval = dataset.set_time_period_year_range([2005, 2002, 2003])
        assert retval == [2002, 2003, 2005]
        retval = dataset.set_time_period_year_range((2005, 2002, 2003))
        assert retval == [2002, 2003, 2005]

    def test_is_set_subnational(self):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset["subnational"] == "1"
        assert dataset.is_subnational() is True
        dataset.set_subnational(False)
        assert dataset["subnational"] == "0"
        assert dataset.is_subnational() is False
        dataset.set_subnational(True)
        assert dataset["subnational"] == "1"
        assert dataset.is_subnational() is True

    def test_get_add_location(self, locations):
        Country.countriesdata(use_live=False)
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset["groups"] == resultgroups
        assert dataset.get_location_names() == ["Algeria", "Zimbabwe"]
        dataset.add_country_location("sdn")
        expected = copy.deepcopy(resultgroups)
        expected.append({"name": "sdn"})
        assert dataset["groups"] == expected
        assert dataset.get_location_names() == ["Algeria", "Zimbabwe", "Sudan"]
        dataset.add_country_location("dza")
        assert dataset["groups"] == expected
        assert dataset.get_location_names() == ["Algeria", "Zimbabwe", "Sudan"]
        dataset.add_country_locations(["KEN", "Mozambique", "ken"])
        expected.extend([{"name": "ken"}, {"name": "moz"}])
        assert dataset["groups"] == expected
        assert dataset.get_location_names() == [
            "Algeria",
            "Zimbabwe",
            "Sudan",
            "Kenya",
            "Mozambique",
        ]
        assert dataset.get_location_iso3s() == [
            "DZA",
            "ZWE",
            "SDN",
            "KEN",
            "MOZ",
        ]
        dataset.remove_location("sdn")
        assert dataset.get_location_names() == [
            "Algeria",
            "Zimbabwe",
            "Kenya",
            "Mozambique",
        ]
        with pytest.raises(HDXError):
            dataset.add_region_location("NOTEXIST")
        dataset.add_region_location("Africa")
        assert len(dataset["groups"]) == 60
        assert len(dataset.get_location_names()) == 60
        del dataset["groups"]
        assert dataset.get_location_names() == []
        assert dataset.get_location_iso3s() == []
        with pytest.raises(HDXError):
            dataset.add_country_location("abc")
        with pytest.raises(HDXError):
            dataset.add_country_location("lala")
        dataset.add_country_location("Ukrai", exact=False)
        assert dataset["groups"] == [{"name": "ukr"}]
        assert dataset.get_location_names() == ["Ukraine"]
        dataset.add_country_location("ukr")
        dataset.add_other_location("nepal-earthquake")
        assert dataset["groups"] == [
            {"name": "ukr"},
            {"name": "nepal-earthquake"},
        ]
        assert dataset.get_location_names() == ["Ukraine", "Nepal Earthquake"]
        del dataset["groups"]
        dataset.add_other_location("Nepal E", exact=False)
        assert dataset["groups"] == [{"name": "nepal-earthquake"}]
        dataset.add_other_location("Nepal Earthquake")
        assert dataset["groups"] == [{"name": "nepal-earthquake"}]
        with pytest.raises(HDXError):
            dataset.add_other_location("lala")
        with pytest.raises(HDXError):
            dataset.add_other_location("lala", alterror="nana")
        dataset["groups"] = [{"name": "ken"}, {"name": "MOZ"}, {"name": "dza"}]
        dataset.remove_location("moz")
        assert dataset["groups"] == [{"name": "ken"}, {"name": "dza"}]
        dataset.remove_location("KEN")
        assert dataset["groups"] == [{"name": "dza"}]

    def test_transform_update_frequency(self):
        assert len(Dataset.list_valid_update_frequencies()) == 49
        assert Dataset.transform_update_frequency("-2") == "As needed"
        assert Dataset.transform_update_frequency("-1") == "Never"
        assert Dataset.transform_update_frequency("0") == "Live"
        assert Dataset.transform_update_frequency("1") == "Every day"
        assert Dataset.transform_update_frequency("2") == "Every two days"
        assert Dataset.transform_update_frequency("Adhoc") == "-2"
        assert Dataset.transform_update_frequency("As needed") == "-2"
        assert Dataset.transform_update_frequency("Never") == "-1"
        assert Dataset.transform_update_frequency("Live") == "0"
        assert Dataset.transform_update_frequency("Every day") == "1"
        assert Dataset.transform_update_frequency("EVERY WEEK") == "7"
        assert Dataset.transform_update_frequency("every month") == "30"
        assert Dataset.transform_update_frequency("LALA") is None
        assert Dataset.transform_update_frequency(-2) == "As needed"
        assert Dataset.transform_update_frequency(7) == "Every week"
        assert Dataset.transform_update_frequency("") is None
        assert Dataset.transform_update_frequency(23) is None
        assert Dataset.transform_update_frequency("15") is None
        assert Dataset.transform_update_frequency("Quarterly") == "90"
        assert Dataset.transform_update_frequency("every 4 months") == "120"
        assert Dataset.transform_update_frequency("every two years") == "730"

    def test_get_set_expected_update_frequency(self, configuration):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset["data_update_frequency"] == "7"
        assert dataset.get_expected_update_frequency() == "Every week"
        dataset.set_expected_update_frequency("every two weeks")
        assert dataset["data_update_frequency"] == "14"
        dataset.set_expected_update_frequency(30)
        assert dataset["data_update_frequency"] == "30"
        dataset.set_expected_update_frequency("Fortnightly")
        assert dataset["data_update_frequency"] == "14"
        assert dataset.get_expected_update_frequency() == "Every two weeks"
        dataset.set_expected_update_frequency("EVERY SIX MONTHS")
        assert dataset["data_update_frequency"] == "180"
        assert dataset.get_expected_update_frequency() == "Every six months"
        dataset.set_expected_update_frequency("90")
        assert dataset["data_update_frequency"] == "90"
        assert dataset.get_expected_update_frequency() == "Every three months"
        assert dataset["data_update_frequency"] == "90"
        dataset.set_expected_update_frequency(60)
        assert dataset.get_expected_update_frequency() == "Every two months"
        dataset.set_expected_update_frequency("every 10 months")
        assert dataset["data_update_frequency"] == "300"
        with pytest.raises(HDXError):
            dataset.set_expected_update_frequency("lalala")
        with pytest.raises(HDXError):
            dataset.set_expected_update_frequency(9)
        del dataset["data_update_frequency"]
        assert dataset.get_expected_update_frequency() is None

    def test_get_add_tags(self, configuration, vocabulary_read):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset["tags"] == resulttags
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
        ]
        dataset.add_tag("LALA")
        assert dataset["tags"] == resulttags
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
        ]
        dataset.add_tag("conflict")
        expected = copy.deepcopy(resulttags)
        expected.append(
            {
                "name": "conflict-violence",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            }
        )
        assert dataset["tags"] == expected
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
            "conflict-violence",
        ]
        dataset.add_tags(
            [
                "desempleo",
                "desocupación",
                "desempleo",
                "conflict-related deaths",
            ]
        )
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
            "conflict-violence",
            "employment",
            "fatalities",
        ]
        dataset.remove_tag("conflict-violence")
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
            "employment",
            "fatalities",
        ]
        del dataset["tags"]
        assert dataset.get_tags() == []
        dataset.add_tag("conflict-related deaths")
        assert dataset["tags"] == [
            {
                "name": "conflict-violence",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "fatalities",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ]
        assert dataset.get_tags() == [
            "conflict-violence",
            "fatalities",
        ]
        dataset.add_tag("conflict-related deaths")
        assert dataset.get_tags() == [
            "conflict-violence",
            "fatalities",
        ]
        dataset.add_tag("cholera")
        assert dataset.get_tags() == [
            "conflict-violence",
            "fatalities",
            "disease",
        ]
        dataset.remove_tag("conflict-violence")
        assert dataset.get_tags() == [
            "fatalities",
            "disease",
        ]
        dataset.add_tag("cholera")
        assert dataset.get_tags() == [
            "fatalities",
            "disease",
        ]
        dataset.add_tag("cbi")
        assert dataset.get_tags() == [
            "fatalities",
            "disease",
            "cash based interventions-cbi",
        ]
        dataset.add_tag("lala")
        assert dataset.get_tags() == [
            "fatalities",
            "disease",
            "cash based interventions-cbi",
        ]

    def test_add_clean_tags(self, configuration, vocabulary_read):
        Vocabulary.set_tagsdict(None)
        Vocabulary.read_tags_mappings(failchained=False)
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        assert dataset.get_tags() == [
            "conflict",
            "political violence",
            "crisis-somewhere",
        ]
        assert dataset.clean_tags() == (
            ["conflict-violence", "crisis-somewhere"],
            ["political violence"],
        )
        dataset.add_tags(["nodeid123", "transportation"])
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
        ]
        dataset["tags"].append(
            {
                "name": "nodeid123",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            }
        )
        assert dataset.clean_tags() == (
            ["conflict-violence", "crisis-somewhere", "transportation"],
            ["nodeid123"],
        )
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
        ]
        dataset.add_tags(["geodata", "points"])
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
                "geodata",
            ],
            [],
        )
        dataset.add_tag("financial")
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
            "geodata",
        ]
        dataset["tags"].append(
            {
                "name": "financial",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            }
        )
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
                "geodata",
            ],
            ["financial"],
        )
        dataset.add_tag("addresses")
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
                "geodata",
            ],
            [],
        )
        dataset.remove_tag("geodata")
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
        ]
        dataset.add_tag("cultivos coca")
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
                "livelihoods",
            ],
            [],
        )
        dataset.remove_tag("livelihoods")
        dataset.add_tag("atentados")
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
        ]
        dataset["tags"].append(
            {
                "name": "atentados",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            }
        )
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
            ],
            [],
        )
        dataset.add_tag("windspeeds")
        assert dataset.clean_tags() == (
            [
                "conflict-violence",
                "crisis-somewhere",
                "transportation",
                "cyclones-hurricanes-typhoons",
            ],
            [],
        )
        print(dataset.get_tags())
        dataset.add_tag("conservancies")
        print(dataset.get_tags())
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "transportation",
            "cyclones-hurricanes-typhoons",
            "affected area",
        ]
        dataset.remove_tag("transportation")
        dataset.remove_tag("affected area")
        assert dataset.get_tags() == [
            "conflict-violence",
            "crisis-somewhere",
            "cyclones-hurricanes-typhoons",
        ]

    def test_maintainer(self, configuration, user_read):
        dataset = Dataset(dataset_data)
        dataset.set_maintainer("9f3e9973-7dbe-4c65-8820-f48578e3ffea")
        maintainer = dataset.get_maintainer()
        assert maintainer["name"] == "MyUser1"
        user = User(user_data)
        dataset.set_maintainer(user)
        maintainer = dataset.get_maintainer()
        assert maintainer["name"] == "MyUser1"
        with pytest.raises(HDXError):
            dataset.set_maintainer("jpsmith")
        with pytest.raises(HDXError):
            dataset.set_maintainer(123)

    def test_organization(self, configuration, organization_read):
        dataset = Dataset(dataset_data)
        dataset.set_organization("b67e6c74-c185-4f43-b561-0e114a736f19")
        organization = dataset.get_organization()
        assert organization["name"] == "acled"
        organization = Organization(organization_data)
        organization["name"] = "TEST1"
        dataset.set_organization(organization)
        organization = dataset.get_organization()
        assert organization["name"] == "acled"
        with pytest.raises(HDXError):
            dataset.set_organization("123")
        with pytest.raises(HDXError):
            dataset.set_organization(123)

    def test_add_update_delete_showcase(self, configuration, showcase_read):
        datasetdata = copy.deepcopy(dataset_data)
        dataset = Dataset(datasetdata)
        dataset["id"] = "dataset123"
        showcases = dataset.get_showcases()
        assert len(showcases) == 1
        TestDatasetNoncore.association = None
        showcases[0]["id"] = "05e392bf-04e0-4ca6-848c-4e87bba10746"
        dataset.remove_showcase(showcases[0])
        assert TestDatasetNoncore.association == "delete"
        TestDatasetNoncore.association = None
        assert dataset.add_showcase("15e392bf-04e0-4ca6-848c-4e87bba10745") is True
        assert TestDatasetNoncore.association == "create"
        TestDatasetNoncore.association = None
        dataset.add_showcases([{"id": "15e392bf-04e0-4ca6-848c-4e87bba10745"}])
        assert TestDatasetNoncore.association == "create"
        TestDatasetNoncore.association = None
        assert (
            dataset.add_showcases(
                [
                    {"id": "15e392bf-04e0-4ca6-848c-4e87bba10745"},
                    {"id": "05e392bf-04e0-4ca6-848c-4e87bba10746"},
                ]
            )
            is False
        )
        assert TestDatasetNoncore.association == "create"
        TestDatasetNoncore.association = None
        assert dataset.add_showcase({"name": "TEST1"}) is True
        assert TestDatasetNoncore.association == "create"
        TestDatasetNoncore.association = None
        with pytest.raises(HDXError):
            dataset.add_showcase("123")
        with pytest.raises(HDXError):
            dataset.add_showcase(123)

    def test_set_quickchart_resource(self, configuration):
        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        datasetdata["resources"] = resourcesdata
        dataset = Dataset(datasetdata)
        assert "dataset_preview" not in dataset
        assert (
            dataset.set_quickchart_resource("3d777226-96aa-4239-860a-703389d16d1f")[
                "id"
            ]
            == "3d777226-96aa-4239-860a-703389d16d1f"
        )
        assert dataset["dataset_preview"] == "resource_id"
        resources = dataset.get_resources()
        assert resources[0]["dataset_preview_enabled"] == "False"
        assert resources[1]["dataset_preview_enabled"] == "True"
        assert (
            dataset.set_quickchart_resource(resources[0])["id"]
            == "de6549d8-268b-4dfe-adaf-a4ae5c8510d5"
        )
        assert resources[0]["dataset_preview_enabled"] == "True"
        assert resources[1]["dataset_preview_enabled"] == "False"
        assert (
            dataset.set_quickchart_resource(resources[1].data)["id"]
            == "3d777226-96aa-4239-860a-703389d16d1f"
        )
        assert resources[0]["dataset_preview_enabled"] == "False"
        assert resources[1]["dataset_preview_enabled"] == "True"
        assert (
            dataset.set_quickchart_resource(0)["id"]
            == "de6549d8-268b-4dfe-adaf-a4ae5c8510d5"
        )
        assert resources[0]["dataset_preview_enabled"] == "True"
        assert resources[1]["dataset_preview_enabled"] == "False"
        assert dataset.set_quickchart_resource("12345") is None
        with pytest.raises(HDXError):
            dataset.set_quickchart_resource(True)
        dataset.preview_off()
        assert dataset["dataset_preview"] == "no_preview"
        assert resources[0]["dataset_preview_enabled"] == "False"
        assert resources[1]["dataset_preview_enabled"] == "False"
        assert (
            dataset.set_quickchart_resource("Resource2")["id"]
            == "3d777226-96aa-4239-860a-703389d16d1f"
        )
        assert dataset["dataset_preview"] == "resource_id"
        assert resources[0]["dataset_preview_enabled"] == "False"
        assert resources[1]["dataset_preview_enabled"] == "True"
        assert (
            dataset.set_quickchart_resource({"name": "Resource1"})["id"]
            == "de6549d8-268b-4dfe-adaf-a4ae5c8510d5"
        )
        assert dataset["dataset_preview"] == "resource_id"
        assert resources[0]["dataset_preview_enabled"] == "True"
        assert resources[1]["dataset_preview_enabled"] == "False"

    def test_quickcharts_resource_last(self):
        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        datasetdata["resources"] = resourcesdata
        dataset = Dataset(datasetdata)
        assert dataset.quickcharts_resource_last() is False
        resource = {"name": "QuickCharts-resource"}
        dataset.resources.insert(1, resource)
        assert dataset.quickcharts_resource_last() is True
        assert dataset.resources[3]["name"] == resource["name"]
        assert dataset.quickcharts_resource_last() is True

    def test_generate_resource_view(
        self, configuration, vocabulary_update, static_resource_view_yaml
    ):
        datasetdata = copy.deepcopy(dataset_data)
        resourcesdata = copy.deepcopy(resources_data)
        datasetdata["resources"] = resourcesdata
        dataset = Dataset(datasetdata)
        assert "dataset_preview" not in dataset
        resourceview = dataset.generate_quickcharts(path=static_resource_view_yaml)
        hxl_preview_config = json.loads(resourceview["hxl_preview_config"])
        assert resourceview["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert hxl_preview_config["bites"][0]["title"] == "Sum of fatalities"
        assert (
            hxl_preview_config["bites"][1]["title"]
            == "Sum of fatalities grouped by admin1"
        )
        assert (
            hxl_preview_config["bites"][2]["title"]
            == "Sum of fatalities grouped by admin2"
        )
        resourceview = dataset.generate_quickcharts(
            path=static_resource_view_yaml, bites_disabled=[False, True, False]
        )
        hxl_preview_config = json.loads(resourceview["hxl_preview_config"])
        assert resourceview["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert hxl_preview_config["bites"][0]["title"] == "Sum of fatalities"
        assert (
            hxl_preview_config["bites"][1]["title"]
            == "Sum of fatalities grouped by admin2"
        )
        resourceview = dataset.generate_quickcharts(
            path=static_resource_view_yaml, bites_disabled=[True, True, True]
        )
        assert resourceview is None
        indicators = [
            {
                "code": "1",
                "title": "My1",
                "unit": "ones",
                "description": "This is my one!",
            },
            {
                "code": "2",
                "title": "My2",
                "unit": "twos",
                "aggregate_col": "Agg2",
            },
            {
                "code": "3",
                "title": "My3",
                "description": "This is my three!",
                "date_col": "dt3",
                "date_format": "%b %Y",
            },
        ]
        resourceview = dataset.generate_quickcharts(indicators=indicators)
        hxl_preview_config = json.loads(resourceview["hxl_preview_config"])
        assert resourceview["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert (
            hxl_preview_config["bites"][0]["ingredient"]["filters"]["filterWith"][0][
                "#indicator+code"
            ]
            == "1"
        )
        assert (
            hxl_preview_config["bites"][0]["ingredient"]["description"]
            == "This is my one!"
        )
        assert hxl_preview_config["bites"][0]["uiProperties"]["title"] == "My1"
        assert (
            hxl_preview_config["bites"][0]["computedProperties"]["dataTitle"] == "ones"
        )
        assert (
            hxl_preview_config["bites"][1]["ingredient"]["filters"]["filterWith"][0][
                "#indicator+code"
            ]
            == "2"
        )
        assert hxl_preview_config["bites"][1]["ingredient"]["description"] == ""
        assert hxl_preview_config["bites"][1]["uiProperties"]["title"] == "My2"
        assert (
            hxl_preview_config["bites"][1]["computedProperties"]["dataTitle"] == "twos"
        )
        assert hxl_preview_config["bites"][1]["ingredient"]["aggregateColumn"] == "Agg2"
        assert (
            hxl_preview_config["bites"][2]["ingredient"]["filters"]["filterWith"][0][
                "#indicator+code"
            ]
            == "3"
        )
        assert (
            hxl_preview_config["bites"][2]["ingredient"]["description"]
            == "This is my three!"
        )
        assert hxl_preview_config["bites"][2]["ingredient"]["dateColumn"] == "dt3"
        assert hxl_preview_config["bites"][2]["uiProperties"]["title"] == "My3"
        assert hxl_preview_config["bites"][2]["computedProperties"]["dataTitle"] == ""
        assert hxl_preview_config["bites"][2]["uiProperties"]["dateFormat"] == "%b %Y"
        resourceview = dataset.generate_quickcharts(
            indicators=indicators,
            findreplace={
                "#indicator+code": "#item+code",
                "#indicator+value+num": "#value",
            },
        )
        hxl_preview_config = json.loads(resourceview["hxl_preview_config"])
        assert resourceview["id"] == "c06b5a0d-1d41-4a74-a196-41c251c76023"
        assert (
            hxl_preview_config["bites"][0]["ingredient"]["filters"]["filterWith"][0][
                "#item+code"
            ]
            == "1"
        )
        assert hxl_preview_config["bites"][0]["ingredient"]["valueColumn"] == "#value"
        assert dataset.generate_quickcharts(indicators=[]) is None
        assert dataset.generate_quickcharts(indicators=[None, None, None]) is None
        assert (
            dataset.generate_quickcharts(resource="123", path=static_resource_view_yaml)
            is None
        )
        del dataset.get_resources()[0]["id"]
        resourceview = dataset.generate_quickcharts(path=static_resource_view_yaml)
        assert "id" not in resourceview
        assert "resource_id" not in resourceview
        assert resourceview["resource_name"] == "Resource1"
        with pytest.raises(IOError):
            dataset.generate_quickcharts()

    def test_remove_dates_from_title(self):
        dataset = Dataset()
        with pytest.raises(HDXError):
            dataset.remove_dates_from_title()
        assert "title" not in dataset
        title = "Title with no dates"
        dataset["title"] = title
        assert dataset.remove_dates_from_title() == []
        assert dataset["title"] == title
        assert "dataset_date" not in dataset
        assert dataset.remove_dates_from_title(set_time_period=True) == []
        title = "ICA Armenia, 2017 - Drought Risk, 1981-2015"
        dataset["title"] = title
        expected = [
            (
                datetime(1981, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2015, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            ),
            (
                datetime(2017, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2017, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            ),
        ]
        assert dataset.remove_dates_from_title(change_title=False) == expected
        assert dataset["title"] == title
        assert "dataset_date" not in dataset
        assert dataset.remove_dates_from_title() == expected
        newtitle = "ICA Armenia - Drought Risk"
        assert dataset["title"] == newtitle
        assert "dataset_date" not in dataset
        dataset["title"] = title
        assert dataset.remove_dates_from_title(set_time_period=True) == expected
        assert dataset["title"] == newtitle
        assert dataset["dataset_date"] == "[1981-01-01T00:00:00 TO 2015-12-31T23:59:59]"
        assert dataset.remove_dates_from_title() == []
        dataset["title"] = "Mon_State_Village_Tract_Boundaries 9999 2001"
        expected = [
            (
                datetime(2001, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2001, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            )
        ]
        assert dataset.remove_dates_from_title(set_time_period=True) == expected
        assert dataset["title"] == "Mon_State_Village_Tract_Boundaries 9999"
        assert dataset["dataset_date"] == "[2001-01-01T00:00:00 TO 2001-12-31T23:59:59]"
        dataset["title"] = "Mon_State_Village_Tract_Boundaries 2001 99"
        assert dataset.remove_dates_from_title(set_time_period=True) == expected
        assert dataset["title"] == "Mon_State_Village_Tract_Boundaries 99"
        assert dataset["dataset_date"] == "[2001-01-01T00:00:00 TO 2001-12-31T23:59:59]"
        dataset["title"] = "Mon_State_Village_Tract_Boundaries 9999 2001 99"
        assert dataset.remove_dates_from_title(set_time_period=True) == expected
        assert dataset["title"] == "Mon_State_Village_Tract_Boundaries 9999 99"
        assert dataset["dataset_date"] == "[2001-01-01T00:00:00 TO 2001-12-31T23:59:59]"

    def test_load_save_to_json(self, vocabulary_read):
        with temp_dir(
            "LoadSaveDatasetJSON",
            delete_on_success=True,
            delete_on_failure=False,
        ) as temp_folder:
            name = "mydatasetname"
            dataset = Dataset({"name": name, "title": "title", "notes": "description"})
            maintainer = "196196be-6037-4488-8b71-d786adf4c081"
            dataset.set_maintainer(maintainer)
            dataset.set_organization("fb7c2910-6080-4b66-8b4f-0be9b6dc4d8e")
            start_date = "2020-02-09"
            end_date = "2020-10-20"
            dataset.set_time_period(start_date, end_date)
            expected_update_frequency = "Every day"
            dataset.set_expected_update_frequency(expected_update_frequency)
            dataset.set_subnational(False)
            tags = ["hxl", "funding"]
            dataset.add_tags(tags)
            resource_name = "filename.csv"
            resourcedata = {
                "name": resource_name,
                "description": "resource description",
                "format": "csv",
                "url": "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/edit#gid=1088874596",
            }
            dataset.add_update_resource(resourcedata)
            path = join(temp_folder, "dataset.json")
            dataset.save_to_json(path, follow_urls=True)
            dataset = Dataset.load_from_json(path)
            assert dataset["name"] == name
            assert dataset["maintainer"] == maintainer
            dateinfo = dataset.get_time_period()
            assert dateinfo["startdate_str"][:10] == start_date
            assert dateinfo["enddate_str"][:10] == end_date
            assert dataset.get_expected_update_frequency() == expected_update_frequency
            assert dataset.get_tags() == tags
            resource = dataset.get_resource()
            assert resource["name"] == resource_name
            assert (
                resource["url"]
                == "https://docs.google.com/spreadsheets/d/1NjSI2LaS3SqbgYc0HdD8oIb7lofGtiHgoKKATCpwVdY/export?format=csv&gid=1088874596"
            )

            save_text("null", path)
            dataset = Dataset.load_from_json(path)
            assert dataset is None
