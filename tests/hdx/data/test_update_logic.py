from os.path import join

import pytest
from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.loader import load_yaml
from hdx.utilities.matching import multiple_replace


class TestUpdateLogic:
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
                {"name": "health"},
                {"name": "demographics"},
                {"name": "sustainable development goals - sdg"},
            ],
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }

    @pytest.fixture(scope="class")
    def fixture_path(self):
        return join("tests", "fixtures", "update_logic")

    @pytest.fixture(scope="class")
    def new_resources_yaml(self, fixture_path):
        return join(fixture_path, "update_logic_resources_new.yaml")

    @pytest.fixture(scope="class")
    def resources_yaml(self, fixture_path):
        return join(fixture_path, "update_logic_resources.yaml")

    @pytest.fixture(scope="class")
    def dataset_data(self):
        return {
            "name": "who-data-for-zambia",
            "title": "Zambia - Health Indicators",
            "private": False,
            "notes": "Contains data from World Health Organization...",
            "dataset_source": "World Health Organization",
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "methodology": "Registry",
        }

    @pytest.fixture(scope="function")
    def new_dataset(self, dataset_data):
        new_dataset = Dataset(dataset_data)
        new_dataset.set_maintainer("35f7bb2c-4ab6-4796-8334-525b30a94c89")
        new_dataset.set_organization("c021f6be-3598-418e-8f7f-c7a799194dba")
        new_dataset.set_expected_update_frequency("Every month")
        new_dataset.set_subnational(False)
        new_dataset.set_time_period_year_range(1961, 2019)
        new_dataset.add_country_location("zmb")
        new_dataset.add_tag("hxl")
        return new_dataset

    @pytest.fixture(scope="function")
    def dataset(self, dataset_data, resources_yaml):
        dataset = Dataset(dataset_data)
        dataset.set_maintainer("35f7bb2c-4ab6-4796-8334-525b30a94c89")
        dataset.set_organization("c021f6be-3598-418e-8f7f-c7a799194dba")
        dataset.set_expected_update_frequency("Every month")
        dataset.set_subnational(False)
        dataset.set_time_period_year_range(1961, 2019)
        dataset.add_country_location("zmb")
        dataset.add_tag("hxl")
        dataset["id"] = "3adc4bb0-faef-42ae-bd67-0ea08918a629"
        return dataset

    @staticmethod
    def add_dataset_resources(dataset, yaml_path, include=None, exclude=None):
        resources = load_yaml(yaml_path)
        if include is None:
            include = range(len(resources))
        if exclude is None:
            exclude = []
        for i, resource in enumerate(resources):
            if i not in include:
                continue
            if i in exclude:
                continue
            dataset.add_update_resource(resource)

    @staticmethod
    def add_new_dataset_resources(new_dataset, yaml_path, include=None, exclude=None):
        resources_uploads = load_yaml(yaml_path)
        if include is None:
            include = range(len(resources_uploads))
        if exclude is None:
            exclude = []
        for i, resource_upload in enumerate(resources_uploads):
            if i not in include:
                continue
            if i in exclude:
                continue
            resource = resource_upload["resource"]
            file_to_upload = resource_upload["file_to_upload"]
            resource = Resource(resource)
            resource.set_file_to_upload(file_to_upload)
            new_dataset.add_update_resource(resource)

    @staticmethod
    def check_resources(update, results):
        resources = update["resources"]
        files_to_upload = results["files_to_upload"]
        for key in files_to_upload:
            index = key.replace("update__resources__", "")
            index = int(index.replace("__upload", ""))
            resource = resources[index]
            rn = resource["name"]
            name = slugify(rn, separator="_")
            name = multiple_replace(
                name,
                {
                    "all_health": "health",
                    "quickcharts": "qc_health",
                    "for_zambia": "ZMB.csv",
                },
            )
            name = f"/tmp/WHO/{name}"
            fu = files_to_upload[key]
            assert name == fu, (
                f'Mismatch at index {index}: "{rn}", file to upload is "{fu}"'
            )

    def test_update_logic_1(
        self,
        configuration,
        new_dataset,
        dataset,
        new_resources_yaml,
        resources_yaml,
    ):
        self.add_new_dataset_resources(new_dataset, new_resources_yaml, exclude=[13])
        self.add_dataset_resources(dataset, resources_yaml)
        dataset.old_data = new_dataset.data
        dataset.old_data["resources"] = new_dataset.resources
        results = dataset._dataset_hdx_update(
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert results["filter"] == []
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "name": "who-data-for-zambia",
            "title": "Zambia - Health Indicators",
            "private": False,
            "notes": "Contains data from World Health Organization...",
            "dataset_source": "World Health Organization",
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "methodology": "Registry",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "data_update_frequency": "30",
            "subnational": "0",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "groups": [{"name": "zmb"}],
            "tags": [
                {
                    "name": "hxl",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                }
            ],
            "resources": [
                {
                    "name": "All Health Indicators for Zambia",
                    "description": "See resource descriptions below for links to indicator metadata",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Mortality and global health estimates Indicators for Zambia",
                    "description": "*Mortality and global health estimates:*\n[Infant mortality rate (proba",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Sustainable development goals Indicators for Zambia",
                    "description": "*Sustainable development goals:*\n[Adolescent birth rate (per 1000 wome",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "description": "*Millennium Development Goals (MDGs):*\n[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health systems Indicators for Zambia",
                    "description": "*Health systems:*\n[Median availability of selected generic medicines (",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Malaria Indicators for Zambia",
                    "description": "*Malaria:*\n[Children aged <5 years sleeping under insecticide-treated ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Tuberculosis Indicators for Zambia",
                    "description": "*Tuberculosis:*\n[Deaths due to tuberculosis among HIV-negative people ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Child health Indicators for Zambia",
                    "description": "*Child health:*\n[Children aged <5 years stunted (%)](https://www.who.i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Infectious diseases Indicators for Zambia",
                    "description": "*Infectious diseases:*\n[Cholera - number of reported cases](https://ww",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "World Health Statistics Indicators for Zambia",
                    "description": "*World Health Statistics:*\n[Literacy rate among adults aged >= 15 year",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health financing Indicators for Zambia",
                    "description": "*Health financing:*\n[Private prepaid plans as a percentage of private ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Public health and environment Indicators for Zambia",
                    "description": "*Public health and environment:*\n[Population using solid fuels (%)](ht",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Substance use and mental health Indicators for Zambia",
                    "description": "*Substance use and mental health:*\n[Fines for violations](https://www.",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Injuries and violence Indicators for Zambia",
                    "description": "*Injuries and violence:*\n[Income level](https://www.who.int/data/gho/i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "HIV/AIDS and other STIs Indicators for Zambia",
                    "description": "*HIV/AIDS and other STIs:*\n[Prevalence of HIV among adults aged 15 to ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Nutrition Indicators for Zambia",
                    "description": "*Nutrition:*\n[Early initiation of breastfeeding (%)](https://www.who.i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Urban health Indicators for Zambia",
                    "description": "*Urban health:*\n[Percentage of the total population living in cities >",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases Indicators for Zambia",
                    "description": "*Noncommunicable diseases:*\n[Prevalence of overweight among adults, BM",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases CCS Indicators for Zambia",
                    "description": "*Noncommunicable diseases CCS:*\n[Existence of operational policy/strat",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Negelected tropical diseases Indicators for Zambia",
                    "description": "*Negelected tropical diseases:*\n[Number of new reported cases of Burul",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health Equity Monitor Indicators for Zambia",
                    "description": "*Health Equity Monitor:*\n[Antenatal care coverage - at least one visit",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Infrastructure Indicators for Zambia",
                    "description": "*Infrastructure:*\n[Total density per 100 000 population: Health posts]",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Essential health technologies Indicators for Zambia",
                    "description": "*Essential health technologies:*\n[Availability of national standards o",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Medical equipment Indicators for Zambia",
                    "description": "*Medical equipment:*\n[Total density per million population: Magnetic R",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Demographic and socioeconomic statistics Indicators for Zambia",
                    "description": "*Demographic and socioeconomic statistics:*\n[Cellular subscribers (per",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Neglected tropical diseases Indicators for Zambia",
                    "description": "*Neglected tropical diseases:*\n[Status of yaws endemicity](https://www",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "International Health Regulations (2005) monitoring framework Indicators for Zambia",
                    "description": "*International Health Regulations (2005) monitoring framework:*\n[Legis",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Insecticide resistance Indicators for Zambia",
                    "description": "*Insecticide resistance:*\n[Number of insecticide classes to which resi",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Universal Health Coverage Indicators for Zambia",
                    "description": "*Universal Health Coverage:*\n[Cataract surgical coverage of adults age",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Global Observatory for eHealth (GOe) Indicators for Zambia",
                    "description": "*Global Observatory for eHealth (GOe):*\n[National universal health cov",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION:*\n[Government uni",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT:*\n[Government unit/",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING:*\n[Five-year change",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVIDERS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVID",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREATMENT COVERAGE Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREAT",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT:*\n",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVENTIONS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVEN",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROVIDERS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROV",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVICES Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVI",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: HUMAN RESOURCES Indicators for Zambia",
                    "description": "*RSUD: HUMAN RESOURCES:*\n[Health professionals providing treatment for",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: INFORMATION SYSTEMS Indicators for Zambia",
                    "description": "*RSUD: INFORMATION SYSTEMS:*\n[Epidemiological data collection for subs",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: YOUTH Indicators for Zambia",
                    "description": "*RSUD: YOUTH:*\n[Epidemiological data collection system for substance u",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "FINANCIAL PROTECTION Indicators for Zambia",
                    "description": "*FINANCIAL PROTECTION:*\n[Population with household expenditures on hea",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases and mental health Indicators for Zambia",
                    "description": "*Noncommunicable diseases and mental health:*\n[Number of deaths attrib",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health workforce Indicators for Zambia",
                    "description": "*Health workforce:*\n[Medical doctors (per 10 000 population)](https://",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Neglected Tropical Diseases Indicators for Zambia",
                    "description": "*Neglected Tropical Diseases:*\n[Number of new leprosy cases](https://w",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "QuickCharts Indicators for Zambia",
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "TOBACCO Indicators for Zambia",
                    "description": "*TOBACCO:*\n[Monitor](https://www.who.int/data/gho/indicator-metadata-r",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "UHC Indicators for Zambia",
                    "description": "*UHC:*\n[Population in malaria-endemic areas who slept under an insecti",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "ICD Indicators for Zambia",
                    "description": "*ICD:*\n[ICD-11 implementation progress level](https://web-prod.who.int",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia",
                    "description": "*SEXUAL AND REPRODUCTIVE HEALTH:*\n[Institutional Births (birth taken p",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Immunization Indicators for Zambia",
                    "description": "*Immunization:*\n[Proportion of vaccination cards seen (%)](https://www",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "NLIS Indicators for Zambia",
                    "description": "*NLIS:*\n[Subclinical vitamin A deficiency in preschool-age children (s",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
            ],
            "state": "active",
        }
        self.check_resources(update, results)

    def test_update_logic_2(
        self,
        configuration,
        new_dataset,
        dataset,
        new_resources_yaml,
        resources_yaml,
    ):
        self.add_new_dataset_resources(new_dataset, new_resources_yaml, exclude=[52])
        self.add_dataset_resources(dataset, resources_yaml)
        dataset.old_data = new_dataset.data
        dataset.old_data["resources"] = new_dataset.resources
        results = dataset._dataset_hdx_update(
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert results["filter"] == ["-resources__53"]
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "name": "who-data-for-zambia",
            "title": "Zambia - Health Indicators",
            "private": False,
            "notes": "Contains data from World Health Organization...",
            "dataset_source": "World Health Organization",
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "methodology": "Registry",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "data_update_frequency": "30",
            "subnational": "0",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "groups": [{"name": "zmb"}],
            "tags": [
                {
                    "name": "hxl",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                }
            ],
            "resources": [
                {
                    "name": "All Health Indicators for Zambia",
                    "description": "See resource descriptions below for links to indicator metadata",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Mortality and global health estimates Indicators for Zambia",
                    "description": "*Mortality and global health estimates:*\n[Infant mortality rate (proba",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Sustainable development goals Indicators for Zambia",
                    "description": "*Sustainable development goals:*\n[Adolescent birth rate (per 1000 wome",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "description": "*Millennium Development Goals (MDGs):*\n[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health systems Indicators for Zambia",
                    "description": "*Health systems:*\n[Median availability of selected generic medicines (",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Malaria Indicators for Zambia",
                    "description": "*Malaria:*\n[Children aged <5 years sleeping under insecticide-treated ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Tuberculosis Indicators for Zambia",
                    "description": "*Tuberculosis:*\n[Deaths due to tuberculosis among HIV-negative people ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Child health Indicators for Zambia",
                    "description": "*Child health:*\n[Children aged <5 years stunted (%)](https://www.who.i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Infectious diseases Indicators for Zambia",
                    "description": "*Infectious diseases:*\n[Cholera - number of reported cases](https://ww",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "World Health Statistics Indicators for Zambia",
                    "description": "*World Health Statistics:*\n[Literacy rate among adults aged >= 15 year",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health financing Indicators for Zambia",
                    "description": "*Health financing:*\n[Private prepaid plans as a percentage of private ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Public health and environment Indicators for Zambia",
                    "description": "*Public health and environment:*\n[Population using solid fuels (%)](ht",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Substance use and mental health Indicators for Zambia",
                    "description": "*Substance use and mental health:*\n[Fines for violations](https://www.",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Tobacco Indicators for Zambia",
                    "description": "*Tobacco:*\n[Prevalence of smoking any tobacco product among persons ag",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Injuries and violence Indicators for Zambia",
                    "description": "*Injuries and violence:*\n[Income level](https://www.who.int/data/gho/i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "HIV/AIDS and other STIs Indicators for Zambia",
                    "description": "*HIV/AIDS and other STIs:*\n[Prevalence of HIV among adults aged 15 to ",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Nutrition Indicators for Zambia",
                    "description": "*Nutrition:*\n[Early initiation of breastfeeding (%)](https://www.who.i",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Urban health Indicators for Zambia",
                    "description": "*Urban health:*\n[Percentage of the total population living in cities >",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases Indicators for Zambia",
                    "description": "*Noncommunicable diseases:*\n[Prevalence of overweight among adults, BM",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases CCS Indicators for Zambia",
                    "description": "*Noncommunicable diseases CCS:*\n[Existence of operational policy/strat",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Negelected tropical diseases Indicators for Zambia",
                    "description": "*Negelected tropical diseases:*\n[Number of new reported cases of Burul",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health Equity Monitor Indicators for Zambia",
                    "description": "*Health Equity Monitor:*\n[Antenatal care coverage - at least one visit",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Infrastructure Indicators for Zambia",
                    "description": "*Infrastructure:*\n[Total density per 100 000 population: Health posts]",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Essential health technologies Indicators for Zambia",
                    "description": "*Essential health technologies:*\n[Availability of national standards o",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Medical equipment Indicators for Zambia",
                    "description": "*Medical equipment:*\n[Total density per million population: Magnetic R",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Demographic and socioeconomic statistics Indicators for Zambia",
                    "description": "*Demographic and socioeconomic statistics:*\n[Cellular subscribers (per",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Neglected tropical diseases Indicators for Zambia",
                    "description": "*Neglected tropical diseases:*\n[Status of yaws endemicity](https://www",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "International Health Regulations (2005) monitoring framework Indicators for Zambia",
                    "description": "*International Health Regulations (2005) monitoring framework:*\n[Legis",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Insecticide resistance Indicators for Zambia",
                    "description": "*Insecticide resistance:*\n[Number of insecticide classes to which resi",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Universal Health Coverage Indicators for Zambia",
                    "description": "*Universal Health Coverage:*\n[Cataract surgical coverage of adults age",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Global Observatory for eHealth (GOe) Indicators for Zambia",
                    "description": "*Global Observatory for eHealth (GOe):*\n[National universal health cov",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION:*\n[Government uni",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT:*\n[Government unit/",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING Indicators for Zambia",
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING:*\n[Five-year change",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVIDERS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVID",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREATMENT COVERAGE Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREAT",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT:*\n",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVENTIONS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVEN",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROVIDERS Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROV",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVICES Indicators for Zambia",
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVI",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: HUMAN RESOURCES Indicators for Zambia",
                    "description": "*RSUD: HUMAN RESOURCES:*\n[Health professionals providing treatment for",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: INFORMATION SYSTEMS Indicators for Zambia",
                    "description": "*RSUD: INFORMATION SYSTEMS:*\n[Epidemiological data collection for subs",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "RSUD: YOUTH Indicators for Zambia",
                    "description": "*RSUD: YOUTH:*\n[Epidemiological data collection system for substance u",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "FINANCIAL PROTECTION Indicators for Zambia",
                    "description": "*FINANCIAL PROTECTION:*\n[Population with household expenditures on hea",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Noncommunicable diseases and mental health Indicators for Zambia",
                    "description": "*Noncommunicable diseases and mental health:*\n[Number of deaths attrib",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Health workforce Indicators for Zambia",
                    "description": "*Health workforce:*\n[Medical doctors (per 10 000 population)](https://",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Neglected Tropical Diseases Indicators for Zambia",
                    "description": "*Neglected Tropical Diseases:*\n[Number of new leprosy cases](https://w",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "QuickCharts Indicators for Zambia",
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "TOBACCO Indicators for Zambia",
                    "description": "*TOBACCO:*\n[Monitor](https://www.who.int/data/gho/indicator-metadata-r",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "UHC Indicators for Zambia",
                    "description": "*UHC:*\n[Population in malaria-endemic areas who slept under an insecti",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "ICD Indicators for Zambia",
                    "description": "*ICD:*\n[ICD-11 implementation progress level](https://web-prod.who.int",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia",
                    "description": "*SEXUAL AND REPRODUCTIVE HEALTH:*\n[Institutional Births (birth taken p",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Immunization Indicators for Zambia",
                    "description": "*Immunization:*\n[Proportion of vaccination cards seen (%)](https://www",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
            ],
            "state": "active",
        }
        self.check_resources(update, results)

    def test_update_logic_3(
        self,
        configuration,
        new_dataset,
        dataset,
        new_resources_yaml,
        resources_yaml,
    ):
        self.add_new_dataset_resources(new_dataset, new_resources_yaml, include=[0, 3])
        self.add_dataset_resources(dataset, resources_yaml, include=[0, 1, 2])
        dataset.old_data = new_dataset.data
        dataset.old_data["resources"] = new_dataset.resources
        results = dataset._dataset_hdx_update(
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert results["filter"] == []
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "name": "who-data-for-zambia",
            "title": "Zambia - Health Indicators",
            "private": False,
            "notes": "Contains data from World Health Organization...",
            "dataset_source": "World Health Organization",
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "methodology": "Registry",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "data_update_frequency": "30",
            "subnational": "0",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "groups": [{"name": "zmb"}],
            "tags": [
                {
                    "name": "hxl",
                    "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                }
            ],
            "resources": [
                {
                    "name": "All Health Indicators for Zambia",
                    "description": "See resource descriptions below for links to indicator metadata",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
                {
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "description": "*Millennium Development Goals (MDGs):*\n[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "url_type": "upload",
                    "resource_type": "file.upload",
                    "url": "updated_by_file_upload_step",
                },
            ],
            "state": "active",
        }
        self.check_resources(update, results)
