import string
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
from hdx.utilities.path import get_temp_dir


class TestUpdateLogic:
    characters = string.ascii_letters + string.digits + string.punctuation

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
    def temp_dir(self):
        return get_temp_dir("WHO", delete_if_exists=True)

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

    @classmethod
    # Function to generate file with number
    def generate_content(cls, path, number):
        # Write the content to a file
        with open(path, "w") as file:
            file.write(f"{number:02d}")

    @classmethod
    def add_new_dataset_resources(
        cls, temp_dir, new_dataset, yaml_path, include=None, exclude=None
    ):
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
            resource = Resource(resource)
            file_to_upload = resource_upload["file_to_upload"]
            cls.generate_content(file_to_upload, i)
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
        temp_dir,
    ):
        self.add_new_dataset_resources(
            temp_dir, new_dataset, new_resources_yaml, exclude=[13]
        )
        self.add_dataset_resources(dataset, resources_yaml)
        dataset._old_data = new_dataset.data
        dataset._old_data["resources"] = new_dataset._resources
        statuses, results = dataset._dataset_hdx_update(
            allow_no_resources=False,
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert statuses == {
            "All Health Indicators for Zambia": 2,
            "Child health Indicators for Zambia": 2,
            "Demographic and socioeconomic statistics Indicators for Zambia": 2,
            "Essential health technologies Indicators for Zambia": 2,
            "FINANCIAL PROTECTION Indicators for Zambia": 2,
            "Global Observatory for eHealth (GOe) Indicators for Zambia": 2,
            "HIV/AIDS and other STIs Indicators for Zambia": 2,
            "Health Equity Monitor Indicators for Zambia": 2,
            "Health financing Indicators for Zambia": 2,
            "Health systems Indicators for Zambia": 2,
            "Health workforce Indicators for Zambia": 2,
            "ICD Indicators for Zambia": 2,
            "Immunization Indicators for Zambia": 2,
            "Infectious diseases Indicators for Zambia": 2,
            "Infrastructure Indicators for Zambia": 2,
            "Injuries and violence Indicators for Zambia": 2,
            "Insecticide resistance Indicators for Zambia": 2,
            "International Health Regulations (2005) monitoring framework Indicators for Zambia": 2,
            "Malaria Indicators for Zambia": 2,
            "Medical equipment Indicators for Zambia": 2,
            "Millennium Development Goals (MDGs) Indicators for Zambia": 2,
            "Mortality and global health estimates Indicators for Zambia": 2,
            "NLIS Indicators for Zambia": 2,
            "Negelected tropical diseases Indicators for Zambia": 2,
            "Neglected Tropical Diseases Indicators for Zambia": 2,
            "Neglected tropical diseases Indicators for Zambia": 2,
            "Noncommunicable diseases CCS Indicators for Zambia": 2,
            "Noncommunicable diseases Indicators for Zambia": 2,
            "Noncommunicable diseases and mental health Indicators for Zambia": 2,
            "Nutrition Indicators for Zambia": 2,
            "Public health and environment Indicators for Zambia": 2,
            "QuickCharts Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT Indicators for Zambia": 2,
            "RSUD: HUMAN RESOURCES Indicators for Zambia": 2,
            "RSUD: INFORMATION SYSTEMS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROVIDERS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVENTIONS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVICES Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREATMENT COVERAGE Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVIDERS Indicators for Zambia": 2,
            "RSUD: YOUTH Indicators for Zambia": 2,
            "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia": 2,
            "Substance use and mental health Indicators for Zambia": 2,
            "Sustainable development goals Indicators for Zambia": 2,
            "TOBACCO Indicators for Zambia": 2,
            "Tuberculosis Indicators for Zambia": 2,
            "UHC Indicators for Zambia": 2,
            "Universal Health Coverage Indicators for Zambia": 2,
            "Urban health Indicators for Zambia": 2,
            "World Health Statistics Indicators for Zambia": 2,
        }
        assert results["filter"] == ["-resources__13"]
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "data_update_frequency": "30",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "dataset_source": "World Health Organization",
            "groups": [{"name": "zmb"}],
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "methodology": "Registry",
            "name": "who-data-for-zambia",
            "notes": "Contains data from World Health Organization...",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "private": False,
            "resources": [
                {
                    "description": "See resource descriptions below for links to "
                    "indicator metadata",
                    "format": "csv",
                    "hash": "b4b147bc522828731f1a016bfa72c073",
                    "name": "All Health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Mortality and global health estimates:*\n"
                    "[Infant mortality rate (proba",
                    "format": "csv",
                    "hash": "96a3be3cf272e017046d1b2674a52bd3",
                    "name": "Mortality and global health estimates Indicators for "
                    "Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Sustainable development goals:*\n"
                    "[Adolescent birth rate (per 1000 wome",
                    "format": "csv",
                    "hash": "a2ef406e2c2351e0b9e80029c909242d",
                    "name": "Sustainable development goals Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Millennium Development Goals (MDGs):*\n"
                    "[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "hash": "e45ee7ce7e88149af8dd32b27f9512ce",
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health systems:*\n"
                    "[Median availability of selected generic "
                    "medicines (",
                    "format": "csv",
                    "hash": "7d0665438e81d8eceb98c1e31fca80c1",
                    "name": "Health systems Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Malaria:*\n"
                    "[Children aged <5 years sleeping under "
                    "insecticide-treated ",
                    "format": "csv",
                    "hash": "751d31dd6b56b26b29dac2c0e1839e34",
                    "name": "Malaria Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Tuberculosis:*\n"
                    "[Deaths due to tuberculosis among HIV-negative "
                    "people ",
                    "format": "csv",
                    "hash": "faeac4e1eef307c2ab7b0a3821e6c667",
                    "name": "Tuberculosis Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Child health:*\n"
                    "[Children aged <5 years stunted "
                    "(%)](https://www.who.i",
                    "format": "csv",
                    "hash": "d72d187df41e10ea7d9fcdc7f5909205",
                    "name": "Child health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Infectious diseases:*\n"
                    "[Cholera - number of reported "
                    "cases](https://ww",
                    "format": "csv",
                    "hash": "fad6f4e614a212e80c67249a666d2b09",
                    "name": "Infectious diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*World Health Statistics:*\n"
                    "[Literacy rate among adults aged >= 15 year",
                    "format": "csv",
                    "hash": "d3d9446802a44259755d38e6d163e820",
                    "name": "World Health Statistics Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health financing:*\n"
                    "[Private prepaid plans as a percentage of "
                    "private ",
                    "format": "csv",
                    "hash": "6512bd43d9caa6e02c990b0a82652dca",
                    "name": "Health financing Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Public health and environment:*\n"
                    "[Population using solid fuels (%)](ht",
                    "format": "csv",
                    "hash": "c74d97b01eae257e44aa9d5bade97baf",
                    "name": "Public health and environment Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Substance use and mental health:*\n"
                    "[Fines for violations](https://www.",
                    "format": "csv",
                    "hash": "c20ad4d76fe97759aa27a0c99bff6710",
                    "name": "Substance use and mental health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Injuries and violence:*\n"
                    "[Income level](https://www.who.int/data/gho/i",
                    "format": "csv",
                    "hash": "aab3238922bcc25a6f606eb525ffdc56",
                    "name": "Injuries and violence Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*HIV/AIDS and other STIs:*\n"
                    "[Prevalence of HIV among adults aged 15 to ",
                    "format": "csv",
                    "hash": "9bf31c7ff062936a96d3c8bd1f8f2ff3",
                    "name": "HIV/AIDS and other STIs Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Nutrition:*\n"
                    "[Early initiation of breastfeeding "
                    "(%)](https://www.who.i",
                    "format": "csv",
                    "hash": "70efdf2ec9b086079795c442636b55fb",
                    "name": "Nutrition Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Urban health:*\n"
                    "[Percentage of the total population living in "
                    "cities >",
                    "format": "csv",
                    "hash": "6f4922f45568161a8cdf4ad2299f6d23",
                    "name": "Urban health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases:*\n"
                    "[Prevalence of overweight among adults, BM",
                    "format": "csv",
                    "hash": "1f0e3dad99908345f7439f8ffabdffc4",
                    "name": "Noncommunicable diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases CCS:*\n"
                    "[Existence of operational policy/strat",
                    "format": "csv",
                    "hash": "98f13708210194c475687be6106a3b84",
                    "name": "Noncommunicable diseases CCS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Negelected tropical diseases:*\n"
                    "[Number of new reported cases of Burul",
                    "format": "csv",
                    "hash": "3c59dc048e8850243be8079a5c74d079",
                    "name": "Negelected tropical diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health Equity Monitor:*\n"
                    "[Antenatal care coverage - at least one visit",
                    "format": "csv",
                    "hash": "4e732ced3463d06de0ca9a15b6153677",
                    "name": "Health Equity Monitor Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Infrastructure:*\n"
                    "[Total density per 100 000 population: Health "
                    "posts]",
                    "format": "csv",
                    "hash": "b6d767d2f8ed5d21a44b0e5886680cb9",
                    "name": "Infrastructure Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Essential health technologies:*\n"
                    "[Availability of national standards o",
                    "format": "csv",
                    "hash": "37693cfc748049e45d87b8c7d8b9aacd",
                    "name": "Essential health technologies Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Medical equipment:*\n"
                    "[Total density per million population: "
                    "Magnetic R",
                    "format": "csv",
                    "hash": "1ff1de774005f8da13f42943881c655f",
                    "name": "Medical equipment Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Demographic and socioeconomic statistics:*\n"
                    "[Cellular subscribers (per",
                    "format": "csv",
                    "hash": "8e296a067a37563370ded05f5a3bf3ec",
                    "name": "Demographic and socioeconomic statistics Indicators "
                    "for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Neglected tropical diseases:*\n"
                    "[Status of yaws endemicity](https://www",
                    "format": "csv",
                    "hash": "33e75ff09dd601bbe69f351039152189",
                    "name": "Neglected tropical diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*International Health Regulations (2005) "
                    "monitoring framework:*\n"
                    "[Legis",
                    "format": "csv",
                    "hash": "6ea9ab1baa0efb9e19094440c317e21b",
                    "name": "International Health Regulations (2005) monitoring "
                    "framework Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Insecticide resistance:*\n"
                    "[Number of insecticide classes to which resi",
                    "format": "csv",
                    "hash": "34173cb38f07f89ddbebc2ac9128303f",
                    "name": "Insecticide resistance Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Universal Health Coverage:*\n"
                    "[Cataract surgical coverage of adults age",
                    "format": "csv",
                    "hash": "c16a5320fa475530d9583c34fd356ef5",
                    "name": "Universal Health Coverage Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Global Observatory for eHealth (GOe):*\n"
                    "[National universal health cov",
                    "format": "csv",
                    "hash": "182be0c5cdcd5072bb1864cdee4d3d6e",
                    "name": "Global Observatory for eHealth (GOe) Indicators for "
                    "Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING : "
                    "PREVENTION:*\n"
                    "[Government uni",
                    "format": "csv",
                    "hash": "e369853df766fa44e1ed0ff613f563bd",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: "
                    "TREATMENT:*\n"
                    "[Government unit/",
                    "format": "csv",
                    "hash": "1c383cd30b7c298ab50293adfecb7b18",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: "
                    "FINANCING:*\n"
                    "[Five-year change",
                    "format": "csv",
                    "hash": "19ca14e7ea6328a42e0eb13d585e4c22",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "TREATMENT SECTORS AND PROVID",
                    "format": "csv",
                    "hash": "a5bfc9e07964f8dddeb95fc584cd965d",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT "
                    "SECTORS AND PROVIDERS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "TREATMENT CAPACITY AND TREAT",
                    "format": "csv",
                    "hash": "a5771bce93e200c36f7cd9dfd0e5deaa",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT "
                    "CAPACITY AND TREATMENT COVERAGE Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PHARMACOLOGICAL TREATMENT:*\n",
                    "format": "csv",
                    "hash": "d67d8ab4f4c10bf22aa353e27879133c",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PHARMACOLOGICAL TREATMENT Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "SCREENING AND BRIEF INTERVEN",
                    "format": "csv",
                    "hash": "d645920e395fedad7bbbed0eca3fe2e0",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING "
                    "AND BRIEF INTERVENTIONS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PREVENTION PROGRAMS AND PROV",
                    "format": "csv",
                    "hash": "3416a75f4cea9109507cacd8e2f2aefc",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION "
                    "PROGRAMS AND PROVIDERS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "SPECIAL PROGRAMMES AND SERVI",
                    "format": "csv",
                    "hash": "a1d0c6e83f027327d8461063f4ac58a6",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL "
                    "PROGRAMMES AND SERVICES Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: HUMAN RESOURCES:*\n"
                    "[Health professionals providing treatment for",
                    "format": "csv",
                    "hash": "17e62166fc8586dfa4d1bc0e1742c08b",
                    "name": "RSUD: HUMAN RESOURCES Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: INFORMATION SYSTEMS:*\n"
                    "[Epidemiological data collection for subs",
                    "format": "csv",
                    "hash": "f7177163c833dff4b38fc8d2872f1ec6",
                    "name": "RSUD: INFORMATION SYSTEMS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: YOUTH:*\n"
                    "[Epidemiological data collection system for "
                    "substance u",
                    "format": "csv",
                    "hash": "6c8349cc7260ae62e3b1396831a8398f",
                    "name": "RSUD: YOUTH Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*FINANCIAL PROTECTION:*\n"
                    "[Population with household expenditures on hea",
                    "format": "csv",
                    "hash": "d9d4f495e875a2e075a1a4a6e1b9770f",
                    "name": "FINANCIAL PROTECTION Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases and mental health:*\n"
                    "[Number of deaths attrib",
                    "format": "csv",
                    "hash": "67c6a1e7ce56d3d6fa748ab6d9af3fd7",
                    "name": "Noncommunicable diseases and mental health Indicators "
                    "for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health workforce:*\n"
                    "[Medical doctors (per 10 000 "
                    "population)](https://",
                    "format": "csv",
                    "hash": "642e92efb79421734881b53e1e1b18b6",
                    "name": "Health workforce Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Neglected Tropical Diseases:*\n"
                    "[Number of new leprosy cases](https://w",
                    "format": "csv",
                    "hash": "33e75ff09dd601bbe69f351039152189",
                    "name": "Neglected Tropical Diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "hash": "d82c8d1619ad8176d665453cfb2e55f0",
                    "name": "QuickCharts Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*TOBACCO:*\n"
                    "[Monitor](https://www.who.int/data/gho/indicator-metadata-r",
                    "format": "csv",
                    "hash": "02e74f10e0327ad868d138f2b4fdd6f0",
                    "name": "TOBACCO Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*UHC:*\n"
                    "[Population in malaria-endemic areas who slept "
                    "under an insecti",
                    "format": "csv",
                    "hash": "6364d3f0f495b6ab9dcf8d3b5c6e0b01",
                    "name": "UHC Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*ICD:*\n"
                    "[ICD-11 implementation progress "
                    "level](https://web-prod.who.int",
                    "format": "csv",
                    "hash": "f457c545a9ded88f18ecee47145a72c0",
                    "name": "ICD Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*SEXUAL AND REPRODUCTIVE HEALTH:*\n"
                    "[Institutional Births (birth taken p",
                    "format": "csv",
                    "hash": "c0c7c76d30bd3dcaefc96f40275bdc0a",
                    "name": "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Immunization:*\n"
                    "[Proportion of vaccination cards seen "
                    "(%)](https://www",
                    "format": "csv",
                    "hash": "2838023a778dfaecdc212708f721b788",
                    "name": "Immunization Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*NLIS:*\n"
                    "[Subclinical vitamin A deficiency in "
                    "preschool-age children (s",
                    "format": "csv",
                    "hash": "9a1158154dfa42caddbd0694a4e9bdc8",
                    "name": "NLIS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
            ],
            "state": "active",
            "subnational": "0",
            "tags": [
                {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"}
            ],
            "title": "Zambia - Health Indicators",
        }
        self.check_resources(update, results)

    def test_update_logic_2(
        self,
        configuration,
        new_dataset,
        dataset,
        new_resources_yaml,
        resources_yaml,
        temp_dir,
    ):
        self.add_new_dataset_resources(
            temp_dir, new_dataset, new_resources_yaml, exclude=[52]
        )
        self.add_dataset_resources(dataset, resources_yaml)
        dataset._old_data = new_dataset.data
        dataset._old_data["resources"] = new_dataset._resources
        statuses, results = dataset._dataset_hdx_update(
            allow_no_resources=False,
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert statuses == {
            "All Health Indicators for Zambia": 2,
            "Child health Indicators for Zambia": 2,
            "Demographic and socioeconomic statistics Indicators for Zambia": 2,
            "Essential health technologies Indicators for Zambia": 2,
            "FINANCIAL PROTECTION Indicators for Zambia": 2,
            "Global Observatory for eHealth (GOe) Indicators for Zambia": 2,
            "HIV/AIDS and other STIs Indicators for Zambia": 2,
            "Health Equity Monitor Indicators for Zambia": 2,
            "Health financing Indicators for Zambia": 2,
            "Health systems Indicators for Zambia": 2,
            "Health workforce Indicators for Zambia": 2,
            "ICD Indicators for Zambia": 2,
            "Immunization Indicators for Zambia": 2,
            "Infectious diseases Indicators for Zambia": 2,
            "Infrastructure Indicators for Zambia": 2,
            "Injuries and violence Indicators for Zambia": 2,
            "Insecticide resistance Indicators for Zambia": 2,
            "International Health Regulations (2005) monitoring framework Indicators for Zambia": 2,
            "Malaria Indicators for Zambia": 2,
            "Medical equipment Indicators for Zambia": 2,
            "Millennium Development Goals (MDGs) Indicators for Zambia": 2,
            "Mortality and global health estimates Indicators for Zambia": 2,
            "Negelected tropical diseases Indicators for Zambia": 2,
            "Neglected Tropical Diseases Indicators for Zambia": 2,
            "Neglected tropical diseases Indicators for Zambia": 2,
            "Noncommunicable diseases CCS Indicators for Zambia": 2,
            "Noncommunicable diseases Indicators for Zambia": 2,
            "Noncommunicable diseases and mental health Indicators for Zambia": 2,
            "Nutrition Indicators for Zambia": 2,
            "Public health and environment Indicators for Zambia": 2,
            "QuickCharts Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING Indicators for Zambia": 2,
            "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT Indicators for Zambia": 2,
            "RSUD: HUMAN RESOURCES Indicators for Zambia": 2,
            "RSUD: INFORMATION SYSTEMS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: PHARMACOLOGICAL TREATMENT Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION PROGRAMS AND PROVIDERS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING AND BRIEF INTERVENTIONS Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL PROGRAMMES AND SERVICES Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT CAPACITY AND TREATMENT COVERAGE Indicators for Zambia": 2,
            "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT SECTORS AND PROVIDERS Indicators for Zambia": 2,
            "RSUD: YOUTH Indicators for Zambia": 2,
            "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia": 2,
            "Substance use and mental health Indicators for Zambia": 2,
            "Sustainable development goals Indicators for Zambia": 2,
            "TOBACCO Indicators for Zambia": 2,
            "Tobacco Indicators for Zambia": 2,
            "Tuberculosis Indicators for Zambia": 2,
            "UHC Indicators for Zambia": 2,
            "Universal Health Coverage Indicators for Zambia": 2,
            "Urban health Indicators for Zambia": 2,
            "World Health Statistics Indicators for Zambia": 2,
        }
        assert results["filter"] == ["-resources__53"]
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "data_update_frequency": "30",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "dataset_source": "World Health Organization",
            "groups": [{"name": "zmb"}],
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "methodology": "Registry",
            "name": "who-data-for-zambia",
            "notes": "Contains data from World Health Organization...",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "private": False,
            "resources": [
                {
                    "description": "See resource descriptions below for links to "
                    "indicator metadata",
                    "format": "csv",
                    "hash": "b4b147bc522828731f1a016bfa72c073",
                    "name": "All Health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Mortality and global health estimates:*\n"
                    "[Infant mortality rate (proba",
                    "format": "csv",
                    "hash": "96a3be3cf272e017046d1b2674a52bd3",
                    "name": "Mortality and global health estimates Indicators for "
                    "Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Sustainable development goals:*\n"
                    "[Adolescent birth rate (per 1000 wome",
                    "format": "csv",
                    "hash": "a2ef406e2c2351e0b9e80029c909242d",
                    "name": "Sustainable development goals Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Millennium Development Goals (MDGs):*\n"
                    "[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "hash": "e45ee7ce7e88149af8dd32b27f9512ce",
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health systems:*\n"
                    "[Median availability of selected generic "
                    "medicines (",
                    "format": "csv",
                    "hash": "7d0665438e81d8eceb98c1e31fca80c1",
                    "name": "Health systems Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Malaria:*\n"
                    "[Children aged <5 years sleeping under "
                    "insecticide-treated ",
                    "format": "csv",
                    "hash": "751d31dd6b56b26b29dac2c0e1839e34",
                    "name": "Malaria Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Tuberculosis:*\n"
                    "[Deaths due to tuberculosis among HIV-negative "
                    "people ",
                    "format": "csv",
                    "hash": "faeac4e1eef307c2ab7b0a3821e6c667",
                    "name": "Tuberculosis Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Child health:*\n"
                    "[Children aged <5 years stunted "
                    "(%)](https://www.who.i",
                    "format": "csv",
                    "hash": "d72d187df41e10ea7d9fcdc7f5909205",
                    "name": "Child health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Infectious diseases:*\n"
                    "[Cholera - number of reported "
                    "cases](https://ww",
                    "format": "csv",
                    "hash": "fad6f4e614a212e80c67249a666d2b09",
                    "name": "Infectious diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*World Health Statistics:*\n"
                    "[Literacy rate among adults aged >= 15 year",
                    "format": "csv",
                    "hash": "d3d9446802a44259755d38e6d163e820",
                    "name": "World Health Statistics Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health financing:*\n"
                    "[Private prepaid plans as a percentage of "
                    "private ",
                    "format": "csv",
                    "hash": "6512bd43d9caa6e02c990b0a82652dca",
                    "name": "Health financing Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Public health and environment:*\n"
                    "[Population using solid fuels (%)](ht",
                    "format": "csv",
                    "hash": "c74d97b01eae257e44aa9d5bade97baf",
                    "name": "Public health and environment Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Substance use and mental health:*\n"
                    "[Fines for violations](https://www.",
                    "format": "csv",
                    "hash": "c20ad4d76fe97759aa27a0c99bff6710",
                    "name": "Substance use and mental health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Tobacco:*\n"
                    "[Prevalence of smoking any tobacco product "
                    "among persons ag",
                    "format": "csv",
                    "hash": "02e74f10e0327ad868d138f2b4fdd6f0",
                    "name": "Tobacco Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Injuries and violence:*\n"
                    "[Income level](https://www.who.int/data/gho/i",
                    "format": "csv",
                    "hash": "aab3238922bcc25a6f606eb525ffdc56",
                    "name": "Injuries and violence Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*HIV/AIDS and other STIs:*\n"
                    "[Prevalence of HIV among adults aged 15 to ",
                    "format": "csv",
                    "hash": "9bf31c7ff062936a96d3c8bd1f8f2ff3",
                    "name": "HIV/AIDS and other STIs Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Nutrition:*\n"
                    "[Early initiation of breastfeeding "
                    "(%)](https://www.who.i",
                    "format": "csv",
                    "hash": "70efdf2ec9b086079795c442636b55fb",
                    "name": "Nutrition Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Urban health:*\n"
                    "[Percentage of the total population living in "
                    "cities >",
                    "format": "csv",
                    "hash": "6f4922f45568161a8cdf4ad2299f6d23",
                    "name": "Urban health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases:*\n"
                    "[Prevalence of overweight among adults, BM",
                    "format": "csv",
                    "hash": "1f0e3dad99908345f7439f8ffabdffc4",
                    "name": "Noncommunicable diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases CCS:*\n"
                    "[Existence of operational policy/strat",
                    "format": "csv",
                    "hash": "98f13708210194c475687be6106a3b84",
                    "name": "Noncommunicable diseases CCS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Negelected tropical diseases:*\n"
                    "[Number of new reported cases of Burul",
                    "format": "csv",
                    "hash": "3c59dc048e8850243be8079a5c74d079",
                    "name": "Negelected tropical diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health Equity Monitor:*\n"
                    "[Antenatal care coverage - at least one visit",
                    "format": "csv",
                    "hash": "4e732ced3463d06de0ca9a15b6153677",
                    "name": "Health Equity Monitor Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Infrastructure:*\n"
                    "[Total density per 100 000 population: Health "
                    "posts]",
                    "format": "csv",
                    "hash": "b6d767d2f8ed5d21a44b0e5886680cb9",
                    "name": "Infrastructure Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Essential health technologies:*\n"
                    "[Availability of national standards o",
                    "format": "csv",
                    "hash": "37693cfc748049e45d87b8c7d8b9aacd",
                    "name": "Essential health technologies Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Medical equipment:*\n"
                    "[Total density per million population: "
                    "Magnetic R",
                    "format": "csv",
                    "hash": "1ff1de774005f8da13f42943881c655f",
                    "name": "Medical equipment Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Demographic and socioeconomic statistics:*\n"
                    "[Cellular subscribers (per",
                    "format": "csv",
                    "hash": "8e296a067a37563370ded05f5a3bf3ec",
                    "name": "Demographic and socioeconomic statistics Indicators "
                    "for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Neglected tropical diseases:*\n"
                    "[Status of yaws endemicity](https://www",
                    "format": "csv",
                    "hash": "33e75ff09dd601bbe69f351039152189",
                    "name": "Neglected tropical diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*International Health Regulations (2005) "
                    "monitoring framework:*\n"
                    "[Legis",
                    "format": "csv",
                    "hash": "6ea9ab1baa0efb9e19094440c317e21b",
                    "name": "International Health Regulations (2005) monitoring "
                    "framework Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Insecticide resistance:*\n"
                    "[Number of insecticide classes to which resi",
                    "format": "csv",
                    "hash": "34173cb38f07f89ddbebc2ac9128303f",
                    "name": "Insecticide resistance Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Universal Health Coverage:*\n"
                    "[Cataract surgical coverage of adults age",
                    "format": "csv",
                    "hash": "c16a5320fa475530d9583c34fd356ef5",
                    "name": "Universal Health Coverage Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Global Observatory for eHealth (GOe):*\n"
                    "[National universal health cov",
                    "format": "csv",
                    "hash": "182be0c5cdcd5072bb1864cdee4d3d6e",
                    "name": "Global Observatory for eHealth (GOe) Indicators for "
                    "Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING : "
                    "PREVENTION:*\n"
                    "[Government uni",
                    "format": "csv",
                    "hash": "e369853df766fa44e1ed0ff613f563bd",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING : PREVENTION "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: "
                    "TREATMENT:*\n"
                    "[Government unit/",
                    "format": "csv",
                    "hash": "1c383cd30b7c298ab50293adfecb7b18",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: TREATMENT "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: GOVERNANCE, POLICY AND FINANCING: "
                    "FINANCING:*\n"
                    "[Five-year change",
                    "format": "csv",
                    "hash": "19ca14e7ea6328a42e0eb13d585e4c22",
                    "name": "RSUD: GOVERNANCE, POLICY AND FINANCING: FINANCING "
                    "Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "TREATMENT SECTORS AND PROVID",
                    "format": "csv",
                    "hash": "a5bfc9e07964f8dddeb95fc584cd965d",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT "
                    "SECTORS AND PROVIDERS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "TREATMENT CAPACITY AND TREAT",
                    "format": "csv",
                    "hash": "a5771bce93e200c36f7cd9dfd0e5deaa",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: TREATMENT "
                    "CAPACITY AND TREATMENT COVERAGE Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PHARMACOLOGICAL TREATMENT:*\n",
                    "format": "csv",
                    "hash": "d67d8ab4f4c10bf22aa353e27879133c",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PHARMACOLOGICAL TREATMENT Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "SCREENING AND BRIEF INTERVEN",
                    "format": "csv",
                    "hash": "d645920e395fedad7bbbed0eca3fe2e0",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SCREENING "
                    "AND BRIEF INTERVENTIONS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "PREVENTION PROGRAMS AND PROV",
                    "format": "csv",
                    "hash": "3416a75f4cea9109507cacd8e2f2aefc",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: PREVENTION "
                    "PROGRAMS AND PROVIDERS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: SERVICE ORGANIZATION AND DELIVERY: "
                    "SPECIAL PROGRAMMES AND SERVI",
                    "format": "csv",
                    "hash": "a1d0c6e83f027327d8461063f4ac58a6",
                    "name": "RSUD: SERVICE ORGANIZATION AND DELIVERY: SPECIAL "
                    "PROGRAMMES AND SERVICES Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: HUMAN RESOURCES:*\n"
                    "[Health professionals providing treatment for",
                    "format": "csv",
                    "hash": "17e62166fc8586dfa4d1bc0e1742c08b",
                    "name": "RSUD: HUMAN RESOURCES Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: INFORMATION SYSTEMS:*\n"
                    "[Epidemiological data collection for subs",
                    "format": "csv",
                    "hash": "f7177163c833dff4b38fc8d2872f1ec6",
                    "name": "RSUD: INFORMATION SYSTEMS Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*RSUD: YOUTH:*\n"
                    "[Epidemiological data collection system for "
                    "substance u",
                    "format": "csv",
                    "hash": "6c8349cc7260ae62e3b1396831a8398f",
                    "name": "RSUD: YOUTH Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*FINANCIAL PROTECTION:*\n"
                    "[Population with household expenditures on hea",
                    "format": "csv",
                    "hash": "d9d4f495e875a2e075a1a4a6e1b9770f",
                    "name": "FINANCIAL PROTECTION Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Noncommunicable diseases and mental health:*\n"
                    "[Number of deaths attrib",
                    "format": "csv",
                    "hash": "67c6a1e7ce56d3d6fa748ab6d9af3fd7",
                    "name": "Noncommunicable diseases and mental health Indicators "
                    "for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Health workforce:*\n"
                    "[Medical doctors (per 10 000 "
                    "population)](https://",
                    "format": "csv",
                    "hash": "642e92efb79421734881b53e1e1b18b6",
                    "name": "Health workforce Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Neglected Tropical Diseases:*\n"
                    "[Number of new leprosy cases](https://w",
                    "format": "csv",
                    "hash": "33e75ff09dd601bbe69f351039152189",
                    "name": "Neglected Tropical Diseases Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "Cut down data for QuickCharts",
                    "format": "csv",
                    "hash": "d82c8d1619ad8176d665453cfb2e55f0",
                    "name": "QuickCharts Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*TOBACCO:*\n"
                    "[Monitor](https://www.who.int/data/gho/indicator-metadata-r",
                    "format": "csv",
                    "hash": "02e74f10e0327ad868d138f2b4fdd6f0",
                    "name": "TOBACCO Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*UHC:*\n"
                    "[Population in malaria-endemic areas who slept "
                    "under an insecti",
                    "format": "csv",
                    "hash": "6364d3f0f495b6ab9dcf8d3b5c6e0b01",
                    "name": "UHC Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*ICD:*\n"
                    "[ICD-11 implementation progress "
                    "level](https://web-prod.who.int",
                    "format": "csv",
                    "hash": "f457c545a9ded88f18ecee47145a72c0",
                    "name": "ICD Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*SEXUAL AND REPRODUCTIVE HEALTH:*\n"
                    "[Institutional Births (birth taken p",
                    "format": "csv",
                    "hash": "c0c7c76d30bd3dcaefc96f40275bdc0a",
                    "name": "SEXUAL AND REPRODUCTIVE HEALTH Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Immunization:*\n"
                    "[Proportion of vaccination cards seen "
                    "(%)](https://www",
                    "format": "csv",
                    "hash": "2838023a778dfaecdc212708f721b788",
                    "name": "Immunization Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
            ],
            "state": "active",
            "subnational": "0",
            "tags": [
                {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"}
            ],
            "title": "Zambia - Health Indicators",
        }
        self.check_resources(update, results)

    def test_update_logic_3(
        self,
        configuration,
        new_dataset,
        dataset,
        new_resources_yaml,
        resources_yaml,
        temp_dir,
    ):
        self.add_new_dataset_resources(
            temp_dir, new_dataset, new_resources_yaml, include=[0, 3]
        )
        self.add_dataset_resources(dataset, resources_yaml, include=[0, 1, 2])
        dataset._old_data = new_dataset.data
        dataset._old_data["resources"] = new_dataset._resources
        statuses, results = dataset._dataset_hdx_update(
            allow_no_resources=False,
            update_resources=True,
            match_resources_by_metadata=True,
            keys_to_delete=[],
            remove_additional_resources=True,
            match_resource_order=False,
            create_default_views=False,
            hxl_update=False,
            test=True,
        )
        assert statuses == {
            "All Health Indicators for Zambia": 2,
            "Millennium Development Goals (MDGs) Indicators for Zambia": 2,
        }
        assert results["filter"] == ["-resources__2", "-resources__1"]
        update = results["update"]
        del update["updated_by_script"]
        assert update == {
            "data_update_frequency": "30",
            "dataset_date": "[1961-01-01T00:00:00 TO 2019-12-31T23:59:59]",
            "dataset_source": "World Health Organization",
            "groups": [{"name": "zmb"}],
            "license_id": "hdx-other",
            "license_other": "CC BY-NC-SA 3.0 IGO",
            "maintainer": "35f7bb2c-4ab6-4796-8334-525b30a94c89",
            "methodology": "Registry",
            "name": "who-data-for-zambia",
            "notes": "Contains data from World Health Organization...",
            "owner_org": "c021f6be-3598-418e-8f7f-c7a799194dba",
            "private": False,
            "resources": [
                {
                    "description": "See resource descriptions below for links to "
                    "indicator metadata",
                    "format": "csv",
                    "hash": "b4b147bc522828731f1a016bfa72c073",
                    "name": "All Health Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
                {
                    "description": "*Millennium Development Goals (MDGs):*\n"
                    "[Contraceptive prevalence (%)](",
                    "format": "csv",
                    "hash": "e45ee7ce7e88149af8dd32b27f9512ce",
                    "name": "Millennium Development Goals (MDGs) Indicators for Zambia",
                    "resource_type": "file.upload",
                    "size": 2,
                    "url": "updated_by_file_upload_step",
                    "url_type": "upload",
                },
            ],
            "state": "active",
            "subnational": "0",
            "tags": [
                {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"}
            ],
            "title": "Zambia - Health Indicators",
        }
        self.check_resources(update, results)
