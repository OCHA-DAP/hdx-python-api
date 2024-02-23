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
            new_dataset.resources, Resource, ("file_to_upload", "data_updated")
        )
        (
            resources_to_update,
            resources_to_delete,
            new_resource_order,
            filestore_resources,
        ) = dataset._dataset_update_resources(True, True, True, True)
        assert resources_to_update == [
            {},
            {},
            {},
            {
                "dataset_preview_enabled": "False",
                "description": "SDG 4 Global and Thematic data with HXL tags.\n"
                "\n"
                "Indicators: Adjusted attendance rate, Adjusted net "
                "attendance rate, Adjusted net enrolment rate, Administration "
                "of a nationally representative learning assessment in Grade "
                "2 or 3 in mathematics, Administration of a nationally "
                "representative learning assessment in Grade 2 or 3 in "
                "reading, Administration of a nationally-representative "
                "learning assessment at the end of lower secondary education "
                "in mathematics, Administration of a "
                "nationally-representative learning assessment at the end of "
                "lower secondary education in reading, Administration of a "
                "nationally-representative learning assessment at the end of "
                "primary in mathematics, Administration of a "
                "nationally-representative learning assessment at the end of "
                "primary in reading, Adult literacy rate, Average teacher "
                "salary in lower secondary education relative to other "
                "professions requiring a comparable level of qualification, "
                "Average teacher salary in pre-primary education relative to "
                "other professions requiring a comparable level of "
                "qualification, Average teacher salary in primary education "
                "relative to other professions requiring a comparable level "
                "of qualification, Average teacher salary in upper secondary "
                "education relative to other professions requiring a "
                "comparable level of qualification, Completion rate, "
                "Educational attainment, Educational attainment rate, Elderly "
                "literacy rate, Expenditure on education as a percentage of "
                "total government expenditure, Government expenditure on "
                "education as a percentage of GDP, Gross attendance ratio for "
                "tertiary education, Gross enrolment ratio, Gross enrolment "
                "ratio for tertiary education, Gross intake ratio to the last "
                "grade of lower secondary general education, Gross intake "
                "ratio to the last grade of primary education, Initial "
                "government funding per lower secondary student, Initial "
                "government funding per lower secondary student as a "
                "percentage of GDP per capita, Initial government funding per "
                "pre-primary student, Initial government funding per "
                "pre-primary student as a percentage of GDP per capita, "
                "Initial government funding per primary student, Initial "
                "government funding per primary student as a percentage of "
                "GDP per capita, Initial government funding per secondary "
                "student, Initial government funding per secondary student as "
                "a percentage of GDP per capita, Initial government funding "
                "per tertiary student, Initial government funding per "
                "tertiary student as a percentage of GDP per capita, Initial "
                "government funding per upper secondary student, Initial "
                "government funding per upper secondary student as a "
                "percentage of GDP per capita, Initial household funding per "
                "primary student, Initial household funding per primary "
                "student as a percentage of GDP per capita, Initial household "
                "funding per secondary student, Initial household funding per "
                "secondary student as a percentage of GDP per capita, Initial "
                "household funding per tertiary student, Initial household "
                "funding per tertiary student as a percentage of GDP per "
                "capita, Literacy rate, Number of attacks on students, Number "
                "of years of compulsory pre-primary education guaranteed in "
                "legal frameworks, Number of years of compulsory primary and "
                "secondary education guaranteed in legal frameworks, Number "
                "of years of free pre-primary education guaranteed in legal "
                "frameworks, Number of years of free primary and secondary "
                "education guaranteed in legal frameworks, Out-of-school "
                "rate, Out-of-school rate for adolescents and youth of lower "
                "and upper secondary school age, Out-of-school rate for "
                "adolescents of lower secondary school age, Out-of-school "
                "rate for children, Out-of-school rate for children and "
                "adolescents of primary and lower secondary school age, "
                "Out-of-school rate for children of primary school age, "
                "Out-of-school rate for children one year younger than "
                "official age, Out-of-school rate for children one year "
                "younger than official primary entry age, Out-of-school rate "
                "for youth of upper secondary school age, Participants in "
                "literacy programmes as a % of the illiterate population, "
                "Participation rate of youth and adults in formal and "
                "non-formal education and training in the previous 12 months, "
                "Percentage of children under 5 years experiencing positive "
                "and stimulating home learning environments, Percentage of "
                "lower secondary schools providing life skills-based HIV and "
                "sexuality education, Percentage of primary schools providing "
                "life skills-based HIV and sexuality education, Percentage of "
                "pupils enrolled in lower secondary general education who are "
                "at least 2 years over-age for their current grade, "
                "Percentage of pupils enrolled in primary education who are "
                "at least 2 years over-age for their current grade, "
                "Percentage of qualified teachers in lower secondary "
                "education, Percentage of qualified teachers in pre-primary "
                "education, Percentage of qualified teachers in primary "
                "education, Percentage of qualified teachers in secondary "
                "education, Percentage of qualified teachers in upper "
                "secondary education, Percentage of students at the end of "
                "lower secondary education who have their first or home "
                "language as language of instruction, Percentage of students "
                "at the end of primary education who have their first or home "
                "language as language of instruction, Percentage of students "
                "experiencing bullying in the last 12 months in lower "
                "secondary education, Percentage of students experiencing "
                "bullying in the last 12 months in primary education, "
                "Percentage of students in early grades who have their first "
                "or home language as language of instruction, Percentage of "
                "students in lower secondary education who have their first "
                "or home language as language of instruction, Percentage of "
                "students in lower secondary showing adequate understanding "
                "of issues relating to global citizenship and sustainability, "
                "Percentage of students in lower secondary showing adequate "
                "understanding of issues relating to global citizenship and "
                "sustainability - Cognitive Dimension, Percentage of students "
                "in lower secondary showing adequate understanding of issues "
                "relating to global citizenship and sustainability â€“ "
                "Non-cognitive Dimension â€“ Freedom, Percentage of students "
                "in lower secondary showing adequate understanding of issues "
                "relating to global citizenship and sustainability â€“ "
                "Non-cognitive Dimension â€“ Gender equality, Percentage of "
                "students in lower secondary showing adequate understanding "
                "of issues relating to global citizenship and sustainability "
                "â€“ Non-cognitive Dimension â€“ Global-local thinking, "
                "Percentage of students in lower secondary showing adequate "
                "understanding of issues relating to global citizenship and "
                "sustainability â€“ Non-cognitive Dimension â€“ "
                "Multiculturalism, Percentage of students in lower secondary "
                "showing adequate understanding of issues relating to global "
                "citizenship and sustainability â€“ Non-cognitive Dimension "
                "â€“ Peace, Percentage of students in lower secondary showing "
                "adequate understanding of issues relating to global "
                "citizenship and sustainability â€“ Non-cognitive Dimension "
                "â€“ Social Justice, Percentage of students in lower "
                "secondary showing adequate understanding of issues relating "
                "to global citizenship and sustainability â€“ Non-cognitive "
                "Dimension â€“ Sustainable development, Percentage of "
                "students in lower secondary showing proficiency in knowledge "
                "of environmental science and geoscience, Percentage of "
                "students in lower secondary showing proficiency in knowledge "
                "of environmental science and geoscience â€“ Cognitive "
                "dimension, Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Cognitive dimension â€“ adjusted gender "
                "parity index, Percentage of students in lower secondary "
                "showing proficiency in knowledge of environmental science "
                "and geoscience â€“ Cognitive dimension â€“ adjusted location "
                "parity index, Percentage of students in lower secondary "
                "showing proficiency in knowledge of environmental science "
                "and geoscience â€“ Cognitive dimension â€“ adjusted wealth "
                "parity index, Percentage of students in lower secondary "
                "showing proficiency in knowledge of environmental science "
                "and geoscience â€“ Non-cognitive dimension - Confidence, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Non-cognitive dimension - Enjoyment, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Non-cognitive dimension â€“ Confidence, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Non-cognitive dimension â€“ Confidence â€“ "
                "adjusted gender parity index, Percentage of students in "
                "lower secondary showing proficiency in knowledge of "
                "environmental science and geoscience â€“ Non-cognitive "
                "dimension â€“ Confidence â€“ adjusted location parity index, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Non-cognitive dimension â€“ Confidence â€“ "
                "adjusted wealth parity index, Percentage of students in "
                "lower secondary showing proficiency in knowledge of "
                "environmental science and geoscience â€“ Non-cognitive "
                "dimension â€“ Enjoyment, Percentage of students in lower "
                "secondary showing proficiency in knowledge of environmental "
                "science and geoscience â€“ Non-cognitive dimension â€“ "
                "Enjoyment â€“ adjusted gender parity index, Percentage of "
                "students in lower secondary showing proficiency in knowledge "
                "of environmental science and geoscience â€“ Non-cognitive "
                "dimension â€“ Enjoyment â€“ adjusted location parity index, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ Non-cognitive dimension â€“ Enjoyment â€“ "
                "adjusted wealth parity index, Percentage of students in "
                "lower secondary showing proficiency in knowledge of "
                "environmental science and geoscience â€“ adjusted gender "
                "parity index, Percentage of students in lower secondary "
                "showing proficiency in knowledge of environmental science "
                "and geoscience â€“ adjusted location parity index, "
                "Percentage of students in lower secondary showing "
                "proficiency in knowledge of environmental science and "
                "geoscience â€“ adjusted wealth parity index, Percentage of "
                "teachers in lower secondary education who received "
                "in-service training in the last 12 months by type of "
                "trained, Percentage of teachers in lower secondary education "
                "who received in-service training in the last 12 months by "
                "type of training, Percentage of teachers in primary "
                "education who received in-service training in the last 12 "
                "months by type of trained, Percentage of teachers in primary "
                "education who received in-service training in the last 12 "
                "months by type of training, Percentage of total aid to "
                "education allocated to least developed countries, Percentage "
                "of upper secondary schools providing life skills-based HIV "
                "and sexuality education, Percentage of youth/adults who have "
                "achieved at least a minimum level of proficiency in digital "
                "literacy skills, Proportion of 15- to 24-year-olds enrolled "
                "in vocational education, Proportion of 15-24 year-olds "
                "enrolled in vocational education, Proportion of children "
                "aged 24-59 months who are developmentally on track in "
                "health, Proportion of lower secondary schools with access to "
                "Internet for pedagogical purposes, Proportion of lower "
                "secondary schools with access to adapted infrastructure and "
                "materials for students with disabilities, Proportion of "
                "lower secondary schools with access to basic drinking water, "
                "Proportion of lower secondary schools with access to "
                "computers for pedagogical purposes, Proportion of lower "
                "secondary schools with access to electricity, Proportion of "
                "lower secondary schools with basic handwashing facilities, "
                "Proportion of lower secondary schools with single-sex basic "
                "sanitation facilities, Proportion of population achieving at "
                "least a fixed level of proficiency in functional literacy "
                "skills, Proportion of population achieving at least a fixed "
                "level of proficiency in functional numeracy skills, "
                "Proportion of primary schools with access to Internet for "
                "pedagogical purposes, Proportion of primary schools with "
                "access to adapted infrastructure and materials for students "
                "with disabilities, Proportion of primary schools with access "
                "to basic drinking water, Proportion of primary schools with "
                "access to computers for pedagogical purposes, Proportion of "
                "primary schools with access to electricity, Proportion of "
                "primary schools with basic handwashing facilities, "
                "Proportion of primary schools with single-sex basic "
                "sanitation facilities, Proportion of pupils enrolled in "
                "lower secondary general education who are at least 2 years "
                "over-age for their current grade, Proportion of pupils "
                "enrolled in primary education who are at least 2 years "
                "over-age for their current grade, Proportion of qualified "
                "teachers in lower secondary education, Proportion of "
                "qualified teachers in pre-primary education, Proportion of "
                "qualified teachers in primary education, Proportion of "
                "qualified teachers in secondary education, Proportion of "
                "qualified teachers in upper secondary education, Proportion "
                "of secondary schools with access to Internet for pedagogical "
                "purposes, Proportion of secondary schools with access to "
                "computers for pedagogical purposes, Proportion of students "
                "at the end of lower secondary achieving at least a minimum "
                "proficiency level in mathematics, Proportion of students at "
                "the end of lower secondary achieving at least a minimum "
                "proficiency level in reading, Proportion of students at the "
                "end of lower secondary education achieving at least a "
                "minimum proficiency level in mathematics, Proportion of "
                "students at the end of lower secondary education achieving "
                "at least a minimum proficiency level in reading, Proportion "
                "of students at the end of primary achieving at least a "
                "minimum proficiency level in mathematics, Proportion of "
                "students at the end of primary achieving at least a minimum "
                "proficiency level in reading, Proportion of students at the "
                "end of primary education achieving at least a minimum "
                "proficiency level in mathematics, Proportion of students at "
                "the end of primary education achieving at least a minimum "
                "proficiency level in reading, Proportion of students in "
                "Grade 2 or 3 achieving at least a minimum proficiency level "
                "in mathematics, Proportion of students in Grade 2 or 3 "
                "achieving at least a minimum proficiency level in reading, "
                "Proportion of studentsÂ at the end of primary education "
                "achieving at least a minimum proficiency level in reading, "
                "Proportion of teachers with the minimum required "
                "qualifications in lower secondary education, Proportion of "
                "teachers with the minimum required qualifications in "
                "pre-primary education, Proportion of teachers with the "
                "minimum required qualifications in primary education, "
                "Proportion of teachers with the minimum required "
                "qualifications in secondary education, Proportion of "
                "teachers with the minimum required qualifications in upper "
                "secondary education, Proportion of upper secondary schools "
                "with access to Internet for pedagogical purposes, Proportion "
                "of upper secondary schools with access to adapted "
                "infrastructure and materials for students with disabilities, "
                "Proportion of upper secondary schools with access to basic "
                "drinking water, Proportion of upper secondary schools with "
                "access to computers for pedagogical purposes, Proportion of "
                "upper secondary schools with access to electricity, "
                "Proportion of upper secondary schools with basic handwashing "
                "facilities, Proportion of upper secondary schools with "
                "single-sex basic sanitation facilities, Proportion of youth "
                "and adults who have Transferred files between a computer and "
                "other devices, Proportion of youth and adults who have "
                "connected and installed new devices, Proportion of youth and "
                "adults who have copied or moved a file or folder, Proportion "
                "of youth and adults who have created electronic "
                "presentations with presentation software, Proportion of "
                "youth and adults who have found, Proportion of youth and "
                "adults who have sent e-mails with attached files, Proportion "
                "of youth and adults who have transferred files between a "
                "computer and other devices, Proportion of youth and adults "
                "who have used basic arithmetic formulae in a spreadsheet, "
                "Proportion of youth and adults who have used copy and paste "
                "tools to duplicate or move information within a document, "
                "Proportion of youth and adults who have wrote a computer "
                "program using a specialised programming language, "
                "Pupil-qualified teacher ratio in lower secondary, "
                "Pupil-qualified teacher ratio in pre-primary education, "
                "Pupil-qualified teacher ratio in primary education, "
                "Pupil-qualified teacher ratio in secondary, Pupil-qualified "
                "teacher ratio in upper secondary, Pupil-trained teacher "
                "ratio in lower secondary education, Pupil-trained teacher "
                "ratio in pre-primary education, Pupil-trained teacher ratio "
                "in primary education, Pupil-trained teacher ratio in "
                "secondary education, Pupil-trained teacher ratio in upper "
                "secondary education, Teacher attrition rate from general "
                "secondary education, Teacher attrition rate from lower "
                "secondary education, Teacher attrition rate from pre-primary "
                "education, Teacher attrition rate from primary education, "
                "Teacher attrition rate from secondary education, Teacher "
                "attrition rate from upper secondary education, Teacher "
                "attrition rate from vocational secondary education, Volume "
                "of official development assistance flows for scholarships by "
                "sector and type of study, Youth literacy rate",
                "format": "csv",
                "name": "SDG 4 Global and Thematic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "SDG 4 Global and Thematic indicator list with HXL tags",
                "format": "csv",
                "name": "SDG 4 Global and Thematic indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "SDG 4 Global and Thematic metadata with HXL tags",
                "format": "csv",
                "name": "SDG 4 Global and Thematic metadata",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "Demographic and Socio-economic data with HXL tags.\n"
                "\n"
                "Indicators: DEC alternative conversion factor, Fertility "
                "rate, GDP, GDP at market prices, GDP deflator, GDP growth, "
                "GDP per capita, GNI, GNI per capita, General government "
                "total expenditure, Life expectancy at birth, Mortality rate, "
                "Official exchange rate, PPP conversion factor, Population "
                "aged 14 years or younger, Population aged 15-24 years, "
                "Population aged 25-64 years, Population aged 65 years or "
                "older, Population growth, Poverty headcount ratio at $3.20 a "
                "day, Prevalence of HIV, Price level ratio of PPP conversion "
                "factor, Rural population, Total debt service, Total "
                "population",
                "format": "csv",
                "name": "Demographic and Socio-economic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "Demographic and Socio-economic indicator list with HXL tags",
                "format": "csv",
                "name": "Demographic and Socio-economic indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {},
            {
                "dataset_preview_enabled": "False",
                "description": "Other Policy Relevant Indicators data with HXL tags.\n"
                "\n"
                "Indicators: Adult illiterate population, Africa, All staff "
                "compensation as a percentage of total expenditure in lower "
                "secondary public institutions, All staff compensation as a "
                "percentage of total expenditure in post-secondary "
                "non-tertiary public institutions, All staff compensation as "
                "a percentage of total expenditure in pre-primary public "
                "institutions, All staff compensation as a percentage of "
                "total expenditure in primary public institutions, All staff "
                "compensation as a percentage of total expenditure in public "
                "institutions, All staff compensation as a percentage of "
                "total expenditure in secondary public institutions, All "
                "staff compensation as a percentage of total expenditure in "
                "tertiary public institutions, All staff compensation as a "
                "percentage of total expenditure in upper secondary public "
                "institutions, Asia, Capital expenditure as a percentage of "
                "total expenditure in lower secondary public institutions, "
                "Capital expenditure as a percentage of total expenditure in "
                "post-secondary non-tertiary public institutions, Capital "
                "expenditure as a percentage of total expenditure in "
                "pre-primary public institutions, Capital expenditure as a "
                "percentage of total expenditure in primary public "
                "institutions, Capital expenditure as a percentage of total "
                "expenditure in public institutions, Capital expenditure as a "
                "percentage of total expenditure in secondary public "
                "institutions, Capital expenditure as a percentage of total "
                "expenditure in tertiary public institutions, Capital "
                "expenditure as a percentage of total expenditure in "
                "upper-secondary public institutions, Caribbean and Central "
                "America, Current expenditure as a percentage of total "
                "expenditure in lower secondary public institutions, Current "
                "expenditure as a percentage of total expenditure in "
                "post-secondary non-tertiary public institutions, Current "
                "expenditure as a percentage of total expenditure in "
                "pre-primary public institutions, Current expenditure as a "
                "percentage of total expenditure in primary public "
                "institutions, Current expenditure as a percentage of total "
                "expenditure in public institutions, Current expenditure as a "
                "percentage of total expenditure in secondary public "
                "institutions, Current expenditure as a percentage of total "
                "expenditure in tertiary public institutions, Current "
                "expenditure as a percentage of total expenditure in "
                "upper-secondary public institutions, Current expenditure "
                "other than staff compensation as a percentage of total "
                "expenditure in lower secondary public institutions, Current "
                "expenditure other than staff compensation as a percentage of "
                "total expenditure in post-secondary non-tertiary public "
                "institutions, Current expenditure other than staff "
                "compensation as a percentage of total expenditure in "
                "pre-primary public institutions, Current expenditure other "
                "than staff compensation as a percentage of total expenditure "
                "in primary public institutions, Current expenditure other "
                "than staff compensation as a percentage of total expenditure "
                "in public institutions, Current expenditure other than staff "
                "compensation as a percentage of total expenditure in "
                "secondary public institutions, Current expenditure other "
                "than staff compensation as a percentage of total expenditure "
                "in tertiary public institutions, Current expenditure other "
                "than staff compensation as a percentage of total expenditure "
                "in upper secondary public institutions, Duration of "
                "compulsory education, End month of the academic school year, "
                "End of the academic school year, Enrolment in early "
                "childhood education, Enrolment in early childhood "
                "educational development programmes, Enrolment in lower "
                "secondary education, Enrolment in post-secondary "
                "non-tertiary education, Enrolment in pre-primary education, "
                "Enrolment in primary education, Enrolment in secondary "
                "education, Enrolment in tertiary education, Enrolment in "
                "upper secondary education, Europe, Expenditure on school "
                "books and teaching material as % of total expenditure in "
                "primary public institutions, Expenditure on school books and "
                "teaching material as % of total expenditure in secondary "
                "public institutions, Government expenditure on education, "
                "Government expenditure on education not specified by level, "
                "Government expenditure on lower secondary education, "
                "Government expenditure on lower secondary education as a "
                "percentage of GDP, Government expenditure on post-secondary "
                "non-tertiary education, Government expenditure on "
                "post-secondary non-tertiary education as a percentage of "
                "GDP, Government expenditure on pre-primary education, "
                "Government expenditure on pre-primary education as a "
                "percentage of GDP, Government expenditure on primary "
                "education, Government expenditure on primary education as a "
                "percentage of GDP, Government expenditure on secondary and "
                "post-secondary non-tertiary vocational education as a "
                "percentage of GDP, Government expenditure on secondary and "
                "post-secondary non-tertiary vocational education only, "
                "Government expenditure on secondary education, Government "
                "expenditure on secondary education as a percentage of GDP, "
                "Government expenditure on tertiary education, Government "
                "expenditure on tertiary education as a percentage of GDP, "
                "Government expenditure on upper secondary education, "
                "Government expenditure on upper secondary education as a "
                "percentage of GDP, Gross enrolment ratio, Gross graduation "
                "ratio from first degree programmes, Illiterate population, "
                "Illiterate youth population, Inbound internationally mobile "
                "students from Africa, Inbound internationally mobile "
                "students from Asia, Inbound internationally mobile students "
                "from Central Asia, Inbound internationally mobile students "
                "from Central and Eastern Europe, Inbound internationally "
                "mobile students from East Asia and the Pacific, Inbound "
                "internationally mobile students from Europe, Inbound "
                "internationally mobile students from Latin America and the "
                "Caribbean, Inbound internationally mobile students from "
                "North America, Inbound internationally mobile students from "
                "North America and Western Europe, Inbound internationally "
                "mobile students from Oceania, Inbound internationally mobile "
                "students from South America, Inbound internationally mobile "
                "students from South and West Asia, Inbound internationally "
                "mobile students from sub-Saharan Africa, Inbound "
                "internationally mobile students from the Arab States, "
                "Inbound internationally mobile students from the Caribbean "
                "and Central America, Inbound internationally mobile students "
                "from unknown continents, Inbound internationally mobile "
                "students from unknown regions, Inbound mobility rate, Mean "
                "years of schooling, Net flow of internationally mobile "
                "students, Net flow ratio of internationally mobile students, "
                "Non-teaching staff compensation as a percentage of total "
                "expenditure in lower secondary public institutions, "
                "Non-teaching staff compensation as a percentage of total "
                "expenditure in post-secondary non-tertiary public "
                "institutions, Non-teaching staff compensation as a "
                "percentage of total expenditure in pre-primary public "
                "institutions, Non-teaching staff compensation as a "
                "percentage of total expenditure in primary public "
                "institutions, Non-teaching staff compensation as a "
                "percentage of total expenditure in public institutions, "
                "Non-teaching staff compensation as a percentage of total "
                "expenditure in secondary public institutions, Non-teaching "
                "staff compensation as a percentage of total expenditure in "
                "tertiary public institutions, Non-teaching staff "
                "compensation as a percentage of total expenditure in upper "
                "secondary public institutions, North America, Oceania, "
                "Official entrance age to compulsory education, Official "
                "entrance age to early childhood education, Official entrance "
                "age to early childhood educational development, Official "
                "entrance age to lower secondary education, Official entrance "
                "age to post-secondary non-tertiary education, Official "
                "entrance age to pre-primary education, Official entrance age "
                "to primary education, Official entrance age to upper "
                "secondary education, Out-of-school adolescents and youth of "
                "secondary school age, Out-of-school adolescents of lower "
                "secondary school age, Out-of-school children, Out-of-school "
                "children and adolescents of primary and lower secondary "
                "school age, Out-of-school children of primary school age, "
                "Out-of-school youth of upper secondary school age, Outbound "
                "internationally mobile tertiary students studying in Central "
                "Asia, Outbound internationally mobile tertiary students "
                "studying in Central and Eastern Europe, Outbound "
                "internationally mobile tertiary students studying in East "
                "Asia and the Pacific, Outbound internationally mobile "
                "tertiary students studying in Latin America and the "
                "Caribbean, Outbound internationally mobile tertiary students "
                "studying in North America and Western Europe, Outbound "
                "internationally mobile tertiary students studying in South "
                "and West Asia, Outbound internationally mobile tertiary "
                "students studying in sub-Saharan Africa, Outbound "
                "internationally mobile tertiary students studying in the "
                "Arab States, Outbound mobility ratio, Outbound mobility "
                "ratio to Central Asia, Outbound mobility ratio to Central "
                "and Eastern Europe, Outbound mobility ratio to East Asia and "
                "the Pacific, Outbound mobility ratio to Latin America and "
                "the Caribbean, Outbound mobility ratio to North America and "
                "Western Europe, Outbound mobility ratio to South and West "
                "Asia, Outbound mobility ratio to sub-Saharan Africa, "
                "Outbound mobility ratio to the Arab States, Percentage of "
                "enrolment in early childhood education programmes in private "
                "institutions, Percentage of enrolment in early childhood "
                "educational development programmes in private institutions, "
                "Percentage of enrolment in lower secondary education in "
                "private institutions, Percentage of enrolment in "
                "post-secondary non-tertiary education in private "
                "institutions, Percentage of enrolment in pre-primary "
                "education in private institutions, Percentage of enrolment "
                "in primary education in private institutions, Percentage of "
                "enrolment in secondary education in private institutions, "
                "Percentage of enrolment in tertiary education in private "
                "institutions, Percentage of enrolment in upper secondary "
                "education in private institutions, Percentage of graduates "
                "from Science, Percentage of graduates from programmes other "
                "than Science, Percentage of graduates from tertiary "
                "education graduating from Agriculture, Percentage of "
                "graduates from tertiary education graduating from Arts and "
                "Humanities programmes, Percentage of graduates from tertiary "
                "education graduating from Business, Percentage of graduates "
                "from tertiary education graduating from Education "
                "programmes, Percentage of graduates from tertiary education "
                "graduating from Engineering, Percentage of graduates from "
                "tertiary education graduating from Health and Welfare "
                "programmes, Percentage of graduates from tertiary education "
                "graduating from Information and Communication Technologies "
                "programmes, Percentage of graduates from tertiary education "
                "graduating from Natural Sciences, Percentage of graduates "
                "from tertiary education graduating from Services programmes, "
                "Percentage of graduates from tertiary education graduating "
                "from Social Sciences, Percentage of graduates from tertiary "
                "education graduating from programmes in unspecified fields, "
                "Percentage of teachers in lower secondary education who are "
                "female, Percentage of teachers in post-secondary "
                "non-tertiary education who are female, Percentage of "
                "teachers in pre-primary education who are female, Percentage "
                "of teachers in primary education who are female, Percentage "
                "of teachers in secondary education who are female, "
                "Percentage of teachers in tertiary education who are female, "
                "Percentage of teachers in upper secondary education who are "
                "female, Population of compulsory school age, Population of "
                "the official entrance age to primary education, Population "
                "of the official entrance age to secondary general education, "
                "Repeaters in Grade 1 of lower secondary general education, "
                "Repeaters in Grade 1 of primary education, Repeaters in "
                "Grade 2 of lower secondary general education, Repeaters in "
                "Grade 2 of primary education, Repeaters in Grade 3 of lower "
                "secondary general education, Repeaters in Grade 3 of primary "
                "education, Repeaters in Grade 4 of lower secondary general "
                "education, Repeaters in Grade 4 of primary education, "
                "Repeaters in Grade 5 of lower secondary general education, "
                "Repeaters in Grade 5 of primary education, Repeaters in "
                "Grade 6 of lower secondary general education, Repeaters in "
                "Grade 6 of primary education, Repeaters in Grade 7 of "
                "primary education, Repeaters in grade unknown of lower "
                "secondary general education, Repeaters in grade unknown of "
                "primary education, Repeaters in lower secondary general "
                "education, Repeaters in primary education, Repetition rate "
                "in Grade 1 of lower secondary general education, Repetition "
                "rate in Grade 1 of primary education, Repetition rate in "
                "Grade 2 of lower secondary general education, Repetition "
                "rate in Grade 2 of primary education, Repetition rate in "
                "Grade 3 of lower secondary general education, Repetition "
                "rate in Grade 3 of primary education, Repetition rate in "
                "Grade 4 of lower secondary general education, Repetition "
                "rate in Grade 4 of primary education, Repetition rate in "
                "Grade 5 of lower secondary general education, Repetition "
                "rate in Grade 5 of primary education, Repetition rate in "
                "Grade 6 of primary education, Repetition rate in Grade 7 of "
                "primary education, Repetition rate in lower secondary "
                "general education, Repetition rate in primary education, "
                "School age population, School life expectancy, Share of all "
                "students in lower secondary education enrolled in general "
                "programmes, Share of all students in lower secondary "
                "education enrolled in vocational programmes, Share of all "
                "students in post-secondary non-tertiary education enrolled "
                "in general programmes, Share of all students in "
                "post-secondary non-tertiary education enrolled in vocational "
                "programmes, Share of all students in secondary education "
                "enrolled in general programmes, Share of all students in "
                "secondary education enrolled in vocational programmes, Share "
                "of all students in upper secondary education enrolled in "
                "general programmes, Share of all students in upper secondary "
                "education enrolled in vocational programmes, South America, "
                "Start month of the academic school year, Start of the "
                "academic school year, Survival rate to Grade 4 of primary "
                "education, Survival rate to Grade 5 of primary education, "
                "Survival rate to the last grade of primary education, "
                "Teachers in early childhood educational development "
                "programmes, Teachers in lower secondary education, Teachers "
                "in post-secondary non-tertiary education, Teachers in "
                "pre-primary education, Teachers in primary education, "
                "Teachers in secondary education, Teachers in tertiary "
                "education ISCED 5 programmes, Teachers in tertiary education "
                "ISCED 6, Teachers in tertiary education programmes, Teachers "
                "in upper secondary education, Teaching staff compensation as "
                "a percentage of total expenditure in lower secondary public "
                "institutions, Teaching staff compensation as a percentage of "
                "total expenditure in post-secondary non-tertiary public "
                "institutions, Teaching staff compensation as a percentage of "
                "total expenditure in pre-primary public institutions, "
                "Teaching staff compensation as a percentage of total "
                "expenditure in primary public institutions, Teaching staff "
                "compensation as a percentage of total expenditure in public "
                "institutions, Teaching staff compensation as a percentage of "
                "total expenditure in secondary public institutions, Teaching "
                "staff compensation as a percentage of total expenditure in "
                "tertiary public institutions, Teaching staff compensation as "
                "a percentage of total expenditure in upper secondary public "
                "institutions, Theoretical duration of early childhood "
                "education, Theoretical duration of early childhood "
                "educational development, Theoretical duration of lower "
                "secondary education, Theoretical duration of post-secondary "
                "non-tertiary education, Theoretical duration of pre-primary "
                "education, Theoretical duration of primary education, "
                "Theoretical duration of secondary education, Theoretical "
                "duration of upper secondary education, Total inbound "
                "internationally mobile students, Total net attendance rate, "
                "Total net enrolment rate, Total outbound internationally "
                "mobile tertiary students studying abroad, Youth illiterate "
                "population",
                "format": "csv",
                "name": "Other Policy Relevant Indicators data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "Other Policy Relevant Indicators indicator list with HXL "
                "tags",
                "format": "csv",
                "name": "Other Policy Relevant Indicators indicator list",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "False",
                "description": "Other Policy Relevant Indicators metadata with HXL tags",
                "format": "csv",
                "name": "Other Policy Relevant Indicators metadata",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
            {
                "dataset_preview_enabled": "True",
                "description": "Cut down data for QuickCharts",
                "format": "csv",
                "name": "QuickCharts-SDG 4 Global and Thematic data",
                "resource_type": "file.upload",
                "url": "updated_by_file_upload_step",
                "url_type": "upload",
            },
        ]
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
        dataset._prepare_hdx_call(dataset.old_data, {})
        results = dataset._revise_dataset(
            tuple(),
            resources_to_update,
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
