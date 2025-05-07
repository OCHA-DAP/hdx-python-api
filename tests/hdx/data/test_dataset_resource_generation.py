"""Dataset Tests (noncore methods)"""

from datetime import datetime, timezone
from os.path import join

import pytest

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir


class TestDatasetResourceGeneration:
    url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/main/tests/fixtures/test_data.csv"
    hxltags = {
        "EVENT_ID_CNTY": "#event+code",
        "EVENT_DATE": "#date+occurred",
        "YEAR": "#date+year",
        "EVENT_TYPE": "#event+type",
        "ACTOR1": "#group+name+first",
        "ASSOC_ACTOR_1": "#group+name+first+assoc",
        "ACTOR2": "#group+name+second",
        "ASSOC_ACTOR_2": "#group+name+second+assoc",
        "REGION": "#region+name",
        "COUNTRY": "#country+name",
        "ADMIN1": "#adm1+name",
        "ADMIN2": "#adm2+name",
        "ADMIN3": "#adm3+name",
        "LOCATION": "#loc+name",
        "LATITUDE": "#geo+lat",
        "LONGITUDE": "#geo+lon",
        "SOURCE": "#meta+source",
        "NOTES": "#description",
        "FATALITIES": "#affected+killed",
        "ISO3": "#country+code",
    }

    def test_generate_qc_resource_from_rows(self, configuration):
        with temp_dir("test") as folder:
            with Download(user_agent="test") as downloader:
                _, rows = downloader.get_tabular_rows(
                    TestDatasetResourceGeneration.url,
                    dict_form=True,
                    format="csv",
                )
                rows = list(rows)
                dataset = Dataset({"name": "test"})
                qc_filename = "qc_conflict_data_alg.csv"
                resourcedata = {
                    "name": "Conflict Data for Algeria",
                    "description": "Conflict data with HXL tags",
                }
                columnname = "EVENT_ID_CNTY"
                qc_indicator_codes = ["1416RTA", "XXXXRTA", "2231RTA"]
                resource = dataset.generate_qc_resource_from_rows(
                    folder,
                    qc_filename,
                    rows,
                    resourcedata,
                    TestDatasetResourceGeneration.hxltags,
                    columnname,
                    qc_indicator_codes,
                )
                assert resource == {
                    "name": "Conflict Data for Algeria",
                    "description": "Conflict data with HXL tags",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                assert_files_same(
                    join("tests", "fixtures", "qc_from_rows", qc_filename),
                    join(folder, qc_filename),
                )
                qc_filename = "qc_conflict_data_alg_one_col.csv"
                dataset.generate_qc_resource_from_rows(
                    folder,
                    qc_filename,
                    rows,
                    resourcedata,
                    TestDatasetResourceGeneration.hxltags,
                    columnname,
                    qc_indicator_codes,
                    headers=[columnname],
                )
                assert_files_same(
                    join("tests", "fixtures", "qc_from_rows", qc_filename),
                    join(folder, qc_filename),
                )
                rows = []
                resource = dataset.generate_qc_resource_from_rows(
                    folder,
                    qc_filename,
                    rows,
                    resourcedata,
                    TestDatasetResourceGeneration.hxltags,
                    columnname,
                    qc_indicator_codes,
                )
                assert resource is None

    def test_download_and_generate_resource(self, configuration):
        with temp_dir("test") as folder:
            filename = "conflict_data_alg.csv"
            resourcedata = {
                "name": "Conflict Data for Algeria",
                "description": "Conflict data with HXL tags",
            }
            admin1s = set()

            def process_row(headers, row):
                row["lala"] = "lala"
                admin1 = row.get("ADMIN1")
                if admin1 is not None:
                    admin1s.add(admin1)
                return row

            dataset = Dataset()
            with Download(user_agent="test") as downloader:
                quickcharts = {
                    "hashtag": "#event+code",
                    "values": ["1416RTA", "XXXXRTA", "2231RTA"],
                    "cutdown": 2,
                }
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    TestDatasetResourceGeneration.url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    yearcol="YEAR",
                    quickcharts=quickcharts,
                )
                assert success is True
                assert results == {
                    "startdate": datetime(2001, 1, 1, 0, 0, tzinfo=timezone.utc),
                    "enddate": datetime(2002, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
                    "bites_disabled": [False, True, False],
                    "resource": {
                        "description": "Conflict data with HXL tags",
                        "format": "csv",
                        "name": "Conflict Data for Algeria",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    "headers": [
                        "lala",
                        "GWNO",
                        "EVENT_ID_CNTY",
                        "EVENT_ID_NO_CNTY",
                        "EVENT_DATE",
                        "YEAR",
                        "TIME_PRECISION",
                        "EVENT_TYPE",
                        "ACTOR1",
                        "ALLY_ACTOR_1",
                        "INTER1",
                        "ACTOR2",
                        "ALLY_ACTOR_2",
                        "INTER2",
                        "INTERACTION",
                        "COUNTRY",
                        "ADMIN1",
                        "ADMIN2",
                        "ADMIN3",
                        "LOCATION",
                        "LATITUDE",
                        "LONGITUDE",
                        "GEO_PRECISION",
                        "SOURCE",
                        "NOTES",
                        "FATALITIES",
                    ],
                    "rows": [
                        {
                            "lala": "",
                            "GWNO": "",
                            "EVENT_ID_CNTY": "#event+code",
                            "EVENT_ID_NO_CNTY": "",
                            "EVENT_DATE": "#date+occurred",
                            "YEAR": "#date+year",
                            "TIME_PRECISION": "",
                            "EVENT_TYPE": "#event+type",
                            "ACTOR1": "#group+name+first",
                            "ALLY_ACTOR_1": "",
                            "INTER1": "",
                            "ACTOR2": "#group+name+second",
                            "ALLY_ACTOR_2": "",
                            "INTER2": "",
                            "INTERACTION": "",
                            "COUNTRY": "#country+name",
                            "ADMIN1": "#adm1+name",
                            "ADMIN2": "#adm2+name",
                            "ADMIN3": "#adm3+name",
                            "LOCATION": "#loc+name",
                            "LATITUDE": "#geo+lat",
                            "LONGITUDE": "#geo+lon",
                            "GEO_PRECISION": "",
                            "SOURCE": "#meta+source",
                            "NOTES": "#description",
                            "FATALITIES": "#affected+killed",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "1416RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "18/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Violence against civilians",
                            "ACTOR1": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_1": None,
                            "INTER1": "1",
                            "ACTOR2": "Civilians (Algeria)",
                            "ALLY_ACTOR_2": "Berber Ethnic Group (Algeria)",
                            "INTER2": "7",
                            "INTERACTION": "17",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Tizi Ouzou",
                            "ADMIN2": "Beni-Douala",
                            "ADMIN3": None,
                            "LOCATION": "Beni Douala",
                            "LATITUDE": "36.61954",
                            "LONGITUDE": "4.08282",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Associated Press Online",
                            "NOTES": "A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.",
                            "FATALITIES": "1",
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2229RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "19/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Rioters (Algeria)",
                            "ALLY_ACTOR_1": "Berber Ethnic Group (Algeria)",
                            "INTER1": "5",
                            "ACTOR2": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_2": None,
                            "INTER2": "1",
                            "INTERACTION": "15",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Tizi Ouzou",
                            "ADMIN2": "Tizi Ouzou",
                            "ADMIN3": None,
                            "LOCATION": "Tizi Ouzou",
                            "LATITUDE": "36.71183",
                            "LONGITUDE": "4.04591",
                            "GEO_PRECISION": "3",
                            "SOURCE": "Kabylie report",
                            "NOTES": "Riots were reported in numerous villages in Kabylie, resulting in dozens wounded in clashes between protesters and police and significant material damage.",
                            "FATALITIES": "0",
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2230RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "20/04/2001",
                            "YEAR": "2002",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Protesters (Algeria)",
                            "ALLY_ACTOR_1": "Students (Algeria)",
                            "INTER1": "6",
                            "ACTOR2": None,
                            "ALLY_ACTOR_2": None,
                            "INTER2": "0",
                            "INTERACTION": "60",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Bejaia",
                            "ADMIN2": "Amizour",
                            "ADMIN3": None,
                            "LOCATION": "Amizour",
                            "LATITUDE": "36.64022",
                            "LONGITUDE": "4.90131",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Crisis Group",
                            "NOTES": "Students protested in the Amizour area. At least 3 were later arrested for allegedly insulting gendarmes.",
                            "FATALITIES": None,
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2231RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "21/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Rioters (Algeria)",
                            "ALLY_ACTOR_1": "Berber Ethnic Group (Algeria)",
                            "INTER1": "5",
                            "ACTOR2": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_2": None,
                            "INTER2": "1",
                            "INTERACTION": "15",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Bejaia",
                            "ADMIN2": "Amizour",
                            "ADMIN3": None,
                            "LOCATION": "Amizour",
                            "LATITUDE": "36.64022",
                            "LONGITUDE": "4.90131",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Kabylie report",
                            "NOTES": "Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.",
                            "FATALITIES": "0",
                            "lala": "lala",
                        },
                    ],
                    "qc_resource": {
                        "description": "Cut down data for QuickCharts",
                        "format": "csv",
                        "name": "QuickCharts-Conflict Data for Algeria",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    "qcheaders": [
                        "EVENT_ID_CNTY",
                        "EVENT_DATE",
                        "YEAR",
                        "EVENT_TYPE",
                        "ACTOR1",
                        "ACTOR2",
                        "COUNTRY",
                        "ADMIN1",
                        "ADMIN2",
                        "ADMIN3",
                        "LOCATION",
                        "LATITUDE",
                        "LONGITUDE",
                        "SOURCE",
                        "NOTES",
                        "FATALITIES",
                    ],
                    "qcrows": [
                        {
                            "EVENT_ID_CNTY": "#event+code",
                            "EVENT_DATE": "#date+occurred",
                            "YEAR": "#date+year",
                            "EVENT_TYPE": "#event+type",
                            "ACTOR1": "#group+name+first",
                            "ACTOR2": "#group+name+second",
                            "COUNTRY": "#country+name",
                            "ADMIN1": "#adm1+name",
                            "ADMIN2": "#adm2+name",
                            "ADMIN3": "#adm3+name",
                            "LOCATION": "#loc+name",
                            "LATITUDE": "#geo+lat",
                            "LONGITUDE": "#geo+lon",
                            "SOURCE": "#meta+source",
                            "NOTES": "#description",
                            "FATALITIES": "#affected+killed",
                        },
                        {
                            "EVENT_ID_CNTY": "1416RTA",
                            "EVENT_DATE": "18/04/2001",
                            "YEAR": "2001",
                            "EVENT_TYPE": "Violence against civilians",
                            "ACTOR1": "Police Forces of Algeria (1999-)",
                            "ACTOR2": "Civilians (Algeria)",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Tizi Ouzou",
                            "ADMIN2": "Beni-Douala",
                            "ADMIN3": None,
                            "LOCATION": "Beni Douala",
                            "LATITUDE": "36.61954",
                            "LONGITUDE": "4.08282",
                            "SOURCE": "Associated Press Online",
                            "NOTES": "A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.",
                            "FATALITIES": "1",
                        },
                        {
                            "EVENT_ID_CNTY": "2231RTA",
                            "EVENT_DATE": "21/04/2001",
                            "YEAR": "2001",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Rioters (Algeria)",
                            "ACTOR2": "Police Forces of Algeria (1999-)",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Bejaia",
                            "ADMIN2": "Amizour",
                            "ADMIN3": None,
                            "LOCATION": "Amizour",
                            "LATITUDE": "36.64022",
                            "LONGITUDE": "4.90131",
                            "SOURCE": "Kabylie report",
                            "NOTES": "Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.",
                            "FATALITIES": "0",
                        },
                    ],
                }
                assert (
                    dataset["dataset_date"]
                    == "[2001-01-01T00:00:00 TO 2002-12-31T23:59:59]"
                )
                assert admin1s == {"Bejaia", "Tizi Ouzou"}
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "Conflict Data for Algeria",
                        "description": "Conflict data with HXL tags",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "name": "QuickCharts-Conflict Data for Algeria",
                        "description": "Cut down data for QuickCharts",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                assert_files_same(
                    join("tests", "fixtures", "gen_resource", filename),
                    join(folder, filename),
                )
                qc_filename = f"qc_{filename}"
                assert_files_same(
                    join("tests", "fixtures", "gen_resource", qc_filename),
                    join(folder, qc_filename),
                )

                success, results = dataset.download_and_generate_resource(
                    downloader,
                    TestDatasetResourceGeneration.url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    datecol="EVENT_DATE",
                    quickcharts=quickcharts,
                )
                assert success is True
                assert (
                    dataset["dataset_date"]
                    == "[2001-04-18T00:00:00 TO 2001-04-21T23:59:59]"
                )

                quickcharts = {
                    "hashtag": "#event+code",
                    "values": ["1416RTA", "2230RTA", "2231RTA"],
                    "numeric_hashtag": "#affected+killed",
                    "cutdown": 2,
                    "cutdownhashtags": ["#event+code"],
                }
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    TestDatasetResourceGeneration.url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    yearcol="YEAR",
                    quickcharts=quickcharts,
                )
                assert success is True
                assert results == {
                    "startdate": datetime(2001, 1, 1, 0, 0, tzinfo=timezone.utc),
                    "enddate": datetime(2002, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
                    "bites_disabled": [False, True, False],
                    "resource": {
                        "description": "Conflict data with HXL tags",
                        "format": "csv",
                        "name": "Conflict Data for Algeria",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    "headers": [
                        "lala",
                        "GWNO",
                        "EVENT_ID_CNTY",
                        "EVENT_ID_NO_CNTY",
                        "EVENT_DATE",
                        "YEAR",
                        "TIME_PRECISION",
                        "EVENT_TYPE",
                        "ACTOR1",
                        "ALLY_ACTOR_1",
                        "INTER1",
                        "ACTOR2",
                        "ALLY_ACTOR_2",
                        "INTER2",
                        "INTERACTION",
                        "COUNTRY",
                        "ADMIN1",
                        "ADMIN2",
                        "ADMIN3",
                        "LOCATION",
                        "LATITUDE",
                        "LONGITUDE",
                        "GEO_PRECISION",
                        "SOURCE",
                        "NOTES",
                        "FATALITIES",
                    ],
                    "rows": [
                        {
                            "lala": "",
                            "GWNO": "",
                            "EVENT_ID_CNTY": "#event+code",
                            "EVENT_ID_NO_CNTY": "",
                            "EVENT_DATE": "#date+occurred",
                            "YEAR": "#date+year",
                            "TIME_PRECISION": "",
                            "EVENT_TYPE": "#event+type",
                            "ACTOR1": "#group+name+first",
                            "ALLY_ACTOR_1": "",
                            "INTER1": "",
                            "ACTOR2": "#group+name+second",
                            "ALLY_ACTOR_2": "",
                            "INTER2": "",
                            "INTERACTION": "",
                            "COUNTRY": "#country+name",
                            "ADMIN1": "#adm1+name",
                            "ADMIN2": "#adm2+name",
                            "ADMIN3": "#adm3+name",
                            "LOCATION": "#loc+name",
                            "LATITUDE": "#geo+lat",
                            "LONGITUDE": "#geo+lon",
                            "GEO_PRECISION": "",
                            "SOURCE": "#meta+source",
                            "NOTES": "#description",
                            "FATALITIES": "#affected+killed",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "1416RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "18/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Violence against civilians",
                            "ACTOR1": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_1": None,
                            "INTER1": "1",
                            "ACTOR2": "Civilians (Algeria)",
                            "ALLY_ACTOR_2": "Berber Ethnic Group (Algeria)",
                            "INTER2": "7",
                            "INTERACTION": "17",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Tizi Ouzou",
                            "ADMIN2": "Beni-Douala",
                            "ADMIN3": None,
                            "LOCATION": "Beni Douala",
                            "LATITUDE": "36.61954",
                            "LONGITUDE": "4.08282",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Associated Press Online",
                            "NOTES": "A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.",
                            "FATALITIES": "1",
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2229RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "19/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Rioters (Algeria)",
                            "ALLY_ACTOR_1": "Berber Ethnic Group (Algeria)",
                            "INTER1": "5",
                            "ACTOR2": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_2": None,
                            "INTER2": "1",
                            "INTERACTION": "15",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Tizi Ouzou",
                            "ADMIN2": "Tizi Ouzou",
                            "ADMIN3": None,
                            "LOCATION": "Tizi Ouzou",
                            "LATITUDE": "36.71183",
                            "LONGITUDE": "4.04591",
                            "GEO_PRECISION": "3",
                            "SOURCE": "Kabylie report",
                            "NOTES": "Riots were reported in numerous villages in Kabylie, resulting in dozens wounded in clashes between protesters and police and significant material damage.",
                            "FATALITIES": "0",
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2230RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "20/04/2001",
                            "YEAR": "2002",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Protesters (Algeria)",
                            "ALLY_ACTOR_1": "Students (Algeria)",
                            "INTER1": "6",
                            "ACTOR2": None,
                            "ALLY_ACTOR_2": None,
                            "INTER2": "0",
                            "INTERACTION": "60",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Bejaia",
                            "ADMIN2": "Amizour",
                            "ADMIN3": None,
                            "LOCATION": "Amizour",
                            "LATITUDE": "36.64022",
                            "LONGITUDE": "4.90131",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Crisis Group",
                            "NOTES": "Students protested in the Amizour area. At least 3 were later arrested for allegedly insulting gendarmes.",
                            "FATALITIES": None,
                            "lala": "lala",
                        },
                        {
                            "GWNO": "615",
                            "EVENT_ID_CNTY": "2231RTA",
                            "EVENT_ID_NO_CNTY": None,
                            "EVENT_DATE": "21/04/2001",
                            "YEAR": "2001",
                            "TIME_PRECISION": "1",
                            "EVENT_TYPE": "Riots/Protests",
                            "ACTOR1": "Rioters (Algeria)",
                            "ALLY_ACTOR_1": "Berber Ethnic Group (Algeria)",
                            "INTER1": "5",
                            "ACTOR2": "Police Forces of Algeria (1999-)",
                            "ALLY_ACTOR_2": None,
                            "INTER2": "1",
                            "INTERACTION": "15",
                            "COUNTRY": "Algeria",
                            "ADMIN1": "Bejaia",
                            "ADMIN2": "Amizour",
                            "ADMIN3": None,
                            "LOCATION": "Amizour",
                            "LATITUDE": "36.64022",
                            "LONGITUDE": "4.90131",
                            "GEO_PRECISION": "1",
                            "SOURCE": "Kabylie report",
                            "NOTES": "Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.",
                            "FATALITIES": "0",
                            "lala": "lala",
                        },
                    ],
                    "qc_resource": {
                        "description": "Cut down data for QuickCharts",
                        "format": "csv",
                        "name": "QuickCharts-Conflict Data for Algeria",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    "qcheaders": ["EVENT_ID_CNTY", "FATALITIES"],
                    "qcrows": [
                        {
                            "EVENT_ID_CNTY": "#event+code",
                            "FATALITIES": "#affected+killed",
                        },
                        {"EVENT_ID_CNTY": "1416RTA", "FATALITIES": "1"},
                        {"EVENT_ID_CNTY": "2231RTA", "FATALITIES": "0"},
                    ],
                }

                def process_year(row):
                    year = row["YEAR"]
                    if year == "2002":
                        return None
                    startdate, enddate = parse_date_range(
                        year, zero_time=True, max_endtime=True
                    )
                    return {"startdate": startdate, "enddate": enddate}

                del quickcharts["hashtag"]
                del quickcharts["numeric_hashtag"]
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    TestDatasetResourceGeneration.url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    date_function=process_year,
                    quickcharts=quickcharts,
                )
                assert success is True
                assert results["startdate"] == datetime(
                    2001, 1, 1, 0, 0, tzinfo=timezone.utc
                )
                assert results["enddate"] == datetime(
                    2001, 12, 31, 23, 59, 59, tzinfo=timezone.utc
                )
                assert (
                    dataset["dataset_date"]
                    == "[2001-01-01T00:00:00 TO 2001-12-31T23:59:59]"
                )
                assert_files_same(
                    join(
                        "tests",
                        "fixtures",
                        "gen_resource",
                        f"min_{qc_filename}",
                    ),
                    join(folder, qc_filename),
                )

                with pytest.raises(HDXError):
                    dataset.download_and_generate_resource(
                        downloader,
                        TestDatasetResourceGeneration.url,
                        TestDatasetResourceGeneration.hxltags,
                        folder,
                        filename,
                        resourcedata,
                        yearcol="YEAR",
                        date_function=process_year,
                    )
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    TestDatasetResourceGeneration.url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                )
                assert success is True
                url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/main/tests/fixtures/empty.csv"
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    yearcol="YEAR",
                )
                assert success is False
                url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/main/tests/fixtures/gen_resource/test_data_no_data.csv"
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    quickcharts=quickcharts,
                )
                assert success is False
                url = "https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/main/tests/fixtures/gen_resource/test_data_no_years.csv"
                success, results = dataset.download_and_generate_resource(
                    downloader,
                    url,
                    TestDatasetResourceGeneration.hxltags,
                    folder,
                    filename,
                    resourcedata,
                    header_insertions=[(0, "lala")],
                    row_function=process_row,
                    yearcol="YEAR",
                )
                assert success is False
