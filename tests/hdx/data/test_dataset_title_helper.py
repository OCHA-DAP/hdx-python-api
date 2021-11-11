"""Dataset Title Helper Tests"""
from datetime import datetime

from hdx.data.dataset_title_helper import DatasetTitleHelper


class TestDatasetTitleHelper:
    def test_fuzzy_match_dates_in_title(self):
        ignore_wrong_years = list()
        ranges = list()
        assert (
            DatasetTitleHelper.fuzzy_match_dates_in_title(
                "Myanmar Town July 2019", ranges, ignore_wrong_years
            )
            == "Myanmar Town"
        )
        assert ranges == [
            (datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))
        ]
        ranges = list()
        assert (
            DatasetTitleHelper.fuzzy_match_dates_in_title(
                "Myanmar Town 2019 July", ranges, ignore_wrong_years
            )
            == "Myanmar Town"
        )
        assert ranges == [
            (datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))
        ]

    def test_get_date_from_title(self):
        assert DatasetTitleHelper.get_dates_from_title(
            "Myanmar Self Administered Regions Boundaries MIMU v9.2.1"
        ) == (
            "Myanmar Self Administered Regions Boundaries MIMU v9.2.1",
            list(),
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Myanmar Town 2019 July"
        ) == (
            "Myanmar Town",
            [(datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Formal Sector School Location Upper Myanmar (  2019   )"
        ) == (
            "Formal Sector School Location Upper Myanmar",
            [(datetime(2019, 1, 1, 0, 0), datetime(2019, 12, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "ICA Armenia, 2017 - Drought Risk, 1981-2015"
        ) == (
            "ICA Armenia - Drought Risk",
            [
                (datetime(1981, 1, 1, 0, 0), datetime(2015, 12, 31, 0, 0)),
                (datetime(2017, 1, 1, 0, 0), datetime(2017, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "ICA Sudan, 2018 - Land Degradation, 2001-2013"
        ) == (
            "ICA Sudan - Land Degradation",
            [
                (datetime(2001, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0)),
                (datetime(2018, 1, 1, 0, 0), datetime(2018, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Central African Republic, Bridges, January 2019"
        ) == (
            "Central African Republic, Bridges",
            [(datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Afghanistan:District Accessibility for WFP and Partners Staff as of 05 May 2019"
        ) == (
            "Afghanistan:District Accessibility for WFP and Partners Staff",
            [(datetime(2019, 5, 5, 0, 0), datetime(2019, 5, 5, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Tanintharyi Region Land Cover - March 2016 (Original)"
        ) == (
            "Tanintharyi Region Land Cover (Original)",
            [(datetime(2016, 3, 1, 0, 0), datetime(2016, 3, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Kachin State and Sagaing Region 2002-2014 Forest Cover Change"
        ) == (
            "Kachin State and Sagaing Region Forest Cover Change",
            [(datetime(2002, 1, 1, 0, 0), datetime(2014, 12, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Ward boundaries Yangon City_mimu_v8_1"
        ) == ("Ward boundaries Yangon City_mimu_v8_1", list())
        assert DatasetTitleHelper.get_dates_from_title(
            "Mon_State_Village_Tract_Boundaries"
        ) == ("Mon_State_Village_Tract_Boundaries", list())
        assert DatasetTitleHelper.get_dates_from_title(
            "ICA Afghanistan, 2019 - Landslide hazard, 2013"
        ) == (
            "ICA Afghanistan - Landslide hazard",
            [
                (datetime(2013, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0)),
                (datetime(2019, 1, 1, 0, 0), datetime(2019, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Afghanistan Percentage of Food Insecure Population Based on Combined Food Consumption Score and Coping Strategy Index by Province - ALCS 2013/14"
        ) == (
            "Afghanistan Percentage of Food Insecure Population Based on Combined Food Consumption Score and Coping Strategy Index by Province - ALCS",
            [(datetime(2013, 1, 1, 0, 0), datetime(2014, 12, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title("ALCS 2000/10") == (
            "ALCS",
            [(datetime(2000, 10, 1, 0, 0), datetime(2000, 10, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "ALCS 2014/13"
        ) == (  # not a month and range going down
            "ALCS 2014/13",
            list(),
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Mon_State_Village_Tract_Boundaries 9999"
        ) == ("Mon_State_Village_Tract_Boundaries 9999", list())
        assert DatasetTitleHelper.get_dates_from_title(
            "Mon_State_Village_Tract_Boundaries 10/12/01 lala"
        ) == ("Mon_State_Village_Tract_Boundaries 10/12/01 lala", list())
        # It's the "Mon" that makes an extra date component that causes it to ignore the date (correctly)
        assert DatasetTitleHelper.get_dates_from_title(
            "State_Village_Tract_Boundaries 10/12/01 lala"
        ) == (
            "State_Village_Tract_Boundaries lala",
            [(datetime(2001, 12, 10, 0, 0), datetime(2001, 12, 10, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Crops production (2016) - Tajikistan Vulnerability & Resilience Atlas, 2019"
        ) == (
            "Crops production - Tajikistan Vulnerability & Resilience Atlas",
            [
                (datetime(2016, 1, 1, 0, 0), datetime(2016, 12, 31, 0, 0)),
                (datetime(2019, 1, 1, 0, 0), datetime(2019, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Location of partners as of Feb. 5, 2019"
        ) == (
            "Location of partners",
            [(datetime(2019, 2, 5, 0, 0), datetime(2019, 2, 5, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "ICA Armenia, 2016 & 2017 - Land Degradation, 2001-2012"
        ) == (
            "ICA Armenia - Land Degradation",
            [
                (datetime(2001, 1, 1, 0, 0), datetime(2012, 12, 31, 0, 0)),
                (datetime(2016, 1, 1, 0, 0), datetime(2017, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "ICA Afghanistan, 2016 - Food Insecurity Risk, 2007/08-2014"
        ) == (
            "ICA Afghanistan - Food Insecurity Risk",
            [
                (datetime(2007, 8, 1, 0, 0), datetime(2014, 12, 31, 0, 0)),
                (datetime(2016, 1, 1, 0, 0), datetime(2016, 12, 31, 0, 0)),
            ],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Risk, 2020/19-2014"
        ) == (
            "Risk, 2020/19",
            [(datetime(2014, 1, 1, 0, 0), datetime(2014, 12, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "south sudan access constraints shp for 20200124"
        ) == (
            "south sudan access constraints shp",
            [(datetime(2020, 1, 24, 0, 0), datetime(2020, 1, 24, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Cambodia Flood Extent in 2011"
        ) == (
            "Cambodia Flood Extent",
            [(datetime(2011, 1, 1, 0, 0), datetime(2011, 12, 31, 0, 0))],
        )
        assert DatasetTitleHelper.get_dates_from_title(
            "Access: Proportion of the population consuming less than 2100 kcal per day (average of 2011-2013), National Statistics Committee 2013"
        ) == (
            "Access: Proportion of the population consuming less than 2100 kcal per day (average), National Statistics Committee",
            [
                (datetime(2011, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0)),
                (datetime(2013, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0)),
            ],
        )
