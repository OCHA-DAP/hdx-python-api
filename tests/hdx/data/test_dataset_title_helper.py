# -*- coding: UTF-8 -*-
"""Dataset Title Helper Tests"""
from datetime import datetime

from hdx.data.dataset_title_helper import DatasetTitleHelper


class TestDatasetTitleHelper:
    def test_fuzzy_match_dates_in_title(self):
        ranges = list()
        assert DatasetTitleHelper.fuzzy_match_dates_in_title('Myanmar Town July 2019', ranges) == 'Myanmar Town'
        assert ranges == [(datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))]
        ranges = list()
        assert DatasetTitleHelper.fuzzy_match_dates_in_title('Myanmar Town 2019 July', ranges) == 'Myanmar Town'
        assert ranges == [(datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))]

    def test_get_date_from_title(self):
        title = 'Myanmar Town 2019 July'
        assert DatasetTitleHelper.get_date_from_title(title) == \
               ('Myanmar Town', datetime(2019, 7, 1, 0, 0), datetime(2019, 7, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('Formal Sector School Location Upper Myanmar (2019)') == \
               ('Formal Sector School Location Upper Myanmar', datetime(2019, 1, 1, 0, 0), datetime(2019, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('ICA Armenia, 2017 - Drought Risk, 1981-2015') == \
               ('ICA Armenia - Drought Risk', datetime(1981, 1, 1, 0, 0), datetime(2015, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('ICA Sudan, 2018 - Land Degradation, 2001-2013') == \
               ('ICA Sudan - Land Degradation', datetime(2001, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('Central African Republic, Bridges, January 2019') == \
               ('Central African Republic, Bridges', datetime(2019, 1, 1, 0, 0), datetime(2019, 1, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title(
            'Afghanistan:District Accessibility for WFP and Partners Staff as of 05 May 2019') == \
               ('Afghanistan:District Accessibility for WFP and Partners Staff', datetime(2019, 5, 5, 0, 0),
                datetime(2019, 5, 5, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('Tanintharyi Region Land Cover - March 2016 (Original)') == (
            'Tanintharyi Region Land Cover (Original)', datetime(2016, 3, 1, 0, 0), datetime(2016, 3, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title(
            'Kachin State and Sagaing Region 2002-2014 Forest Cover Change') == \
               ('Kachin State and Sagaing Region Forest Cover Change', datetime(2002, 1, 1, 0, 0),
                datetime(2014, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('Ward boundaries Yangon City_mimu_v8_1') == \
               ('Ward boundaries Yangon City_mimu_v8_1', None, None)
        assert DatasetTitleHelper.get_date_from_title('Mon_State_Village_Tract_Boundaries') == \
               ('Mon_State_Village_Tract_Boundaries', None, None)
        assert DatasetTitleHelper.get_date_from_title('ICA Afghanistan, 2019 - Landslide hazard, 2013') == \
               ('ICA Afghanistan - Landslide hazard', datetime(2013, 1, 1, 0, 0), datetime(2013, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title(
            'Afghanistan Percentage of Food Insecure Population Based on Combined Food Consumption Score and Coping Strategy Index by Province - ALCS 2013/14') == \
               (
               'Afghanistan Percentage of Food Insecure Population Based on Combined Food Consumption Score and Coping Strategy Index by Province - ALCS',
               datetime(2013, 1, 1, 0, 0), datetime(2014, 12, 31, 0, 0))
        assert DatasetTitleHelper.get_date_from_title('Mon_State_Village_Tract_Boundaries 9999') == \
               ('Mon_State_Village_Tract_Boundaries 9999', None, None)
        assert DatasetTitleHelper.get_date_from_title('Mon_State_Village_Tract_Boundaries 10/12/01 lala') == \
               ('Mon_State_Village_Tract_Boundaries 10/12/01 lala', None, None)  # It's the Mon that makes an extra
        # date component that causes it to ignore the date (correctly)
        assert DatasetTitleHelper.get_date_from_title('State_Village_Tract_Boundaries 10/12/01 lala') == \
               ('State_Village_Tract_Boundaries lala', datetime(2001, 12, 10, 0, 0), datetime(2001, 12, 10, 0, 0))
