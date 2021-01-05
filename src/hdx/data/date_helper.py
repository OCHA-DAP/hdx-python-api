# -*- coding: utf-8 -*-
"""Helper to the Dataset and Resource classes for handling HDX dates.
"""
from collections import Iterable
from datetime import date, datetime
from typing import Union, Dict, Optional, List, Tuple

from hdx.utilities.dateparse import parse_date


class DateHelper(object):
    @staticmethod
    def get_date_info(hdx_date, date_format=None, today=datetime.now()):
        # type: (Dict, Optional[str], date) -> Dict
        """Get date as datetimes and strings in specified format. If no format is supplied, the ISO 8601
        format is used. Returns a dictionary containing keys startdate (start date as datetime), enddate (end
        date as datetime), startdate_str (start date as string), enddate_str (end date as string) and ongoing
        (whether the end date is a rolls forward every day).

        Args:
            hdx_date (str): Input date
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.
            today (date): Date to use for today. Defaults to datetime.now.

        Returns:
            Dict: Dictionary of date information
        """
        result = dict()
        if hdx_date:
            if hdx_date[0] == '[' and hdx_date[-1] == ']':
                hdx_date = hdx_date[1:-1]
            dataset_dates = hdx_date.split(' TO ')
            startdate = parse_date(dataset_dates[0])
            enddate = dataset_dates[1]
            if enddate == '*':
                enddate = datetime(today.year, today.month, today.day, 0, 0)
                ongoing = True
            else:
                enddate = parse_date(enddate)
                ongoing = False
            result['startdate'] = startdate
            result['enddate'] = enddate
            if date_format is None:
                startdate_str = startdate.isoformat()
                enddate_str = enddate.isoformat()
            else:
                startdate_str = startdate.strftime(date_format)
                enddate_str = enddate.strftime(date_format)
            result['startdate_str'] = startdate_str
            result['enddate_str'] = enddate_str
            result['ongoing'] = ongoing
        return result

    @staticmethod
    def get_hdx_date(startdate, enddate=None, ongoing=False):
        # type: (Union[datetime, str], Union[datetime, str, None], bool) -> str
        """Get an HDX date from either datetime.datetime objects or strings with option to set ongoing.

        Args:
            startdate (Union[datetime, str]): Dataset start date
            enddate (Union[datetime, str, None]): Dataset end date. Defaults to None.
            ongoing (bool): True if ongoing, False if not. Defaults to False.

        Returns:
            str: HDX date
        """
        if isinstance(startdate, str):
            startdate = parse_date(startdate)
        startdate = startdate.isoformat()
        if ongoing:
            enddate = '*'
        elif not enddate:
            enddate = startdate
        if enddate != '*':
            if isinstance(enddate, str):
                enddate = parse_date(enddate)
            enddate = enddate.isoformat()
        return '[%s TO %s]' % (startdate, enddate)

    @classmethod
    def get_hdx_date_from_years(cls, startyear, endyear=None):
        # type: (Union[str, int, Iterable], Union[str, int, None]) -> Tuple[str,List[int]]
        """Get an HDX date from an iterable of years or a start and end year.

        Args:
            startyear (Union[str, int, Iterable]): Start year given as string or int or range in an iterable
            endyear (Union[str, int, None]): End year given as string or int

        Returns:
            Tuple[str,List[int]]: (HDX date, the start and end year if supplied or sorted list of years)
        """
        retval = list()
        if isinstance(startyear, str):
            startyear = int(startyear)
        if not isinstance(startyear, Iterable):
            retval.append(startyear)
            if endyear is not None:
                if isinstance(endyear, str):
                    endyear = int(endyear)
                retval.append(endyear)
        else:
            retval = sorted([int(x) for x in list(startyear)])
        startyear = retval[0]
        endyear = retval[-1]
        startdate = datetime(startyear, 1, 1)
        enddate = datetime(endyear, 12, 31)
        return cls.get_hdx_date(startdate, enddate), retval

