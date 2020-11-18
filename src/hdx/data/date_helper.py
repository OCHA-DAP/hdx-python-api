# -*- coding: utf-8 -*-
"""Helper to the Dataset and Resource classes for handling HDX dates.
"""
from datetime import date, datetime

from hdx.utilities.dateparse import parse_date


class DateHelper(object):
    @staticmethod
    def get_date_info(hdx_date, date_format=None, today=date.today()):
        # type: (Dict, Optional[str], datetime.date) -> Dict
        """Get date as datetimes and strings in specified format. If no format is supplied, the ISO 8601
        format is used. Returns a dictionary containing keys startdate (start date as datetime), enddate (end
        date as datetime), startdate_str (start date as string), enddate_str (end date as string) and ongoing
        (whether the end date is a rolls forward every day).

        Args:
            hdx_date (str): Input date
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.
            today (datetime.date): Date to use for today. Defaults to date.today.

        Returns:
            Dict: Dictionary of date information
        """
        result = dict()
        if hdx_date:
            dataset_dates = hdx_date[1:-1].split(' TO ')
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
    def get_hdx_date(startdate, enddate):
        # type: (Union[datetime.datetime, str], Union[datetime.datetime, str]) -> str
        """Get an HDX date from either datetime.datetime objects or strings.

        Args:
            startdate (Union[datetime.datetime, str]): Dataset start date
            enddate (Union[datetime.datetime, str]): Dataset end date

        Returns:
            str: HDX date
        """
        if isinstance(startdate, str):
            startdate = parse_date(startdate)
        startdate = startdate.isoformat()
        if enddate != '*':
            if isinstance(enddate, str):
                enddate = parse_date(enddate)
            enddate = enddate.isoformat()
        return '[%s TO %s]' % (startdate, enddate)
