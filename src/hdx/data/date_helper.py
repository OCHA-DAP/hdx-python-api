"""Helper to the Dataset and Resource classes for handling HDX dates.
"""
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union

from hdx.utilities.dateparse import now_utc, parse_date


class DateHelper:
    @staticmethod
    def get_date_info(
        hdx_date: Dict,
        date_format: Optional[str] = None,
        today: datetime = now_utc(),
    ) -> Dict:
        """Get date as datetimes and strings in specified format. If no format is
        supplied, the ISO 8601 format is used. Returns a dictionary containing keys
        startdate (start date as datetime), enddate (end date as datetime),
        startdate_str (start date as string), enddate_str (end date as string) and
        ongoing (whether the end date is a rolls forward every day).

        Args:
            hdx_date (str): Input date
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.
            today (datetime): Date to use for today. Defaults to now_utc().

        Returns:
            Dict: Dictionary of date information
        """
        result = dict()
        if hdx_date:
            if hdx_date[0] == "[" and hdx_date[-1] == "]":
                hdx_date = hdx_date[1:-1]
            dataset_dates = hdx_date.split(" TO ")
            startdate = parse_date(dataset_dates[0])
            enddate = dataset_dates[1]
            if enddate == "*":
                enddate = today.replace(
                    hour=23,
                    minute=59,
                    second=59,
                    tzinfo=timezone.utc,
                )
                ongoing = True
            else:
                enddate = parse_date(enddate, max_time=True)
                ongoing = False
            result["startdate"] = startdate
            result["enddate"] = enddate
            if date_format is None:
                startdate_str = startdate.isoformat()
                enddate_str = enddate.isoformat()
            else:
                startdate_str = startdate.strftime(date_format)
                enddate_str = enddate.strftime(date_format)
            result["startdate_str"] = startdate_str
            result["enddate_str"] = enddate_str
            result["ongoing"] = ongoing
        return result

    @staticmethod
    def get_hdx_date(
        startdate: Union[datetime, str],
        enddate: Union[datetime, str, None] = None,
        ongoing: bool = False,
        ignore_timeinfo: bool = True,
    ) -> str:
        """Get an HDX date from either datetime.datetime objects or strings with option
        to set ongoing.

        Args:
            startdate (Union[datetime, str]): Dataset start date
            enddate (Union[datetime, str, None]): Dataset end date. Defaults to None.
            ongoing (bool): True if ongoing, False if not. Defaults to False.
            ignore_timeinfo (bool): Ignore time and time zone of date. Defaults to True.

        Returns:
            str: HDX date
        """
        if ignore_timeinfo:
            timezone_handling = 0
        else:
            timezone_handling = 3

        def get_date_str(dt, max=False):
            if ignore_timeinfo:
                if max:
                    dt = dt.replace(
                        hour=23,
                        minute=59,
                        second=59,
                        microsecond=0,
                        tzinfo=None,
                    )
                else:
                    dt = dt.replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                    )
            else:
                dt = dt.astimezone(timezone.utc).replace(
                    microsecond=0, tzinfo=None
                )
            return dt.isoformat()

        if isinstance(startdate, str):
            startdate = parse_date(
                startdate, timezone_handling=timezone_handling
            )
        startdate = get_date_str(startdate)
        if ongoing:
            enddate = "*"
        else:
            if not enddate:
                enddate = startdate
            else:
                if isinstance(enddate, str):
                    enddate = parse_date(
                        enddate, timezone_handling=timezone_handling
                    )
                enddate = get_date_str(enddate, max=True)
        return f"[{startdate} TO {enddate}]"

    @classmethod
    def get_hdx_date_from_years(
        cls,
        startyear: Union[str, int, Iterable],
        endyear: Union[str, int, None] = None,
    ) -> Tuple[str, List[int]]:
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
            retval = sorted(int(x) for x in list(startyear))
        startyear = retval[0]
        endyear = retval[-1]
        startdate = datetime(startyear, 1, 1, tzinfo=timezone.utc)
        enddate = datetime(endyear, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        return (
            cls.get_hdx_date(startdate, enddate, ignore_timeinfo=False),
            retval,
        )
