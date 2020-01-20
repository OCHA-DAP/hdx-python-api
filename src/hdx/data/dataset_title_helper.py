# -*- coding: utf-8 -*-
"""Helper to the Dataset class for handling processing dataset titles.
"""
import logging
import re
from datetime import datetime, timedelta
from parser import ParserError
from string import punctuation, whitespace
from typing import List, Tuple, Optional

from hdx.utilities.dateparse import parse_date_range, parse_date
from hdx.utilities.text import remove_end_characters, remove_from_end, PUNCTUATION_MINUS_BRACKETS, remove_string

logger = logging.getLogger(__name__)


class DatasetTitleHelper(object):
    YEAR_RANGE_PATTERN = re.compile('([12]\d\d\d)(-| % | and )([12]\d\d\d)')
    YEAR_RANGE_PATTERN2 = re.compile('([12]\d\d\d)(/|-)(\d\d)')
    YEAR_PATTERN = re.compile('([12]\d\d\d)')
    PUNCTUATION_PATTERN = re.compile('[%s]' % punctuation)

    @classmethod
    def fuzzy_match_dates_in_title(cls, title, ranges):
        # type: (str, List[Tuple[datetime,datetime]]) -> str
        """
        Fuzzy match dates in title appending to ranges

        Args:
            title (str): Title to parse
            ranges (List[Tuple[datetime,datetime]]): List of date ranges found so far

        Returns:
            str: Title with dates removed

        """
        match = cls.YEAR_PATTERN.search(title)
        while match:
            start = match.start()
            end = match.end()
            stringlr = title[start - 13:end]
            stringlr = cls.PUNCTUATION_PATTERN.split(stringlr)[-1]
            fuzzylr = dict()
            startdatelr = None
            enddatelr = None
            deltalr = timedelta(days=1000)
            try:
                startdatelr, enddatelr = parse_date_range(stringlr, fuzzy=fuzzylr)
                if startdatelr and enddatelr:
                    deltalr = enddatelr - startdatelr
            except ParserError:
                pass
            fuzzyrl = dict()
            stringrl = title[start:end + 13]
            stringrl = cls.PUNCTUATION_PATTERN.split(stringrl)[0]
            startdaterl = None
            enddaterl = None
            deltarl = timedelta(days=1000)
            try:
                startdaterl, enddaterl = parse_date_range(stringrl, fuzzy=fuzzyrl)
                if startdaterl and enddaterl:
                    deltarl = enddaterl - startdaterl
            except ParserError:
                pass
            if startdatelr and deltalr <= deltarl:
                date_components = fuzzylr['date']
                ranges.append((startdatelr, enddatelr))
            elif startdaterl:
                date_components = fuzzyrl['date']
                ranges.append((startdaterl, enddaterl))
            else:
                year = match.group(0)
                date_components = (year)
                ranges.append(parse_date_range(year))
            newtitle = title
            for date_component in date_components:
                newtitle = remove_string(newtitle, date_component, PUNCTUATION_MINUS_BRACKETS)
            logger.info('Removing date from title: %s -> %s' % (title, newtitle))
            title = newtitle
            match = cls.YEAR_PATTERN.search(title, end)
        try:
            fuzzy = dict()
            startdate, enddate = parse_date_range(title, fuzzy=fuzzy)
            if startdate == enddate and len(fuzzy['date']) == 1:  # only accept dates where day, month and year are
                # all together not split throughout the string and where the date is a precise day not a range
                ranges.append((startdate, enddate))
                date_component = fuzzy['date'][0]
                newtitle = remove_string(title, date_component, PUNCTUATION_MINUS_BRACKETS)
                logger.info('Removing date from title: %s -> %s' % (title, newtitle))
                title = newtitle
        except (ParserError, OverflowError):
            pass

        return title

    @classmethod
    def get_date_from_title(cls, title):
        # type: (str) -> Tuple[str,Optional[datetime],Optional[datetime]]
        """
        Get dataset date from title and clean title of dates

        Args:
            title (str): Title to get date from and clean

        Returns:
            Tuple[str,Optional[datetime],Optional[datetime]]: Cleaned title, start and end dates

        """
        ranges = list()
        for match in cls.YEAR_RANGE_PATTERN.finditer(title):
            startdate = parse_date('%s-01-01' % match.group(1), '%Y-%m-%d')
            enddate = parse_date('%s-12-31' % match.group(3), '%Y-%m-%d')
            ranges.append((startdate, enddate))
            newtitle = remove_string(title, match.group(0))
            logger.info('Removing date range from title: %s -> %s' % (title, newtitle))
            title = newtitle

        for match in cls.YEAR_RANGE_PATTERN2.finditer(title):
            first_year = match.group(1)
            startdate = parse_date('%s-01-01' % first_year, '%Y-%m-%d')
            enddate = parse_date('%s%s-12-31' % (first_year[:2], match.group(3)), '%Y-%m-%d')
            ranges.append((startdate, enddate))
            newtitle = remove_string(title, match.group(0))
            logger.info('Removing date range from title: %s -> %s' % (title, newtitle))
            title = newtitle

        title = cls.fuzzy_match_dates_in_title(title, ranges)

        title = title.replace('()', '')
        title = remove_end_characters(title, '%s%s' % (PUNCTUATION_MINUS_BRACKETS, whitespace))
        title = remove_from_end(title, ['as of'], 'Removing - from title: %s -> %s')
        if len(ranges) == 0:
            return title, None, None
        else:
            startdate, enddate = sorted(ranges)[0]
            return title, startdate, enddate
