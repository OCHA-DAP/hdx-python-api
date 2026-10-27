# Set Dataset Time Period

Time Period is a mandatory field in HDX. It is the earliest start date and
latest end date across all the resources included in the dataset. The time
period may be of any length: a year, a month, or even a day. It should not to
be confused with when data was last added/changed in the dataset. It can be a
single date or a range.

To get the time period, you can do as shown below. It returns a dictionary
containing keys "startdate" (start date as datetime), "enddate" (end date as
datetime), "startdate_str" (start date as string), "enddate_str" (end date as
string) and ongoing (whether the end date is a rolls forward every day). You
can supply a [date format](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior).
If you do not, the output format will be an
[ISO 8601 date](https://en.wikipedia.org/wiki/ISO_8601) eg. 2007-01-25.

    time_period = dataset.get_time_period("OPTIONAL FORMAT")

To set the time period, you must pass either datetime.datetime objects or strings to
the function below. It accepts a start date and an optional end date which if not
supplied is assumed to be the same as the start date. Instead of the end date, the flag
"ongoing" which by default is False can be set to True which indicates that the end date
rolls forward every day.

    dataset.set_time_period("START DATE", "END DATE")

The method below allows you to set the time period using a year range. The start and
end year can be supplied as integers or strings. If no end year is supplied then the
range will be from the beginning of the start year to the end of that year.

    dataset.set_time_period_year_range(START YEAR, END YEAR)