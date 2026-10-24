# Generate Resources

There are a range of helpful functions to generate resources. In the following
examples, RESOURCE DATA takes the form {"name": NAME, "description":
DESCRIPTION} and ENCODING is a file encoding like "utf-8".

A resource can be generated from ROWS which is a list of list, tuple or
dictionary. HEADERS is either a row number (rows start counting at 1), or the
actual headers defined as a list of strings. If not set, all rows will be
treated as containing values:

    dataset.generate_resource("FOLDER", "FILENAME", ROWS, RESOURCE_DATA,
                              headers=HEADERS, columns=COLUMNS, format="FORMAT",
                              encoding="ENCODING", datecol=DATECOL)

To use years or a custom date function instead, replace `datecol=DATECOL` with
`yearcol=YEARCOL` or `date_function=DATE_FUNCTION`. Supply only one of these
three arguments, using its keyword.

The first 4 parameters are mandatory, the rest are optional. A resource can be generated
from a given list or tuple or other iterable. The method returns a tuple with a bool
True is the resource was addeed and a dictionary of information. FOLDER and FILENAME
specify where the file will be generated for upload to the filestore. The dataset
time period can optionally be set by supplying DATECOL for looking up
dates or YEARCOL for looking up years. DATECOl and YEARCOL can be a column name
or the index of a column. Note that any timezone information is ignored and UTC
is assumed.

Alternatively, DATE_FUNCTION can be supplied to handle any dates
in a row. It should accept a row and should return None to ignore the row or a
dictionary which can either be empty if there are no dates in the row or can be
populated with keys startdate and/or enddate which are of type timezone-aware
datetime. The lowest start date and highest end date are used to set the
time period and are returned in the results dictionary in keys startdate
and enddate.

`download_generate_resource` builds on `generate_resource`.
It uses a DOWNLOADER, an object of class `Download`, `Retrieve` or other class
that implements `BaseDownload` to download from a URL. Additional arguments in
**KWARGS are passed to the `get_tabular_rows` method of the DOWNLOADER.

Optionally, headers can be inserted at specific positions. This is achieved
using HEADER_INSERTIONS. If supplied, it is a list of tuples of the form
(position, header) to be inserted. A function, ROW_FUNCTION, is called for each
row. If supplied, it takes as arguments: headers (prior to any insertions) and
row (which will be in dict or list form depending upon the dict_rows argument)
and outputs a modified row.

The rest of the arguments are the same as for `generate_resource`.

    dataset.download_generate_resource(DOWNLOADER, "URL", "FOLDER", "FILENAME",
                                       RESOURCE_DATA, header_insertions=HEADER_INSERTIONS,
                                       row_function=ROW_FUNCTION, datecol=DATECOL,
                                       **KWARGS)

As with `generate_resource`, use `yearcol=YEARCOL` or
`date_function=DATE_FUNCTION` instead of `datecol=DATECOL` when appropriate.
