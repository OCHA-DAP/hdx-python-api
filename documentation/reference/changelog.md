# Breaking Changes

From 6.6.5, removed DatasetTitleHelper class and Dataset method remove_dates_from_title

From 6.6.0, Python 3.10 or later is required

From 6.5.7, get_size_and_hash moved to HDX Python Utilities

From 6.5.2, remove unused `generate_qc_resource_from_rows` method.
`generate_resource_from_rows`, `generate_resource_from_iterable` and
`download_and_generate_resource` are deprecated. They are replaced by
`generate_resource` and `download_generate_resource`.

From 6.5.0, files will not be uploaded to the HDX filestore if the hash and size have
not changed, but if there are any resource metadata changes, except for last_modified,
they will still take place.

From 6.4.5, fix for changes in dependency defopt 7.0.0

From 6.2.8, fix mark_data_updated which was broken due to an error in
dataset_update_filestore_resource in which timezone information was
incorrectly added to the iso formatted string

From 6.2.7, generate_resource_from_iterator renamed to
generate_resource_from_iterable with requirement of iterable rather iterator

From 6.2.6, kwargs take preference over environment variables which take
preference over configuration files

From 6.2.5, environment variables take preference over kwargs which take
preference over configuration files

From 6.1.5, any method or parameter with "reference_period" in it is renamed
to "time_period" and  any method or parameter with "file_type" in it is renamed
to "format"

From 6.0.0, generate_resource_view is renamed to generate_quickcharts

From 5.9.9, get_location_iso3s returns uppercase codes instead of lowercase

From 5.9.8, get_date_of_dataset has become get_reference_period,
set_date_of_dataset has become set_reference_period and set_dataset_year_range
has become set_reference_period_year_range

From 5.9.7, Python 3.7 no longer supported

From 5.8.2, date handling uses timezone aware dates instead of naive dates and defaults
to UTC

From 5.6.0, creating and updating datastores removed

From 5.4.0, Configuration class moved to hdx.api.configuration and Locations class moved
to hdx.api.locations

From 5.3.0, only supports Python 3.6 and above

From 5.0.1, Dataset functions get_location_iso3s and get_location_names replace
get_location

From 4.8.3, some date functions in Dataset have been deprecated: get_dataset_date_type,
get_dataset_date_as_datetime, get_dataset_end_date_as_datetime, get_dataset_date,
get_dataset_end_date, set_dataset_date_from_datetime and set_dataset_date.

From 3.9.2, the default sort order for returned results from search and getting all
datasets has changed.

From 3.7.3, the return type for add_tag, add_tags and clean_tags is now Tuple[List[str],
List[str]] (Tuple containing list of added tags and list of deleted tags and tags not
added).

From 3.7.1, the list of tags must be from a set of approved tags (see
[Set Tags](../how-to/operate-datasets/tags.md)).