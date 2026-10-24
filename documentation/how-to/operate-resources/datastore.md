# Use the Datastore

A resource's data can be loaded into the HDX datastore and queried. HDX calls this
[Tabular Data Endpoints (TDE)](https://docs.humdata.org/build/hdx-apis/tabular-data-endpoints),
and it is available only for resources with TDE enabled (their `datastore_active`
field is true). Contact hdx@un.org to request TDE for your datasets. HDX requires an
API key for all datastore actions and dumps, including reads of public resources,
and limits each IP address to 60 requests per minute. To create, update or delete
the datastore for a resource:

    resource.create_datastore(
        [{"id": "code", "type": "text"}, {"id": "value", "type": "numeric"}],
        primary_key="code",
    )
    resource.update_datastore([{"code": "A", "value": 1.0}])  # upsert by default
    has_datastore = resource.has_datastore()
    resource.delete_datastore()

HDX loads uploaded files into the datastore using DataPusher+. To reload a
resource's file into the datastore without uploading it again (so
`last_modified` is unchanged), use **submit_to_datapusher**. It returns False
if the job was not submitted, eg. because one is already pending. HDX's
datastore allowlist and format checks, which apply to automatic submissions
on upload, are not applied. **get_datapusher_status** returns the status of
the latest job or None if there is none:

    submitted = resource.submit_to_datapusher(ignore_hash=True)
    status = resource.get_datapusher_status()  # eg. status["status"] == "complete"

HDX sysadmins can instead use **push_to_datastore**, which submits the
resource through HDX's wrapper action:

    submitted = resource.push_to_datastore()

To download the whole datastore (rather than the resource's file) use
**download_datastore**, which returns the URL and path like **download**.
`file_format` can be `csv` (default), `tsv`, `json` or `xml`, and `filename`
defaults to the resource name with that extension. **get_datastore_dump_url**
returns the URL without downloading:

    url, path = resource.download_datastore("FOLDER_TO_DOWNLOAD_TO")
    url = resource.get_datastore_dump_url(file_format="json")

**search_datastore** wraps CKAN's `datastore_search`, returning the result
dictionary (with keys including `records`, `fields` and `total`). It accepts
`filters`, `q`, `fields`, `sort`, `distinct`, `limit` (default 100), `offset`
and `include_total`, and passes any other keyword arguments to CKAN:

    result = resource.search_datastore(
        filters={"code": ["A", "B"]}, fields=["code", "value"], sort="value desc"
    )
    records = result["records"]

**get_datastore_records** takes the same query parameters but pages through
all matching rows, yielding each record (`page_size` defaults to 10000).
`records_format` csv and tsv are not supported; use `search_datastore` for those:

    for record in resource.get_datastore_records(filters={"code": "A"}):
        ...

**get_datastore_fields** returns the datastore field definitions excluding the
internal `_id` field:

    fields = resource.get_datastore_fields()

**search_datastore_sql** wraps CKAN's `datastore_search_sql`, running a
SELECT statement where tables are named by double quoted resource id and
returning the list of records. HDX returns at most 32,000 rows per query and
cuts off larger results, in which case a warning is logged (see
[paging in the TDE docs](https://docs.humdata.org/build/hdx-apis/tabular-data-endpoints/troubleshooting-and-error-handling#pagination)):

    records = Resource.search_datastore_sql(
        'SELECT code, SUM(value) AS total FROM "RESOURCE_ID" GROUP BY code'
    )
