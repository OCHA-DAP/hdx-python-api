# Manage the Data Dictionary

A resource can have a data dictionary describing the columns in the data it
contains. It is a list of dictionaries, each with the keys `field` (the
column name as it appears in the file), `label` (a human-readable label) and
`description` (a human-readable description of the column). Set it using
**set_hdx_data_dictionary**:

    resource.set_hdx_data_dictionary([
        {
            "field": "col_name",
            "label": "Column Label",
            "description": "Human-readable description.",
        },
        {
            "field": "another_col",
            "label": "Another Label",
            "description": "Second column description.",
        },
    ])

`field`, `label` and `description` must all be non-empty strings for every
column, otherwise an `HDXError` is raised. Read the data dictionary back
using the getter:

    data_dictionary = resource.get_hdx_data_dictionary()

To change the data dictionary of a resource already in HDX without sending
dataset update notifications, use **update_hdx_data_dictionary**. It patches
only the `hdx_data_dictionary` field (so `last_modified` is unchanged), then,
if the resource has a datastore, updates the datastore field labels and
descriptions without reloading the data. The given columns are merged by
`field` into the existing data dictionary in the resource metadata, so read
the resource from HDX first. Pass `replace=True` to replace the data
dictionary instead and `update_datastore=False` to leave the datastore alone:

    resource = Resource.read_from_hdx(RESOURCE_ID)
    resource.update_hdx_data_dictionary([
        {
            "field": "col_name",
            "label": "Column Label",
            "description": "New description.",
        },
    ])

Columns are matched to datastore fields by `field`. A warning is logged for
any column with no matching datastore field.
