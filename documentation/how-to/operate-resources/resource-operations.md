# Operate on Resources

When creating or updating a resource that doesn't have an id, if you supply a parameter
dataset, then the resource will be assigned to that dataset and it will be compared to
resources in that dataset. If a match is found, then the resource will be given the
corresponding id and that resource on HDX will be overwritten.

    resource.create_in_hdx(dataset=DATASET)

Alternatively, if a resource doesn't have an id, but contains a package_id, the create
and update methods will use it to load the corresponding dataset, the resource will be
assigned to that dataset and it will be compared to resources in that dataset. If a
match is found, then the resource will be given the corresponding id and that resource
on HDX will be overwritten.

You can download a resource using the **download** function eg.

    url, path = resource.download("FOLDER_TO_DOWNLOAD_TO")

If you do not supply **FOLDER_TO_DOWNLOAD_TO**, then a temporary folder is used.

Before creating or updating a resource by calling **create_in_hdx** or
**update_in_hdx** on the resource or its parent dataset, it is possible to
specify the path to a local file to upload to the HDX filestore if that is
preferred over hosting the file externally to HDX. Rather than the url of the
resource pointing to your server or api, in this case the url will point to a
location in the HDX filestore containing a copy of your file.

    resource.set_file_to_upload(file_to_upload="PATH_TO_FILE")

There is a getter to read the value back:

    file_to_upload = resource.get_file_to_upload()

To indicate that the data in an externally hosted resource (given by a URL) has
been updated, call **mark_data_updated** on the resource, before calling
**create_in_hdx** or **update_in_hdx** on the resource or parent dataset which
will result in the resource `last_modified` field being set to now.

    resource.mark_data_updated()
    dataset.update_in_hdx()

Alternatively, when calling **create_in_hdx** or **update_in_hdx** on the
resource, it is possible to supply the parameter `data_updated` eg.

    resource.update_in_hdx(data_updated=True)

Using **mark_data_updated** on multiple resources in a dataset has the
advantage of only requiring a single call to HDX (by way of the dataset's
**create_in_hdx** or **update_in_hdx** method). Setting `data_updated` to
`True` when using each resource's **create_in_hdx** or **update_in_hdx** method
requires a call to HDX per resource.

If you need to set a specific date for date of update (`last_modified`), you
can call the following:

    resource.set_date_data_updated(date)

`date` can be a datetime object or string. You can retrieve the date of update
(`last_modified`) using the getter:

    date = resource.get_date_data_updated()

If the method **set_file_to_upload** is used to supply a file, the resource
`last_modified` field is set to now automatically regardless of the value of
`data_updated` or whether **mark_data_updated** has been called.

The file structure check events of a resource (see Dataset Specific
Operations) can be obtained from its `fs_check_info` field without a call to
HDX:

    events = resource.get_fs_check_info()

Users who are sysadmins or have permission to manage QA can set or clear a
file structure check field of a resource. If the key is `fs_check_info` and the
value is a dictionary, it is appended to the existing check events (the last 20
are kept):

    resource.revise_fs_check("fs_check_info", {"state": "success", ...})
    resource.reset_fs_check()  # key defaults to fs_check_info

They can also quarantine a resource (which removes its `shape_info` and
geopreview) or take it out of quarantine, and patch other QA fields:

    in_quarantine = resource.is_in_quarantine()
    resource.set_in_quarantine(True)
    resource.qa_patch(pii="true", sdc_report_flag="false")

A resource can be flagged as containing p-codes or as being in HAPI. Setting
these flags requires sysadmin or permission to manage p-codes or the in HAPI
flag respectively:

    p_coded = resource.is_p_coded()  # None if not set
    resource.set_p_coded(True)
    in_hapi = resource.is_in_hapi()
    resource.set_in_hapi(True)

These methods update the resource from the result.
