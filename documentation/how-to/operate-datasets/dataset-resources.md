# Manage Dataset Resources

A dataset can have resources and can be in a showcase.

If you wish to add a resource, you can create a resource dictionary and set the
format then call the **add_update_resource** function, for example:

    resource = Resource({
        "name": "myfile.xlsx",
        "description": "description",
    })
    resource.set_format("xlsx")
    resource.set_file_to_upload(PATH_TO_FILE)
    dataset.add_update_resource(resource)

It is also possible to supply a resource id string or dictionary to the
**add_update_resource** function. A url can be given instead of uploading a
file to the HDX filestore (although using the filestore is preferred):

    resource = Resource({
        "name": "myfile.xlsx",
        "description": "description",
        "url": "https://www.blah.com/myfile.xlsx"
    })
    resource.set_format("xlsx")
    dataset.add_update_resource(resource)

You can delete a Resource object from the dataset using the **delete_resource**
function, for example:

    dataset.delete_resource(resource)

**add_update_resources** creates a list of HDX Resource objects
in a dataset:

    dataset.add_update_resources(resources)

To see the list of resources, you use the **get_resources** function eg.

    resources = dataset.get_resources()

You can get all the resources from a list of datasets as follows:

    resources = Dataset.get_all_resources(datasets)

To see the list of showcases a dataset is in, you use the **get_showcases**
function eg.

    showcases = dataset.get_showcases()

If you wish to add the dataset to a showcase, you must first create the
showcase in HDX if it does not already exist:

    showcase = Showcase({"name": "new-showcase-1",
                         "title": "MyShowcase1",
                         "notes": "My Showcase",
                         "package_id": "6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d",
                         "image_display_url": "http://myvisual/visual.png",
                         "url": "http://visualisation/url/"})
    showcase.create_in_hdx()

Then you can supply an id, dictionary or Showcase object and call the
**add_showcase** function, for example:

    dataset.add_showcase(showcase)

You can remove the dataset from a showcase using the **remove_showcase**
function, for example:

    dataset.remove_showcase(showcase)

HDX runs file structure checks on resources, recording schema changes (eg.
column names, order or types) and whether sheets have merged cells. You can get
these checks for all of a dataset's resources from HDX:

    fs_check_info = dataset.get_fs_check_info()

This returns a list of dictionaries, each from a resource name to a list of
check events with keys such as `state`, `timestamp`, `sheet_changes` and
`hxl_proxy_response`.

Similarly, you can get the geopreview shape information of a dataset's
resources:

    shape_info = dataset.get_shape_info()

HDX can recommend tags for a dataset based on its title and organization
(`owner_org`). Tags are ordered from most to least recommended:

    tags = dataset.get_recommended_tags()

You can check if the dataset or its organization is in HDX's allowlist for
automatic loading of resources into the datastore:

    allowed = dataset.is_allowed_for_datastore()

HDX sysadmins can submit all of a dataset's CSV resources to DataPusher+
(returning False if there are none):

    submitted = dataset.push_to_datastore()

Users who are sysadmins or have permission to manage QA can mark QA of a
dataset as completed, set a QA field of all its resources or clear a file
structure check field (`fs_check_info` by default) of all its resources. The
dataset is updated from the result:

    dataset.set_qa_completed(True)
    dataset.qa_revise_resources("pii", "false")
    dataset.reset_fs_check()