# Use HDX Objects

You can read an existing HDX object with the static **read_from_hdx** method
which takes an identifier parameter and returns the an object of the appropriate HDX
object type eg. **Dataset** or **None** depending upon whether the object was read eg.

    dataset = Dataset.read_from_hdx("DATASET_ID_OR_NAME")

You can search for datasets and resources in HDX using the **search_in_hdx** method
which takes a query parameter and returns the a list of objects of the appropriate HDX
object type eg. **list[Dataset]**. Here is an example:

    datasets = Dataset.search_in_hdx("QUERY", **kwargs)

The query parameter takes a different format depending upon whether it is for a
[dataset](https://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.get.package_search)
or a
[resource](https://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.get.resource_search).
The resource level search is limited to fields in the resource, so in most cases, it is
preferable to search for datasets and then get their resources.

Various additional arguments (`**kwargs`) can be supplied. These are detailed in the API
documentation. The rows parameter for datasets (limit for resources) is the maximum
number of matches returned and is by default everything.

You can create an HDX Object, such as a dataset, resource, showcase, organization or
user by calling the constructor with an optional dictionary containing metadata. For
example:

    from hdx.data.dataset import Dataset

    dataset = Dataset({
        "name": slugified_name,
        "title": title
    })

The dataset name should not contain special characters and hence if there is any chance
of that, then it needs to be slugified. Slugifying is way of making a string valid
within a URL (eg. **ae** replaces **ä**). There are various packages that can do this
eg. [python-slugify](https://pypi.python.org/pypi/python-slugify).

You can add metadata using the standard Python dictionary square brackets eg.

    dataset["name"] = "My Dataset"

You can also do so by the standard dictionary **update** method, which takes a
dictionary eg.

    dataset.update({"name": "My Dataset"})

Larger amounts of static metadata are best added from files. YAML is very human readable
and recommended, while JSON is also accepted eg.

    dataset.update_from_yaml([path])

    dataset.update_from_json([path])

The default path if unspecified is **config/hdx_TYPE_static.yaml** for YAML and
**config/hdx_TYPE_static.json** for JSON where TYPE is an HDX object's type like
dataset or resource eg. **config/hdx_showcase_static.json**. The YAML file takes the
following form:

    owner_org: "acled"
    maintainer: "acled"
    ...
    tags:
        - name: "violence and conflict"
    resources:
        -
          description: "Resource1"
          url: "http://resource1.xlsx"
          format: "xlsx"
    ...

Notice how you can define resources (each resource starts with a dash "-") within the
file as shown above.

You can check if all the fields required by HDX are populated by
calling **check_required_fields**. This will throw an exception if any fields are
missing. Before the library posts data to HDX, it will call this method automatically.
You can provide a list of fields to ignore in the check. An example usage:

    resource.check_required_fields([ignore_fields])

Once the HDX object is ready ie. it has all the required metadata, you simply
call **create_in_hdx** eg.

    dataset.create_in_hdx(allow_no_resources, update_resources,
                          update_resources_by_name,
                          remove_additional_resources)

If the object already exists, it will be updated. You can also update
explicitly by calling **update_in_hdx** eg.

    dataset.update_in_hdx(update_resources, update_resources_by_name,
                          remove_additional_resources)

You can delete HDX objects using **delete_from_hdx** and update an object that
already exists in HDX with the method **update_in_hdx**. These take various
boolean parameters that all have defaults and are documented in the API docs.
They do not return anything and they throw exceptions for failures like the
object to update not existing.
