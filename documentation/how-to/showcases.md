# Manage Showcases

The **Showcase** class enables you to manage showcases, creating, deleting and updating
(as for other HDX objects) according to your permissions.

To see the list of datasets a showcase is in, you use the **get_datasets** function eg.

    datasets = showcase.get_datasets()

If you wish to add a dataset to a showcase, you call the **add_dataset** function, for
example:

    showcase.add_dataset(dataset)

You can remove the dataset from a showcase using the **remove\dataset** function, for
example:

    showcase.remove_dataset(dataset)

If you wish to get the current tags, you can use this method:

    tags = showcase.get_tags()

If you want to add a tag, you do it like this:

    showcase.add_tag("TAG")

If you want to add a list of tags, you do it as follows:

    showcase.add_tags(["TAG","TAG","TAG"...])
