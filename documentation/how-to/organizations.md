# Manage Organizations

The **Organization** class enables you to manage organizations, creating, deleting and
updating (as for other HDX objects) according to your permissions.

You can get the datasets in an organization as follows:

    datasets = organization.get_datasets(**kwargs)

Various additional arguments (`**kwargs`) can be supplied. These are detailed in the API
documentation.

You can get the users in an organization like this:

    users = organization.get_users("OPTIONAL FILTER")

OPTIONAL FILTER can be member, editor, admin.

You can add or update a user in an organization as shown below:

    organization.add_update_user(USER)

You need to include a capacity field in the USER where capacity is member, editor, admin.

You can add or update multiple users in an organization as follows:

    organization.add_update_users([LIST OF USERS])

You can delete a user from an organization:

    organization.delete_user("USER ID")
