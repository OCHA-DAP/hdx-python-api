# Manage Users

The **User** class enables you to manage users, creating, deleting and updating (as for
other HDX objects) according to your permissions.

You can obtain the currently logged in user (which is based on the API token used in the
configuration):

    user = User.get_current_user()

You can get the name of the API token in the configuration and the md5 hash
of its owner's email:

    token_info = User.get_current_token_info()  # {"email_hash": ..., "token_name": ...}

You can check that the current user has a particular permission to a specific
organization:

    result = User.check_current_user_organization_access("hdx", "read")

For a general access check to use before running a script that creates or updates
datasets:

    username = User.check_current_user_write_access("hdx")

You can email a user. First you need to set up an email server using a dictionary or
file:

    email_config_dict = {"connection_type": "TYPE", "host": "HOST",
                         "port": PORT, "username": USERNAME,
                         "password": PASSWORD}
    Configuration.read().setup_emailer(email_config_dict=email_config_dict)

Then you can email a user like this:

    user.email("SUBJECT", "BODY", sender="SENDER EMAIL")

You can email multiple users like this:

    User.email_users(LIST_OF_USERS, "SUBJECT", "BODY", sender="SENDER EMAIL")
