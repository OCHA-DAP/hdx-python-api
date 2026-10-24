# Customise the Configuration

It is necessary to pass configuration parameters in the facade call eg.

    facade(main, user_agent=USER_AGENT, hdx_site = HDX_SITE_TO_USE, hdx_read_only = ONLY_READ_NOT_WRITE, hdx_key_file = LOCATION_OF_HDX_KEY_FILE, hdx_config_yaml=PATH_TO_HDX_YAML_CONFIGURATION, project_config_dict = {"MY_PARAMETER", "MY_VALUE"})

If you do not use the facade, you can use the **create** method of the **Configuration** class directly, passing in
appropriate keyword arguments ie.

    from hdx.api.configuration import Configuration
    ...
    Configuration.create([configuration], [user_agent], [user_agent_config_yaml], [remoteckan], KEYWORD ARGUMENTS)

You must supply a user agent using one of the following approaches:

1. Populate parameter **user_agent** (which should be the name of your
organisation and project)
2. Supply **user_agent_config_yaml** which should point to a YAML file which
contains a parameter **user_agent**
3. Supply **user_agent_config_yaml** which should point to a YAML file and populate
**user_agent_lookup** which is a key to look up in the YAML file which should
be of form:

        myproject:
            user_agent: test
        myproject2:
            user_agent: test2

4. Include **user_agent** in one of the configuration dictionaries or files outlined in
the table below eg.
    **hdx_config_json** or **project_config_dict**.

**KEYWORD ARGUMENTS** can be:

|Choose|Argument|Type|Value|Default|
|---|---|---|---|---|
| |hdx_site|Optional\[str\]|HDX site to use eg. prod, feature|test|
| |hdx_read_only|bool|Read only or read/write access to HDX|False|
| |hdx_key|Optional\[str\]|HDX key (not needed for read only)||
|Above or one of:|hdx_config_dict|dict|Dictionary with hdx_site, hdx_read_only, hdx_key||
|or|hdx_config_json|str|Path to JSON configuration with values as above||
|or|hdx_config_yaml|str|Path to YAML configuration with values as above||
|Zero or one of:|project_config_dict|dict|Project specific configuration dictionary||
|or|project_config_json|str|Path to JSON Project||

To access the configuration, you use the **read** method of the **Configuration** class as follows:

    Configuration.read()

For more advanced users, there are methods to allow you to pass in your own
configuration object, remote CKAN object and list of valid locations. See the API
documentation for more information.

This global configuration is used by default by the library but can be replaced by
Configuration instances passed to the constructors of HDX objects like Dataset eg.

    configuration = Configuration(KEYWORD ARGUMENTS)
    configuration.setup_remoteckan(REMOTE CKAN OBJECT)
    configuration.setup_validlocations(LIST OF VALID LOCATIONS)
    dataset = Dataset(configuration=configuration)
