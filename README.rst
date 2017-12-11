|Build_Status| |Coverage_Status|

| The HDX Python Library is designed to enable you to easily develop
  code that interacts with the `Humanitarian Data Exchange`_ (HDX) platform. The
  major goal of the library is to make pushing and pulling data from HDX
  as simple as possible for the end user. If you have humanitarian-related data,
  please upload your datasets to HDX.

| For more about the purpose and design philosophy, please visit `HDX
  Python
  Library <https://humanitarian.atlassian.net/wiki/display/HDX/HDX+Python+Library>`__.

-  `Usage <#usage>`__
-  `Getting Started <#getting-started>`__

   -  `Obtaining your API Key <#obtaining-your-api-key>`__
   -  `Installing the Library <#installing-the-library>`__
   -  `Docker <#docker>`__
   -  `A Quick Example <#a-quick-example>`__

-  `Building a Project <#building-a-project>`__

   -  `Default Configuration for
      Facades <#default-configuration-for-facades>`__
   -  `Facades <#facades>`__
   -  `Customising the Configuration <#customising-the-configuration>`__
   -  `Configuring Logging <#configuring-logging>`__
   -  `Operations on HDX Objects <#operations-on-hdx-objects>`__
   -  `Dataset Specific Operations <#dataset-specific-operations>`__

      -  `Dataset Date <#dataset-date>`__
      -  `Expected Update Frequency <#expected-update-frequency>`__
      -  `Location <#location>`__
      -  `Tags <#tags>`__
      -  `Maintainer <#maintainer>`__
      -  `Organization <#organization>`__

   -  `Resource Specific Operations <#resource-specific-operations>`__
   -  `User Management <#user-management>`__
   -  `Organization Management <#organization-management>`__

-  `Working Example <#working-example>`__
-  `ACLED Example <#acled-example>`__

Usage
-----

The library has detailed API documentation which can be found
here: \ http://ocha-dap.github.io/hdx-python-api/. The code for the
library is here: \ https://github.com/ocha-dap/hdx-python-api.

Getting Started
---------------

Obtaining your API Key
~~~~~~~~~~~~~~~~~~~~~~

If you just want to read data from HDX, then an API key is not necessary
and you can ignore the 7 steps below. However, if you want to write data
to HDX, then you need to register on the website to obtain an API key.
You can supply this key as an argument or create an API key file. If
you create an API key file, by default this is assumed to be called
**.hdxkey** and is located in the current user's home directory **~**.
Assuming you are using a desktop browser, the API key is obtained by:

#. Browse to the \ `HDX website <https://data.humdata.org/>`__
#. Left click on LOG IN in the top right of the web page if not logged
   in and log in
#. Left click on your username in the top right of the web page and
   select PROFILE from the drop down menu
#. Scroll down to the bottom of the profile page
#. Copy the API key which will be of the form:
   xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
#. To make an API key file, paste the API key into a text file
#. Save the text file with filename **.hdxkey** in the current user's
   home directory

Installing the Library
~~~~~~~~~~~~~~~~~~~~~~

To include the HDX Python library in your project, you must
**pip install** or add to your **requirements.txt** file the following
line:

::

    hdx-python-api==VERSION

Replace **VERSION** with the latest tag available from
https://github.com/OCHA-DAP/hdx-python-api/tags.

If you get dependency errors, it is probably the dependencies of the
cryptography package that are missing eg. for Ubuntu: python-dev,
libffi-dev and libssl-dev. See `cryptography dependencies
<https://cryptography.io/en/latest/installation/#building-cryptography-on-linux>`__.

If you get import or other errors, then please either recreate your
virtualenv if you are using one or uninstall hdx-python-api,
hdx-python-country and hdx-python-utilities using **pip uninstall**,
then install hdx-python-api (which will pull in the other dependencies).

Docker
~~~~~~

The library is also available set up and ready to go in a Docker image:

::

    docker pull mcarans/hdx-python-api
    docker run -i -t mcarans/hdx-python-api:latest python3

A Quick Example
~~~~~~~~~~~~~~~

|A_Quick_Example|

Let's start with a simple example that also ensures that the library is
working properly. In this tutorial, we use virtualenv, a sandbox, so
that your Python install is not modified.

#. If you just want to read data from HDX, then an API key is not
   necessary. However, if you want to write data to HDX, then you need
   to register on the website to obtain an API key. Please see above
   about where to find it on the website. Once you have it, then put it
   into a file in your home directory:

   ::

       cd ~
       echo xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx > .hdxkey

#. If you are using the Docker image, you can jump to step 7, otherwise
   install virtualenv if not installed:

   ::

       pip install virtualenv

On some Linux distributions, you can do the following instead to install
from the distribution's official repository:

::

        sudo apt-get install virtualenv

#. Create a Python 3 virtualenv and activate it:

On Windows (assuming the Python 3 executable is in your path):

::

        virtualenv test
        test\Scripts\activate

On other OSs:

::

        virtualenv -p python3 test
        source test/bin/activate

#. Install the HDX Python library:

   ::

       pip install hdx-python-api

#. If you get errors, it is probably the `dependencies of the
   cryptography package <#installing-the-library>`__

#. Launch python:

   ::

       python

#. Import required classes:

   ::

       from hdx.hdx_configuration import Configuration
       from hdx.data.dataset import Dataset

#. Use configuration defaults.

   If you only want to read data, then connect to the production HDX server:

   ::

       Configuration.create(hdx_site='prod', hdx_read_only=True)

   If you want to write data, then for experimentation, do not use the production HDX server.
   Instead you can use one of the test servers. Assuming you have an API key stored in a file
   **.hdxkey** in the current user's home directory:

   ::

       Configuration.create(hdx_site='test')

#. Read this dataset `ACLED Conflict Data for Africa (Realtime -
   2016) <https://test-data.humdata.org/dataset/acled-conflict-data-for-africa-realtime-2016>`__
   from HDX and view the date of the dataset:

   ::

       dataset = Dataset.read_from_hdx('acled-conflict-data-for-africa-realtime-2016')
       print(dataset.get_dataset_date())

#. If you have an API key, as a test, change the dataset date:

   ::

       dataset.set_dataset_date('2015-07-26', date_format='%Y-%m-%d')
       print(dataset.get_dataset_date())
       dataset.update_in_hdx()

#. You can view it on HDX before changing it back (if you have an API
   key):

   ::

       dataset.set_dataset_date('2016-06-25', date_format='%Y-%m-%d')
       dataset.update_in_hdx()

#. You can search for datasets on HDX and get their resources:

   ::

       datasets = Dataset.search_in_hdx('ACLED', rows=10)
       print(datasets)
       resources = Dataset.get_all_resources(datasets)
       print(resources)

#. You can download a resource in the dataset:

   ::

       url, path = resources[0].download()
       print('Resource URL %s downloaded to %s' % (url, path))

#. Exit and remove virtualenv:

   ::

       exit()
       deactivate

   On Windows:

   ::

       rd /s /q test

   On other OSs:

   ::

       rm -rf test

Building a Project
------------------

Default Configuration for Facades
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to get started is to use the facades and configuration
defaults. The facades set up both logging and HDX configuration.

The default configuration loads an internal HDX configuration located
within the library, and assumes that there is an API key file called
**.hdxkey** in the current user's home directory **~** and a YAML
project configuration located relative to your working directory at
**config/project_configuration.yml** which you must create. The project
configuration is used for any configuration specific to your project.

The default logging configuration reads a configuration file internal to
the library that sets up an coloured console handler outputting at DEBUG
level and a file handler writing to errors.log at ERROR level.

Facades
~~~~~~~

You will most likely just need the simple facade. If you are in the HDX
team, you may need to use the ScraperWiki facade which reports status to
that platform (in which case replace **simple** with **scraperwiki** in
the code below):

::

    from hdx.facades.simple import facade

    def main():
        ***YOUR CODE HERE***

    if __name__ == '__main__':
        facade(main)

Customising the Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to pass configuration parameters in the facade call eg.

::

    facade(main, hdx_site = HDX_SITE_TO_USE, hdx_read_only = ONLY_READ_NOT_WRITE, hdx_key_file = LOCATION_OF_HDX_KEY_FILE, hdx_config_yaml=PATH_TO_HDX_YAML_CONFIGURATION, project_config_dict = {'MY_PARAMETER', 'MY_VALUE'})

If you do not use the facade, you can use the **create** method of the
**Configuration** class directly, passing in appropriate keyword
arguments ie.

::

    from hdx.hdx_configuration import Configuration
    ...
    Configuration.create(KEYWORD ARGUMENTS)

**KEYWORD ARGUMENTS** can be:

+---------+-----------------------+----------------+---------------------------+-------------------------+
| Choose  | Argument              | Type           | Value                     | Default                 |
|         |                       |                |                           |                         |
+=========+=======================+================+===========================+=========================+
|         | hdx\_site             | Optional[bool] | HDX site to use eg.       | test                    |
|         |                       |                | prod, feature             |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| One of: | hdx\_read\_only       | bool           | Read only or read/write   | False                   |
|         |                       |                | access to HDX             |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | hdx\_key              | Optional[str]  | HDX key                   |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | hdx\_key\_file        | Optional[str]  | Path to HDX key file.     | ~/.hdxkey               |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| One of: | hdx\_config\_dict     | dict           | HDX configuration         |                         |
|         |                       |                | dictionary                |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | hdx\_config\_json     | str            | Path to JSON HDX          |                         |
|         |                       |                | configuration             |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | hdx\_config\_yaml     | str            | Path to YAML HDX          | Library's internal      |
|         |                       |                | configuration             | hdx\_configuration.yml  |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| One of: | project\_config\_dict | dict           | Project configuration     |                         |
|         |                       |                | dictionary                |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | project\_config\_json | str            | Path to JSON Project      |                         |
|         |                       |                | configuration             |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+
| or      | project\_config\_yaml | str            | Path to YAML Project      |                         |
|         |                       |                | configuration             |                         |
+---------+-----------------------+----------------+---------------------------+-------------------------+

To access the configuration, you use the **read** method of the
**Configuration** class as follows:

::

    Configuration.read()

For more advanced users, there are methods to allow you to pass in
your own configuration object, remote CKAN object and list of valid
locations. See the API documentation for more information.

This global configuration is used by default by the library but can
be replaced by Configuration instances passed to the constructors
of HDX objects like Dataset eg.

::

    configuration = Configuration(KEYWORD ARGUMENTS)
    configuration.setup_remoteckan(REMOTE CKAN OBJECT)
    configuration.setup_validlocations(LIST OF VALID LOCATIONS)
    dataset = Dataset(configuration=configuration)

Configuring Logging
~~~~~~~~~~~~~~~~~~~

If you wish to change the logging configuration from the defaults, you
will need to call \ **setup_logging** with arguments unless you have
used the simple or ScraperWiki facades, in which case you must update
the **hdx.facades** module variable \ **logging_kwargs** before
importing the facade.

If not using facade:

::

    from hdx.utilities.easy_logging import setup_logging
    ...
    logger = logging.getLogger(__name__)
    setup_logging(KEYWORD ARGUMENTS)

If using facade:

::

    from hdx.facades import logging_kwargs

    logging_kwargs.update(DICTIONARY OF KEYWORD ARGUMENTS)
    from hdx.facades.simple import facade

**KEYWORD ARGUMENTS** can be:

+-----------+-----------------------+------+--------------------------+----------------------------+
| Choose    | Argument              | Type | Value                    | Default                    |
|           |                       |      |                          |                            |
+===========+=======================+======+==========================+============================+
| One of:   | logging\_config\_dict | dict | Logging configuration    |                            |
|           |                       |      | dictionary               |                            |
+-----------+-----------------------+------+--------------------------+----------------------------+
| or        | logging\_config\_json | str  | Path to JSON Logging     |                            |
|           |                       |      | configuration            |                            |
+-----------+-----------------------+------+--------------------------+----------------------------+
| or        | logging\_config\_yaml | str  | Path to YAML Logging     | Library's internal         |
|           |                       |      | configuration            | logging\_configuration.yml |
+-----------+-----------------------+------+--------------------------+----------------------------+
| One of:   | smtp\_config\_dict    | dict | Email Logging            |                            |
|           |                       |      | configuration dictionary |                            |
+-----------+-----------------------+------+--------------------------+----------------------------+
| or        | smtp\_config\_json    | str  | Path to JSON Email       |                            |
|           |                       |      | Logging configuration    |                            |
+-----------+-----------------------+------+--------------------------+----------------------------+
| or        | smtp\_config\_yaml    | str  | Path to YAML Email       |                            |
|           |                       |      | Logging configuration    |                            |
+-----------+-----------------------+------+--------------------------+----------------------------+

Do not supply **smtp_config_dict**, **smtp_config_json** or
**smtp_config_yaml** unless you are using the default logging
configuration!

If you are using the default logging configuration, you have the option
to have a default SMTP handler that sends an email in the event of a
CRITICAL error by supplying either **smtp_config_dict**,
**smtp_config_json** or **smtp_config_yaml**. Here is a template of a
YAML file that can be passed as the **smtp_config_yaml** parameter:

::

    handlers:
        error_mail_handler:
            toaddrs: EMAIL_ADDRESSES
            subject: "RUN FAILED: MY_PROJECT_NAME"

Unless you override it, the mail server **mailhost** for the default
SMTP handler is **localhost** and the from address **fromaddr** is
**noreply@localhost**.

To use logging in your files, simply add the line below to the top of
each Python file:

::

    logger = logging.getLogger(__name__)

Then use the logger like this:

::

    logger.debug('DEBUG message')
    logger.info('INFORMATION message')
    logger.warning('WARNING message')
    logger.error('ERROR message')
    logger.critical('CRITICAL error message')

Operations on HDX Objects
~~~~~~~~~~~~~~~~~~~~~~~~~

You can read an existing HDX object with the
static \ **read_from_hdx** method which takes an identifier parameter
and returns the an object of the appropriate HDX object type eg.
**Dataset** or **None** depending upon whether the object was read eg.

::

    dataset = Dataset.read_from_hdx('DATASET_ID_OR_NAME')

You can search for datasets and resources in HDX using the
**search_in_hdx** method which takes a query parameter and returns the a
list of objects of the appropriate HDX object type eg. **list[Dataset]**
eg.

::

    datasets = Dataset.search_in_hdx('QUERY', **kwargs)

The query parameter takes a different format depending upon whether it
is for a
`dataset <http://docs.ckan.org/en/ckan-2.3.4/api/index.html#ckan.logic.action.get.package_search>`__
or a
`resource <http://docs.ckan.org/en/ckan-2.3.4/api/index.html#ckan.logic.action.get.resource_search>`__.
The resource level search is limited to fields in the resource, so in
most cases, it is preferable to search for datasets and then get their
resources.

Various additional arguments (``**kwargs``) can be supplied. These are
detailed in the API documentation. The rows parameter for datasets
(limit for resources) is the maximum number of matches returned and is
by default everything.

You can create an HDX Object, such as a dataset, resource, showcase,
organization or user by calling the constructor with an optional
dictionary containing metadata. For example:

::

    from hdx.data.dataset import Dataset

    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })

The dataset name should not contain special characters and hence if
there is any chance of that, then it needs to be slugified. Slugifying
is way of making a string valid within a URL (eg. **ae** replaces
**ä**). There are various packages that can do this eg.
`awesome-slugify <https://pypi.python.org/pypi/awesome-slugify>`__.

You can add metadata using the standard Python dictionary square
brackets eg.

::

    dataset['name'] = 'My Dataset'

You can also do so by the standard dictionary \ **update** method, which
takes a dictionary eg.

::

    dataset.update({'name': 'My Dataset'})

Larger amounts of static metadata are best added from files. YAML is
very human readable and recommended, while JSON is also accepted eg.

::

    dataset.update_from_yaml([path])

    dataset.update_from_json([path])

The default path if unspecified is **config/hdx_TYPE_static.yml** for
YAML and **config/hdx_TYPE_static.json** for JSON where TYPE is an HDX
object's type like dataset or resource eg.
**config/hdx_showcase_static.json**. The YAML file takes the
following form:

::

    owner_org: "acled"
    maintainer: "acled"
    ...
    tags:
        - name: "conflict"
        - name: "political violence"
    resources:
        -
          description: "Resource1"
          url: "http://resource1.xlsx"
          format: "xlsx"
    ...

Notice how you can define resources (each resource starts with a dash
'-') within the file as shown above.

You can check if all the fields required by HDX are populated by
calling \ **check_required_fields**. This will throw an exception if any
fields are missing. Before the library posts data to HDX, it will call
this method automatically. You can provide a list of fields to ignore in
the check. An example usage:

::

    resource.check_required_fields([ignore_fields])

Once the HDX object is ready ie. it has all the required metadata, you
simply call \ **create_in_hdx** eg.

::

    dataset.create_in_hdx()

Existing HDX objects can be updated by calling \ **update_in_hdx** eg.

::

    dataset.update_in_hdx()

You can delete HDX objects using \ **delete_from_hdx** and update an
object that already exists in HDX with the method \ **update_in_hdx**.
These do not take any parameters or return anything and throw exceptions
for failures like the object to delete or update not existing.

Dataset Specific Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A dataset can have resources and can be in a showcase.

If you wish to add resources, you can supply a list and
call the \ **add_update_resources*** function, for example:

::

    resources = [{
        'name': xlsx_resourcename,
        'format': 'xlsx',
        'url': xlsx_url
     }, {
        'name': csv_resourcename,
        'format': 'zipped csv',
        'url': csv_url
     }]
     for resource in resources:
         resource['description'] = resource['url'].rsplit('/', 1)[-1]
     dataset.add_update_resources(resources)

Calling \ **add_update_resources** creates a list of HDX Resource
objects in dataset and operations can be performed on those objects.

To see the list of resources, you use the
\ **get_resources*** function eg.

::

    resources = dataset.get_resources()

If you wish to add one resource, you can supply an id string,
dictionary or Resource object and call the
\ **add_update_resource*** function, for example:

::

    dataset.add_update_resource(resource)

You can delete a Resource object from the dataset using the
\ **delete_resource*** function, for example:

::

    dataset.delete_resource(resource)

You can get all the resources from a list of datasets as follows:

::

    resources = Dataset.get_all_resources(datasets)

To see the list of showcases a dataset is in, you use the
\ **get_showcases*** function eg.

::

    resources = dataset.get_showcases()

If you wish to add the dataset to a showcase, you must first create
the showcase in HDX if it does not already exist:

::

    showcase = Showcase({'name': 'new-showcase-1',
                         'title': 'MyShowcase1',
                         'notes': 'My Showcase',
                         'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                         'image_display_url': 'http://myvisual/visual.png',
                         'url': 'http://visualisation/url/'})
    showcase.create_in_hdx()

Then you can supply an id, dictionary or Showcase object and call
the \ **add_showcase*** function, for example:

::

    dataset.add_showcase(showcase)

You can remove the dataset from a showcase using the
\ **remove_showcase*** function, for example:

::

    dataset.remove_showcase(showcase)


Dataset Date
^^^^^^^^^^^^

Dataset date is a mandatory field in HDX. This date is the date of the
data in the dataset, not to be confused with when data was last
added/changed in the dataset. It can be a single date or a range.

To determine if a dataset date is a single date or range you can call:

::

    dataset.get_dataset_date_type()

It returns 'date' for a single date or 'range' for a date range.

To get the dataset start date of a range or single date as a string, you can do as shown below. You can
supply a `date
format <https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior>`__.
If you don't, the output format will be an `ISO 8601
date <https://en.wikipedia.org/wiki/ISO_8601>`__ eg. 2007-01-25.

::

    dataset_date = dataset.get_dataset_date('FORMAT')


To get the dataset end date of a range, you call:

::

    dataset_date = dataset.get_dataset_end_date('FORMAT')


To set the dataset date, you pass a start date and end date for a range or just a start date for a single date. If you
do not supply any dates format, the method will try to guess, which for unambiguous formats
should be fine.

::

    dataset.set_dataset_date('START DATE', 'END DATE', 'FORMAT')

To retrieve the dataset date or range as a **datetime.datetime** object, you can
do:

::

    dataset_date = dataset.get_dataset_date_as_datetime()
    dataset_date = dataset.get_dataset_end_date_as_datetime()

The method below allows you to set the dataset's date using a
**datetime.datetime** object:

::

    dataset.set_dataset_date_from_datetime(START DATETIME.DATETIME OBJECT, END DATETIME.DATETIME OBJECT)

Expected Update Frequency
^^^^^^^^^^^^^^^^^^^^^^^^^

HDX datasets have a mandatory field, the expected update frequency. This
is your best guess of how often the dataset will be updated.

The HDX web interface uses set frequencies:

::

    Every day
    Every week
    Every two weeks
    Every month
    Every three months
    Every six months
    Every year
    Never

Although the API allows much greater granularity (a number of days), you
are encouraged to use the options above (avoiding using **Never** if
possible). To assist with this, you can use methods that allow this.

The following method will return a textual expected update frequency
corresponding to what would be shown in the HDX web interface.

::

    update_frequency = dataset.get_expected_update_frequency()

The method below allows you to set the dataset's expected update
frequency using one of the set frequencies above. (It also allows you to
pass a number of days cast to a string, but this is discouraged.)

::

    dataset.set_expected_update_frequency('UPDATE_FREQUENCY')

Transforming backwards and forwards between representations can be
achieved with this function:

::

    update_frequency = Dataset.transform_update_frequency('UPDATE_FREQUENCY')

Location
^^^^^^^^

Each HDX dataset must have at least one location associated with it.

If you wish to get the current location (ISO 3 country codes), you can
call the method below:

::

    locations = dataset.get_location()


If you want to add a country, you do as shown below. If you don't
provide an ISO 3 country code, the text you give will be parsed and
converted to an ISO 3 code if it is a valid country name.

::

    dataset.add_country_location('ISO 3 COUNTRY CODE')

If you want to add a list of countries, the following method enables you
to do it. If you don't provide ISO 3 country codes, conversion will take
place where valid country names are found.

::

    dataset.add_country_locations(['ISO 3','ISO 3','ISO 3'...])

If you want to add a region, you do it as follows. If you don't
provide a three digit `UNStats M49`_ region code, then parsing and
conversion will occur if a valid region name is supplied.

::

    dataset.add_region_location('M49 REGION CODE')

**add_region_location** accepts regions, intermediate regions or
subregions as specified on the `UNStats M49`_ website.

If you want to add any other kind of location (which must be in this
\ `list of valid locations <https://data.humdata.org/api/action/group_list?all_fields=true>`__),
you do as shown below.

::

    dataset.add_other_location('LOCATION')

Tags
^^^^

HDX datasets can have tags which help people to find them eg. "COD",
"PROTESTS".

If you wish to get the current tags, you can use this method:

::

    tags = dataset.get_tags()


If you want to add a tag, you do it like this:

::

    dataset.add_tag('TAG')

If you want to add a list of tags, you do it as follows:

::

    dataset.add_tags(['TAG','TAG','TAG'...])

Maintainer
^^^^^^^^^^

HDX datasets must have a maintainer.

If you wish to get the current maintainer, you can do this:

::

    maintainer = dataset.get_maintainer()

If you want to set the maintainer, you do it like this:

::

    dataset.set_maintainer(USER)

USER is either a string id, dictionary or a User object.

Organization
^^^^^^^^^^^^

HDX datasets must be part of an organization.

If you wish to get the current organization, you can do this:

::

    organization = dataset.get_organization()

If you want to set the organization, you do it like this:

::

    dataset.set_organization(ORGANIZATION)

ORGANIZATION is either a string id, dictionary or an Organization object.

Resource Specific Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can download a resource using the **download** function eg.

::

    url, path = resource.download('FOLDER_TO_DOWNLOAD_TO')

If you do not supply **FOLDER_TO_DOWNLOAD_TO**, then a temporary folder
is used.

Before creating or updating a resource, it is possible to specify the
path to a local file to upload to the HDX filestore if that is preferred
over hosting the file externally to HDX. Rather than the url of the
resource pointing to your server or api, in this case the url will point
to a location in the HDX filestore containing a copy of your file.

::

    resource.set_file_to_upload(file_to_upload='PATH_TO_FILE')

There is a getter to read the value back:

::

    file_to_upload = resource.get_file_to_upload()

If you wish to set up the data preview feature in HDX and your file (HDX
or externally hosted) is a csv, then you can call the
**create_datastore** or **update_datastore** methods. If you do not pass
any parameters, all fields in the csv will be assumed to be text.

::

    resource.create_datastore()
    resource.update_datastore()

More fine grained control is possible by passing certain parameters and
using other related methods eg.

::

    resource.create_datastore(schema={'id': 'FIELD', 'type': 'TYPE'}, primary_key='PRIMARY_KEY_OF_SCHEMA', delete_first=0 (No) / 1 (Yes) / 2 (If no primary key), path='LOCAL_PATH_OF_UPLOADED_FILE') -> None:
    resource.create_datastore_from_yaml_schema(yaml_path='PATH_TO_YAML_SCHEMA', delete_first=0 (No) / 1 (Yes) / 2 (If no primary key), path='LOCAL_PATH_OF_UPLOADED_FILE')
    resource.update_datastore(schema={'id': 'FIELD', 'type': 'TYPE'}, primary_key='PRIMARY_KEY_OF_SCHEMA', path='LOCAL_PATH_OF_UPLOADED_FILE') -> None:
    resource.update_datastore_from_json_schema(json_path='PATH_TO_JSON_SCHEMA', path='LOCAL_PATH_OF_UPLOADED_FILE')

User Management
~~~~~~~~~~~~~~~

The **User** class enables you to manage users, creating, deleting and
updating (as for other HDX objects) according to your permissions.

You can email a user. First you need to set up an email server using a
dictionary or file:

::

    email_config_dict = {'connection_type': 'TYPE', 'host': 'HOST',
                         'port': PORT, 'username': USERNAME,
                         'password': PASSWORD}
    Configuration.read().setup_emailer(email_config_dict=email_config_dict)


Then you can email a user like this:

::

    user.email('SUBJECT', 'BODY', sender='SENDER EMAIL')

You can email multiple users like this:

::

    User.email_users(LIST_OF_USERS, 'SUBJECT', 'BODY', sender='SENDER EMAIL')

Organization Management
~~~~~~~~~~~~~~~~~~~~~~~

The **Organization** class enables you to manage organizations,
creating, deleting and updating (as for other HDX objects)
according to your permissions.

You can get the datasets in an organization as follows:

::

    datasets = organization.get_datasets(**kwargs)

Various additional arguments (``**kwargs``) can be supplied. These are
detailed in the API documentation.

You can get the users in an organization like this:

::

    users = organization.get_users('OPTIONAL FILTER')

OPTIONAL FILTER can be member, editor, admin.

You can add or update a user in an organization as shown below:

::

    organization.add_update_user(USER)

You need to include a capacity field in the USER where capacity is member,
editor, admin.

You can add or update multiple users in an organization as follows:

::

    organization.add_update_users([LIST OF USERS])

You can delete a user from an organization:

::

    organization.delete_user('USER ID')

Working Example
---------------

Here we will create a working example from scratch.

First, pip install the library or alternatively add it to a
requirements.txt file if you are comfortable with doing so as described
above.

Next create a file called **run.py** and copy into it the code below.

::

    #!/usr/bin/python
    # -*- coding: utf-8 -*-
    '''
    Calls a function that generates a dataset and creates it in HDX.

    '''
    import logging
    from hdx.facades.scraperwiki import facade
    from .my_code import generate_dataset

    logger = logging.getLogger(__name__)


    def main():
        '''Generate dataset and create it in HDX'''

        dataset = generate_dataset()
        dataset.create_in_hdx()

    if __name__ == '__main__':
        facade(main, hdx_site='test')

The above file will create in HDX a dataset generated by a function
called **generate_dataset** that can be found in the file **my_code.py**
which we will now write.

Create a file **my_code.py** and copy into it the code below:

::

    #!/usr/bin/python
    # -*- coding: utf-8 -*-
    '''
    Generate a dataset

    '''
    import logging
    from hdx.data.dataset import Dataset

    logger = logging.getLogger(__name__)


    def generate_dataset():
        '''Create a dataset
        '''
        logger.debug('Generating dataset!')

You can then fill out the function **generate_dataset** as required.

ACLED Example
-------------

A complete example can be found
here: \ https://github.com/OCHA-DAP/hdxscraper-acled-africa

In particular, take a look at the files **run.py**, **acled_africa.py**
and the **config** folder. If you run it unchanged, it will conflict
with the existing dataset in the ACLED organisation! Therefore, you will
need to modify the dataset **name** in **acled_africa.py** and change
the organisation information to your organisation. Also update metadata
in **config/hdx_dataset_static.yml** appropriately.

The ACLED scraper creates a dataset in HDX for `ACLED realtime
data <https://data.humdata.org/dataset/acled-conflict-data-for-africa-realtime-2017>`__
if it doesn't already exist, populating all the required metadata. It
then creates resources that point to urls of `Excel and csv files for
Realtime 2017 All Africa
data <http://www.acleddata.com/data/realtime-data-2017/>`__ (or updates
the links and metadata if the resources already exist). Finally it
creates a showcase that points to these `dynamic maps and
graphs <http://www.acleddata.com/visuals/maps/dynamic-maps/>`__.

The first iteration of the ACLED scraper was written without the HDX
Python library and it became clear looking at this and previous work by
others that there are operations that are frequently required and which
add unnecessary complexity to the task of coding against HDX.
Simplifying the interface to HDX drove the development of the Python
library and the second iteration of the scraper was built using it. With
the interface using HDX terminology and mapping directly on to datasets,
resources and showcases, the ACLED scraper was faster to develop and
is much easier to understand for someone inexperienced in how it works
and what it is doing. The challenge with ACLED is that sometimes the
urls that the resources point to have not been updated and hence do not
work. In this situation, the extensive logging and transparent
communication of errors is invaluable and enables action to be taken to
resolve the issue as quickly as possible. The static metadata for ACLED
is held in human readable files so if it needs to be modified, it is
straightforward. This is another feature of the HDX Python library that
makes putting data programmatically into HDX a breeze.

.. |Build_Status| image:: https://travis-ci.org/OCHA-DAP/hdx-python-api.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/OCHA-DAP/hdx-python-api
.. |Coverage_Status| image:: https://coveralls.io/repos/github/OCHA-DAP/hdx-python-api/badge.svg?branch=master
    :alt: Coveralls Build Status
    :target: https://coveralls.io/github/OCHA-DAP/hdx-python-api?branch=master
.. _Humanitarian Data Exchange: https://data.humdata.org/
.. _UNStats M49: https://unstats.un.org/unsd/methodology/m49/overview/
.. |A_Quick_Example| image:: https://humanitarian.atlassian.net/wiki/download/attachments/6356996/HDXPythonLibrary.gif?version=1&modificationDate=1469520811486&api=v2

