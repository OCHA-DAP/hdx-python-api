[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-python-api.svg?branch=master&rd=2)](https://travis-ci.org/OCHA-DAP/hdx-python-api) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-python-api/badge.svg?branch=master&rd=2)](https://coveralls.io/github/OCHA-DAP/hdx-python-api?branch=master)

The HDX Python Library is designed to enable you to easily develop code that interacts with the Humanitarian Data Exchange platform. The major goal of the library is to make pushing and pulling data from HDX as simple as possible for the end user.
For more about the purpose and design philosophy, please visit [HDX Python Library](https://humanitarian.atlassian.net/wiki/display/HDX/HDX+Python+Library).

- [Usage](#usage)
- [Getting Started](#getting-started)
	- [Creating the API Key File](#creating-the-api-key-file)
	- [Installing the Library](#installing-the-library)
	- [A Quick Example](#a-quick-example)
- [Building a Project](#building-a-project)
	- [Default Configuration for Facades](#default-configuration-for-facades)
	- [Facades](#facades)
	- [Customising the Configuration](#customising-the-onfiguration)
	- [Configuring Logging](#configuring-logging)
	- [Operations on HDX Objects](#operations-on-hdx-objects)
	- [Dataset Specific Operations](#dataset-specific-operations)
    	- [Dataset Date](#dataset-date)
    	- [Expected Update Frequency](#expected-update-frequency)
    	- [Location](#location)
    	- [Tags](#tags)
	- [Resource Specific Operations](#resource-specific-operations)
- [Working Example](#working-example)
- [ACLED Example](#acled-example)

## Usage
The library has detailed API documentation:  

[![HDX Python API](https://humanitarian.atlassian.net/wiki/download/attachments/6356996/HDXPythonAPI.png?version=1&modificationDate=1469520100446&api=v2)](http://ocha-dap.github.io/hdx-python-api/)

The API documentation can be found here: [http://ocha-dap.github.io/hdx-python-api/](http://ocha-dap.github.io/hdx-python-api/). The code for the library is here: [https://github.com/ocha-dap/hdx-python-api](https://github.com/ocha-dap/hdx-python-api).

Please note that the library only works on Python 3.

## Getting Started
### Creating the API Key File

The first task is to create an API key file. By default this is assumed to be called `.hdxkey` and is located in the current user's home directory `~`. Assuming you are using a desktop browser, the API key is obtained by:

1. Browse to the [HDX website](https://data.humdata.org/)
2. Left click on LOG IN in the top right of the web page if not logged in and log in
3. Left click on your username in the top right of the web page and select PROFILE from the drop down menu
4. Scroll down to the bottom of the profile page
5. Copy the API key which will be of the form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
6. Paste the API key into a text file
7. Save the text file with filename `.hdxkey` in the current user's home directory

### Installing the Library

To include the HDX Python library in your project, pip install the line below or add the following to your `requirements.txt` file:

    git+git://github.com/ocha-dap/hdx-python-api.git@VERSION#egg=hdx-python-api

Replace `VERSION` with the latest tag available from [https://github.com/OCHA-DAP/hdx-python-api/tags](https://github.com/OCHA-DAP/hdx-python-api/tags). 
If you get errors, it is probably the dependencies of the cryptography package that are missing eg. for Ubuntu: python-dev, libffi-dev and libssl-dev. See [cryptography dependencies](https://cryptography.io/en/latest/installation/#building-cryptography-on-linux)

### A Quick Example

![A Quick Example](https://humanitarian.atlassian.net/wiki/download/attachments/6356996/HDXPythonLibrary.gif?version=1&modificationDate=1469520811486&api=v2)

Let's start with a simple example that also ensures that the library is working properly. This assumes you are using Linux, but you can do something similar on Windows:

1. Create the API key if you haven't already. Look it up on the HDX website as mentioned above, then put it into a file in your home directory:

        cd ~
        echo xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx > .hdxkey
2. Install virtualenv if not installed:

        sudo apt-get install virtualenv
3. Create a Python 3 virtualenv and activate it:

        virtualenv -p python3 test
        source test/bin/activate
4. Install the HDX Python library:

        pip install git+git://github.com/ocha-dap/hdx-python-api.git@VERSION#egg=hdx-python-api
Replace `VERSION` with the latest tag available from [https://github.com/OCHA-DAP/hdx-python-api/tags](https://github.com/OCHA-DAP/hdx-python-api/tags). 
5. If you get errors, it is probably the [dependencies of the cryptography package](#installing-the-library)
6. Launch python:

        python
7. Import required classes:

        from hdx.configuration import Configuration
        from hdx.data.dataset import Dataset
8. Use configuration defaults and test HDX site:

        configuration = Configuration(hdx_site='test', project_config_dict = {})
9. Read this dataset [ACLED Conflict Data for Africa (Realtime - 2016)](https://test-data.humdata.org/dataset/acled-conflict-data-for-africa-realtime-2016#) from HDX and view the date of the dataset:

        dataset = Dataset.read_from_hdx(configuration, 'acled-conflict-data-for-africa-realtime-2016')
        print(dataset.get_dataset_date_as_string())
10. As a test, change the dataset date:

        dataset.set_dataset_date_from_string('2015-07-26', 'YYYY-MM-DD')
        print(dataset.get_dataset_date_as_string())
        dataset.update_in_hdx()
11. You can view it on HDX before changing it back:

        dataset.set_dataset_date_from_string('2016-06-25', 'YYYY-MM-DD')
        dataset.update_in_hdx()
12. You can search for datasets on HDX and get their resources:

        datasets = Dataset.search_in_hdx(configuration, 'ACLED', rows=10)
        print(datasets)
        resources = Dataset.get_all_resources(datasets)
        print(resources)
13. You can download a resource in the dataset:

        url, path = resources[0].download()
        print('Resource URL %s downloaded to %s' % (url, path))
14. Exit and remove virtualenv:

        exit()
        deactivate
        rm -rf test

## Building a Project
### Default Configuration for Facades

The easiest way to get started is to use the facades and configuration defaults. The facades set up both logging and HDX configuration.

The default configuration loads an internal HDX configuration located within the library, and assumes that there is an API key file called `.hdxkey` in the current user's home directory `~` and a YAML project configuration located relative to your working directory at `config/project_configuration.yml` which you must create. The project configuration is used for any configuration specific to your project.

The default logging configuration reads a configuration file internal to the library that sets up an coloured console handler outputting at DEBUG level and a file handler writing to errors.log at ERROR level. 

### Facades

You will most likely just need the simple facade. If you are in the HDX team, you may need to use the ScraperWiki facade which reports status to that platform (in which case replace `simple` with `scraperwiki` in the code below):

    from hdx.facades.simple import facade

    def main(configuration):  
        ***YOUR CODE HERE***

    if __name__ == '__main__':  
        facade(main)

The configuration is passed to your main function in the `configuration` argument above.


### Customising the Configuration


It is possible to pass configuration parameters in the facade call eg.

    facade(main, hdx_site = HDX_SITE_TO_USE, hdx_key_file = LOCATION_OF_HDX_KEY_FILE, hdx_config_yaml=PATH_TO_HDX_YAML_CONFIGURATION, 

    project_config_dict = {'MY_PARAMETER', 'MY_VALUE'})

If you did not need a project configuration, you could simply provide an empty dictionary eg.

    facade(main, project_config_dict = {})

If you do not use the facade, you can use the `Configuration` class directly, passing in appropriate keyword arguments ie.

    from hdx.configuration import Configuration  
    ...  
    cfg = Configuration(KEYWORD ARGUMENTS)

`KEYWORD ARGUMENTS` can be:

| Choose |       Argument      |     Type     |               Value                |                 Default                |
|--------|---------------------|--------------|------------------------------------|----------------------------------------|
|        |hdx_site             |Optional[bool]|HDX site to use eg. prod, test      |test                                    |
|        |hdx_key_file         |Optional[str] |Path to HDX key file ~/.hdxkey      |                                        |                                  
|One of: |hdx_config_dict      |dict          |HDX configuration dictionary        |                                        |
|        |hdx_config_json      |str           |Path to JSON HDX configuration      |                                        |
|        |hdx_config_yaml      |str           |Path to YAML HDX configuration      |Library's internal hdx_configuration.yml|
|One of: |project_config_dict  |dict          |Project configuration dictionary    |                                        |
|        |project_config_json  |str           |Path to JSON Project configuration  |                                        |
|        |project_config_yaml  |str           |Path to YAML Project configuration  |config/project_configuration.yml        |

### Configuring Logging

If you wish to change the logging configuration from the defaults, you will need to call `setup_logging` with arguments unless you have used the simple or ScraperWiki facades, in which case you must update the `hdx.facades` module variable `logging_kwargs` before importing the facade.

If not using facade:

    from hdx.logging import setup_logging  
    ...  
    logger = logging.getLogger(__name__)  
    setup_logging(KEYWORD ARGUMENTS)

If using facade:

    from hdx.facades import logging_kwargs

    logging_kwargs.update(DICTIONARY OF KEYWORD ARGUMENTS)  
    from hdx.facades.simple import facade

`KEYWORD ARGUMENTS` can be:

|  Choose |      Argument     |Type|                 Value                  |                   Default                  |
|---------|-------------------|----|----------------------------------------|--------------------------------------------|
|One of:  |logging_config_dict|dict|Logging configuration dictionary        |                                            |
|         |logging_config_json|str |Path to JSON Logging configuration      |                                            |
|         |logging_config_yaml|str |Path to YAML Logging configuration      |Library's internal logging_configuration.yml|
|One of:  |smtp_config_dict   |dict|Email Logging configuration dictionary  |                                            |
|(if using|smtp_config_json   |str |Path to JSON Email Logging configuration|                                            |  
|defaults)|smtp_config_yaml   |str |Path to YAML Email Logging configuration|                                            |

Do not supply `smtp_config_dict`, `smtp_config_json` or `smtp_config_yaml` unless you are using the default logging configuration!

If you are using the default logging configuration, you have the option to have a default SMTP handler that sends an email in the event of a CRITICAL error by supplying either `smtp_config_dict`, `smtp_config_json` or `smtp_config_yaml`. Here is a template of a YAML file that can be passed as the `smtp_config_yaml` parameter:

    handlers:  
        error_mail_handler:  
            toaddrs: EMAIL_ADDRESSES  
            subject: "RUN FAILED: MY_PROJECT_NAME"

Unless you override it, the mail server `mailhost` for the default SMTP handler is `localhost` and the from address `fromaddr` is `noreply@localhost`.

To use logging in your files, simply add the line below to the top of each Python file:

    logger = logging.getLogger(__name__)

Then use the logger like this:

    logger.debug('DEBUG message')  
    logger.info('INFORMATION message')  
    logger.warning('WARNING message')  
    logger.error('ERROR message')  
    logger.critical('CRITICAL error message')

### Operations on HDX Objects

You can read an existing HDX object with the static `read_from_hdx` method which takes a configuration and an identifier parameter and returns the an object of the appropriate HDX object type eg. `Dataset` or `None` depending upon whether the object was read eg.

    dataset = Dataset.read_from_hdx(configuration, 'DATASET_ID_OR_NAME')

You can search for datasets and resources in HDX using the `search_in_hdx` method which takes a configuration and a query parameter and returns the a list of objects of the appropriate HDX object type eg. `list[Dataset]` eg.

    datasets = Dataset.search_in_hdx(configuration, 'QUERY', **kwargs)

The query parameter takes a different format depending upon whether it is for a [dataset](http://docs.ckan.org/en/ckan-2.3.4/api/index.html#ckan.logic.action.get.package_search) or a [resource](http://docs.ckan.org/en/ckan-2.3.4/api/index.html#ckan.logic.action.get.resource_search). The resource level search is limited to fields in the resource, so in most cases, it is preferable to search for datasets and then get their resources.

Various additional arguments (`**kwargs`) can be supplied. These are detailed in the API documentation. The rows parameter for datasets (limit for resources) is the maximum number of matches returned and is by default 10.

You can create an HDX Object, such as a dataset, resource or gallery item by calling the constructor with a configuration, which is required, and an optional dictionary containing metadata. For example:

    from hdx.data.dataset import Dataset

    dataset = Dataset(configuration, {  
        'name': slugified_name,  
        'title': title,  
        'dataset_date': dataset_date, # has to be MM/DD/YYYY  
        'groups': iso  
    })

The dataset name should not contain special characters and hence if there is any chance of that, then it needs to be slugified. Slugifying is way of making a string valid within a URL (eg. `ae` replaces `ä`). There are various packages that can do this eg. [awesome-slugify](https://pypi.python.org/pypi/awesome-slugify).

You can add metadata using the standard Python dictionary square brackets eg.

    dataset['name'] = 'My Dataset'

You can also do so by the standard dictionary `update` method, which takes a dictionary eg.

    dataset.update({'name': 'My Dataset'})

Larger amounts of static metadata are best added from files. YAML is very human readable and recommended, while JSON is also accepted eg.

    dataset.update_from_yaml([path])

    dataset.update_from_json([path])

The default path if unspecified is `config/hdx_TYPE_static.yml` for YAML and `config/hdx_TYPE_static.json` for JSON where TYPE is an HDX object's type like dataset or resource eg. `config/hdx_galleryitem_static.json`. The YAML file takes the following form:

    owner_org: "acled"  
    maintainer: "acled"  
    ...  
    tags:  
        - name: "conflict"  
        - name: "political violence"  
    gallery:  
        - title: "Dynamic Map: Political Conflict in Africa"  
          type: "visualization"  
          description: "The dynamic maps below have been drawn from ACLED Version 6."  
    ...

Notice how you can define a gallery with one or more gallery items (each starting with a dash '-') within the file as shown above. You can do the same for resources.

You can check if all the fields required by HDX are populated by calling `check_required_fields` with an optional list of fields to ignore. This will throw an exception if any fields are missing. Before the library posts data to HDX, it will call this method automatically. An example usage:

    resource.check_required_fields(['package_id'])

Once the HDX object is ready ie. it has all the required metadata, you simply call `create_in_hdx` eg.

    dataset.create_in_hdx()

You can delete HDX objects using `delete_from_hdx` and update an object that already exists in HDX with the method `update_in_hdx`. These do not take any parameters or return anything and throw exceptions for failures like the object to delete or update not existing.

### Dataset Specific Operations

A dataset can have resources and a gallery.

![](https://humanitarian.atlassian.net/wiki/download/attachments/8028192/UMLDiagram.png?api=v2)


If you wish to add resources or a gallery, you can supply a list and call the appropriate `add_update_*` function, for example:

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

Calling `add_update_resources` creates a list of HDX Resource objects in dataset and operations can be performed on those objects.

To see the list of resources or gallery items, you use the appropriate `get_*` function eg.

    resources = dataset.get_resources()

If you wish to add one resource or gallery item, you can supply a dictionary or object of the correct type and call the appropriate `add_update_*` function, for example:

    dataset.add_update_resource(resource)

You can delete a Resource or GalleryItem object from the dataset using the appropriate `delete_*` function, for example:

    dataset.delete_galleryitem('GALLERYITEM_TITLE')
    
You can get all the resources from a list of datasets as follows:

    resources = Dataset.get_all_resources(datasets)

#### Dataset Date

Dataset date is a mandatory field in HDX. This date is the date of the data in the dataset, not to be confused with when data was last added/changed in the dataset.

To retrieve the dataset date as a `datetime.datetime` object, you can do:

    dataset_date = dataset.get_dataset_date()
    
The method below allows you to set the dataset's date using a `datetime.datetime` object:

    dataset.set_dataset_date(DATETIME.DATETIME OBJECT)

As a convenience, there are methods that allow getting and setting the dataset date as/from a string. You can supply a [date format](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior). If you don't, the format is assumed to be [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) eg. 2007-01-25T12:00:00 for getting the dataset date. For setting, the date format will be guessed with unambiguous ones handled without error. 

    dataset.get_dataset_date_as_string('FORMAT')
    dataset.set_dataset_date_from_string('DATE', 'FORMAT')

#### Expected Update Frequency

HDX datasets have a mandatory field, the expected update frequency. This is your best guess of how often the dataset will be updated. 

The HDX web interface uses set frequencies: 
    
    Every day
    Every week
    Every two weeks
    Every month
    Every three months
    Every six months
    Every year
    Never
    
Although the API allows much greater granularity (a number of days), you are encouraged to use the options above (avoiding using `Never` if possible). To assist with this, you can use methods that allow this.

The following method will return a textual expected update frequency corresponding to what would be shown in the HDX web interface.

    update_frequency = dataset.get_expected_update_frequency()
    
The method below allows you to set the dataset's expected update frequency using one of the set frequencies above. (It also allows you to pass a number of days cast to a string, but this is discouraged.)

    dataset.set_expected_update_frequency('UPDATE_FREQUENCY')
    
Transforming backwards and forwards between representations can be achieved with this function:
    
    update_frequency = Dataset.transform_update_frequency('UPDATE_FREQUENCY')

#### Location

Each HDX dataset must have at least one location associated with it.

If you wish to get the current location (ISO 3 country codes), you can call the method below:
 
    locations = dataset.get_location()
     
If you want to add a country, you do as shown below. If you don't provide an ISO 3 country code, the text you give will be parsed and converted to an ISO 3 code if possible.
    
    dataset.add_country_location('ISO 3 COUNTRY CODE')
    
If you want to add a list of countries, the following method enables you to do it. If you don't provide ISO 3 country codes, conversion will take place if possible.

    dataset.add_country_locations(['ISO 3','ISO 3','ISO 3'...])
    
If you want to add a continent, you do it as follows. If you don't provide a two letter continent code, then parsing and conversion will occur if possible.

    dataset.add_continent_location('TWO LETTER CONTINENT CODE')

#### Tags

HDX datasets can have tags which help people to find them eg. "COD", "PROTESTS".

If you wish to get the current tags, you can use this method:
 
    tags = dataset.get_tags()
     
If you want to add a tag, you do it like this:
    
    dataset.add_tag('TAG')
    
If you want to add a list of tags, you do it as follows:

    dataset.add_tags(['TAG','TAG','TAG'...])


### Resource Specific Operations

You can download a resource using the `download` function eg.

    url, path = resource.download(FOLDER_TO_DOWNLOAD_TO)
    
If you do not supply `FOLDER_TO_DOWNLOAD_TO`, then a temporary folder is used.

## Working Example

Here we will create a working example from scratch.

First, pip install the library or alternatively add it to a requirements.txt file if you are comfortable with doing so as described above.

Next create a file called `run.py` and copy into it the code below.

    #!/usr/bin/python
    # -*- coding: utf-8 -*-
    '''
    Calls a function that generates a dataset and creates it in HDX.

    '''
    import logging
    from hdx.facades.scraperwiki import facade
    from .my_code import generate_dataset

    logger = logging.getLogger(__name__)


    def main(configuration: dict):
        '''Generate dataset and create it in HDX'''

        dataset = generate_dataset(configuration)
        dataset.create_in_hdx()

    if __name__ == '__main__':
        facade(main, hdx_site='test')

The above file will create in HDX a dataset generated by a function called `generate_dataset` that can be found in the file `my_code.py` which we will now write.

Create a file `my_code.py` and copy into it the code below:

    #!/usr/bin/python
    # -*- coding: utf-8 -*-
    '''
    Generate a dataset

    '''
    import logging
    from hdx.data.dataset import Dataset

    logger = logging.getLogger(__name__)


    def generate_dataset(configuration):
        '''Create a dataset
        '''
        logger.debug('Generating dataset!')

You can then fill out the function `generate_dataset` as required.

## ACLED Example

A complete example can be found here: [https://github.com/mcarans/hdxscraper-acled-africa](https://github.com/mcarans/hdxscraper-acled-africa)

In particular, take a look at the files `run.py`, `acled_africa.py` and the `config` folder.

The ACLED scraper creates a dataset in HDX for [ACLED realtime data](https://data.humdata.org/dataset/acled-conflict-data-for-africa-realtime-2016) if it doesn't already exist, populating all the required metadata. It then creates resources that point to urls of [Excel and csv files for Realtime 2016 All Africa data](http://www.acleddata.com/data/realtime-data-2016/) (or updates the links and metadata if the resources already exist). Finally it creates a gallery item that points to these [dynamic maps and graphs](http://www.acleddata.com/visuals/maps/dynamic-maps/). 

The first iteration of the ACLED scraper was written without the HDX Python library and it became clear looking at this and previous work by others that there are operations that are frequently required and which add unnecessary complexity to the task of coding against HDX. Simplifying the interface to HDX drove the development of the Python library and the second iteration of the scraper was built using it. With the interface using HDX terminology and mapping directly on to datasets, resources and gallery items, the ACLED scraper was faster to develop and is much easier to understand for someone inexperienced in how it works and what it is doing. The challenge with ACLED is that sometimes the urls that the resources point to have not been updated and hence do not work. In this situation, the extensive logging and transparent communication of errors is invaluable and enables action to be taken to resolve the issue as quickly as possible. The static metadata for ACLED is held in human readable files so if it needs to be modified, it is straightforward. This is another feature of the HDX Python library that makes putting data programmatically into HDX a breeze. 
