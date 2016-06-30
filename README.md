### HDX Python Library
[![Build Status](https://travis-ci.org/mcarans/hdx-python-api.svg?branch=master&ts=1)](https://travis-ci.org/mcarans/hdx-python-api) [![Coverage Status](https://coveralls.io/repos/github/mcarans/hdx-python-api/badge.svg?branch=master&ts=1)](https://coveralls.io/github/mcarans/hdx-python-api?branch=master)

The HDX Python Library is designed to enable you to easily develop code that interacts with the Humanitarian Data Exchange platform. The major goal of the library is to make interacting with HDX as simple as possible for the end user.

For more about the purpose and design philosophy, please visit [HDX Python Library](https://humanitarian.atlassian.net/wiki/display/HDX/HDX+Python+Library).
 
- [Usage](#usage)
- [Creating the API Key File](#creating-the-api-key-file)
- [Starting the Data Collector](#starting-the-data-collector)
- [Setting up the Configuration](#setting-up-the-configuration)
- [Configuring Logging](#configuring-logging)
- [Operations on HDX Objects](#operations-on-hdx-objects)
- [Dataset Specific Operations](#dataset-specific-operations)
- [Full Example](#full-example)

### Usage
The API documentation can be found here: [http://mcarans.github.io/hdx-python-api/](http://mcarans.github.io/hdx-python-api/). The code for the library is here: [https://github.com/mcarans/hdx-python-api](https://github.com/mcarans/hdx-python-api).

### Creating the API Key File

The first task is to create an API key file. By default this is assumed to be called .hdxkey and is located in the current user's home directory (~). Assuming you are using a desktop browser, the API key is obtained by:

1. Browse to the [HDX website](https://data.humdata.org/)
2. Left click on LOG IN in the top right of the web page if not logged in and log in
3. Left click on your username in the top right of the web page and select PROFILE from the drop down menu
4. Scroll down to the bottom of the profile page
5. Copy the API key which will be of the form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
6. Paste the API key into a text file
7. Save the text file with filename ".hdxkey" in the current user's home directory

### Starting the Data Collector

To include the HDX Python library in your project, pip install the line below or add the following to your requirements.txt file:

    git+git://github.com/mcarans/hdx-python-api.git#egg=hdx-python-api

The easiest way to get started is to use the wrappers and configuration defaults. The wrappers set up both logging and HDX configuration.

### Default Configuration for Wrappers

The default configuration loads an internal HDX configuration located within the library, and assumes that there is an API key file called .hdxkey in the current user's home directory and a YAML collector configuration located relative to your working directory at config/collector_configuration.yml which you must create. The collector configuration is used for any configuration specific to your collector.

The default logging configuration reads a configuration file internal to the library that sets up an coloured console handler outputting at DEBUG level, a file handler writing to errors.log at ERROR level and an SMTP handler sending an email in the event of a CRITICAL error. It assumes that you have created a file config/smtp_configuration.yml which contains parameters of the form:

    handlers:  
        error_mail_handler:  
            toaddrs: EMAIL_ADDRESSES  
            subject: "COLLECTOR FAILED: MY_COLLECTOR_NAME"


### Wrappers

You will most likely just need the simple wrapper. If you are in the HDX team, you may need to use the ScraperWiki wrapper which reports status to that platform (in which case replace "simple" with "scraperwiki" in the code below):

    from hdx.collector.simple import wrapper

    def main(configuration):  
        ***YOUR CODE HERE***

    if __name__ == '__main__':  
        wrapper(main)

The configuration is passed to your main function in the "configuration" argument above.


### Customising the Configuration


It is possible to pass configuration parameters in the wrapper call eg.

    wrapper(main, hdx_site = HDX_SITE_TO_USE, hdx_key_file = LOCATION_OF_HDX_KEY_FILE, hdx_config_yaml=PATH_TO_HDX_YAML_CONFIGURATION, 

    collector_config_dict = {'MY_PARAMETER', 'MY_VALUE'})

If you did not need a collector configuration, you could simply provide an empty dictionary eg.

    wrapper(main, collector_config_dict = {})

If you do not use the wrapper, you can use the Configuration class directly, passing in appropriate keyword arguments ie.

    from hdx.configuration import Configuration  
    ...  
    cfg = Configuration(KEYWORD ARGUMENTS)

KEYWORD ARGUMENTS can be:

| Choose |       Argument      |     Type     |               Value                |                 Default                |
|--------|---------------------|--------------|------------------------------------|----------------------------------------|
|        |hdx_site             |Optional[bool]|HDX site to use eg. prod, test      |test                                    |
|        |hdx_key_file         |Optional[str] |Path to HDX key file ~/.hdxkey      |                                        |                                  
|One of: |hdx_config_dict      |dict          |HDX configuration dictionary        |                                        |
|        |hdx_config_json      |str           |Path to JSON HDX configuration      |                                        |
|        |hdx_config_yaml      |str           |Path to YAML HDX configuration      |Library's internal hdx_configuration.yml|
|One of: |collector_config_dict|dict          |Collector configuration dictionary  |                                        |
|        |collector_config_json|str           |Path to JSON Collector configuration|                                        |
|        |collector_config_yaml|str           |Path to YAML Collector configuration|config/collector_configuration.yml      |

### Configuring Logging

If you wish to change the logging configuration from the defaults, you will need to call _setup_logging_ with arguments unless you have used the simple or ScraperWiki wrappers, in which case you must update the hdx.collector module variable logging_kwargs before importing the wrapper.

If not using wrapper:

    from hdx.logging import setup_logging  
    ...  
    logger = logging.getLogger(__name__)  
    setup_logging(KEYWORD ARGUMENTS)

If using wrapper:

    from hdx.collector import logging_kwargs

    logging_kwargs.update(DICTIONARY OF KEYWORD ARGUMENTS)  
    from hdx.collector.simple import wrapper

KEYWORD ARGUMENTS can be:

| Choose |      Argument     |Type|                 Value                  |                   Default                  |
|--------|-------------------|----|----------------------------------------|--------------------------------------------|
|One of: |logging_config_dict|dict|Logging configuration dictionary        |                                            |
|        |logging_config_json|str |Path to JSON Logging configuration      |                                            |
|        |logging_config_yaml|str |Path to YAML Logging configuration      |Library's internal logging_configuration.yml|
|One of: |smtp_config_dict   |dict|Email|Logging configuration dictionary  |                                            |
|        |smtp_config_json   |str |Path to JSON Email Logging configuration|                                            |  
|        |smtp_config_yaml   |str |Path to YAML Email Logging configuration|config/smtp_configuration.yml               |

To use logging in your files, simply add the line below to the top of each Python file:

    logger = logging.getLogger(__name__)

Then use the logger like this:

    logger.debug('DEBUG message')  
    logger.info('INFORMATION message')  
    logger.warning('WARNING message')  
    logger.error('ERROR message')  
    logger.critical('CRITICAL error message')

### Operations on HDX Objects

You can create an HDX Object, such as a dataset, resource or gallery item by calling the constructor with a configuration, which is required, and an optional dictionary containing metadata. For example:

    dataset = Dataset(configuration, {  
        'name': slugified_name,  
        'title': title,  
        'dataset_date': dataset_date, # has to be MM/DD/YYYY  
        'groups': iso  
    })

You can add metadata using the standard Python dictionary square brackets eg.

    dataset['name'] = 'My Dataset'

You can also do so by the standard dictionary _update_ method, which takes a dictionary eg.

    dataset.update({'name': 'My Dataset'})

Larger amounts of static metadata are best added from files. YAML is very human readable and recommended, while JSON is also accepted eg.

    dataset.update_yaml([path])

    dataset.update_json([path])

The default path if unspecified is config/hdx_TYPE_static.yml for YAML and config/hdx_TYPE_static.json for JSON where TYPE is an HDX object's type like dataset or resource eg. config/hdx_galleryitem_static.json. The YAML file takes the following form:

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

You can check if all the fields required by HDX are populated by calling _check_required_fields_ with an optional list of fields to ignore. This will throw an exception if any fields are missing. Before the library posts data to HDX, it will call this method automatically. An example usage:

    resource.check_required_fields(['package_id'])

Once the HDX object is ready ie. it has all the required metadata, you simply call _create_in_hdx_ eg.

    dataset.create_in_hdx()

You can delete HDX objects using _delete_from_hdx_ and update an object that already exists in HDX with the method _update_in_hdx_. These do not take any parameters or return anything and throw exceptions for failures like the object to delete or update not existing.

You can read an existing HDX object with the static _read_from_hdx_ method which takes a configuration and an identifier parameter and returns the an object of the appropriate HDX object type eg. Dataset or None depending upon whether the object was read eg.

    dataset = Dataset.read_from_hdx(configuration, 'DATASET_ID_OR_NAME')

### Dataset Specific Operations

A dataset can have resources and a gallery.

![](https://humanitarian.atlassian.net/wiki/download/attachments/8028192/UMLDiagram.png?api=v2)


If you wish to add resources or a gallery, you can supply a list and call the appropriate _add_update_*_ function, for example:

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

Calling _add_update_resources_ creates a list of HDX Resource objects in dataset and operations can be performed on those objects.

To see the list of resources or gallery items, you use the appropriate _get_*_ function eg.

    resources = dataset.get_resources()

If you wish to add one resource or gallery item, you can supply a dictionary or object of the correct type and call the appropriate _add_update_*_ function, for example:

    dataset.add_update_resource(resource)

You can delete a Resource or GalleryItem object from the dataset using the appropriate _delete_*_ function, for example:

    dataset.delete_galleryitem('GALLERYITEM_TITLE')

### Full Example

An example that puts all this together can be found here: [https://github.com/mcarans/hdxscraper-acled-africa](https://github.com/mcarans/hdxscraper-acled-africa)

In particular, take a look at the files run.py, acled_africa.py and the config folder.

HDX site to use eg. prod, test
