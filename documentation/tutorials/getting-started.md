# A Quick Example

<!-- Removed broken image (HTTP 404): ![A Quick Example](https://humanitarian.atlassian.net/wiki/download/attachments/6356996/HDXPythonLibrary.gif?version=1&modificationDate=1469520811486&api=v2) -->

Let's start with a simple example that also ensures that the library is working
properly. In this tutorial, we use virtualenv, a sandbox, so that your Python install is
not modified.

1. If you just want to read data from HDX, then an API key is not necessary. However,
if you want to write data to HDX, then you need to register on the website to obtain an
API key. Please see above about where to find it on the website. Once you have it, then
put it into a file in your home directory:

        cd ~
        echo "hdx_key: \"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\"" > .hdx_configuration.yaml

2. If you are using the Docker image, you can jump to step 6, otherwise install
virtualenv if not installed:

        pip install virtualenv

    On some Linux distributions, you can do the following instead to install from the
    distribution's official repository:

        sudo apt-get install virtualenv

3. Create a Python 3 virtualenv and activate it:

    On Windows (assuming the Python 3 executable is in your path):

        virtualenv test
        test\Scripts\activate

    On other OSs:

        virtualenv -p python3 test
        source test/bin/activate

4. Install the HDX Python library:

        pip install hdx-python-api

5. If you get errors, it is probably the
[dependencies of the cryptography package](../how-to/install.md)
6. Launch python:

        python

7. Import required classes:

        from hdx.utilities.easy_logging import setup_logging
        from hdx.api.configuration import Configuration
        from hdx.data.dataset import Dataset

8. Setup logging

        setup_logging()

9. Use configuration defaults.

    If you only want to read data, then connect to the production HDX
    server, making sure that you replace MyOrg_MyProject with something that
    describes your organisation and project:

        Configuration.create(hdx_site="prod", user_agent="MyOrg_MyProject", hdx_read_only=True)

    If you want to write data, then for experimentation, do not use the
    production HDX server. Instead you can use one of the test servers.
    Assuming you have an API key stored in a file **.hdxkey** in the
    current user's home directory:

        Configuration.create(hdx_site="stage", user_agent="MyOrg_MyProject")

10. Read this dataset
[Novel Coronavirus (COVID-19) Cases Data](https://data.humdata.org/dataset/novel-coronavirus-2019-ncov-cases)
     from HDX and view the date of the dataset:

        dataset = Dataset.read_from_hdx("novel-coronavirus-2019-ncov-cases")
        print(dataset.get_time_period())

11. You can search for datasets on HDX and get their resources:

        datasets = Dataset.search_in_hdx("thailand subnational boundaries", rows=10)
        print(datasets)
        resources = Dataset.get_all_resources(datasets)
        print(resources)

12. You can download a resource in the dataset:

        url, path = resources[0].download()
        print("Resource URL %s downloaded to %s" % (url, path))

13. If you have an API key, you can write to HDX. You can try it out on a test
    server. With a dataset to which you have permissions, change the dataset date:

        dataset = Dataset.read_from_hdx("ID OR NAME OF DATASET")
        print(dataset.get_time_period())  # record this
        dataset.set_time_period("2015-07-26")
        print(dataset.get_time_period())
        dataset.update_in_hdx()

14. You can view it on HDX before changing it back (if you have an API key):

        dataset.set_time_period("PREVIOUS DATE")
        dataset.update_in_hdx()

15. If you are storing your data on HDX, you can upload a new file to a
    resource:

        resource = dataset.get_resource(0)
        resource.set_file_to_upload("PATH TO FILE")
        resource.update_in_hdx()

16. Alternatively, if you are using a URL to point to data held externally from
    HDX, you can mark that the data has been updated before updating the
    resource or parent dataset:

        resource = dataset.get_resource(2)
        resource.mark_data_updated()
        dataset.update_in_hdx()

17. Exit and remove virtualenv:

        exit()
        deactivate

     On Windows:

        rd /s /q test

     On other OSs:

        rm -rf test