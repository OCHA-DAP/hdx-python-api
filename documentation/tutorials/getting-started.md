# Read a Dataset

<!-- Removed broken image (HTTP 404): ![A Quick Example](https://humanitarian.atlassian.net/wiki/download/attachments/6356996/HDXPythonLibrary.gif?version=1&modificationDate=1469520811486&api=v2) -->

Let's start with a simple example that also ensures that the library is working
properly. In this tutorial, we read data from HDX, which requires no account and no API
key. We use virtualenv, a sandbox, so that your Python install is not modified.

1. If you are using the Docker image, you can jump to step 5, otherwise install
virtualenv if not installed:

        pip install virtualenv

    On some Linux distributions, you can do the following instead to install from the
    distribution's official repository:

        sudo apt-get install virtualenv

2. Create a Python 3 virtualenv and activate it:

    On Windows (assuming the Python 3 executable is in your path):

        virtualenv test
        test\Scripts\activate

    On other OSs:

        virtualenv -p python3 test
        source test/bin/activate

3. Install the HDX Python library:

        pip install hdx-python-api

4. If you get errors, it is probably the
[dependencies of the cryptography package](../how-to/install.md)
5. Launch python:

        python

6. Import required classes:

        from hdx.utilities.easy_logging import setup_logging
        from hdx.api.configuration import Configuration
        from hdx.data.dataset import Dataset

7. Setup logging

        setup_logging()

8. Connect to the production HDX server in read-only mode, making sure that you
    replace MyOrg_MyProject with something that describes your organisation and project:

        Configuration.create(hdx_site="prod", user_agent="MyOrg_MyProject", hdx_read_only=True)

9. Read this dataset
[Novel Coronavirus (COVID-19) Cases Data](https://data.humdata.org/dataset/novel-coronavirus-2019-ncov-cases)
     from HDX and view the date of the dataset:

        dataset = Dataset.read_from_hdx("novel-coronavirus-2019-ncov-cases")
        print(dataset.get_time_period())

10. You can search for datasets on HDX and get their resources:

        datasets = Dataset.search_in_hdx("thailand subnational boundaries", rows=10)
        print(datasets)
        resources = Dataset.get_all_resources(datasets)
        print(resources)

11. You can download a resource in the dataset:

        url, path = resources[0].download()
        print("Resource URL %s downloaded to %s" % (url, path))

12. Exit and remove virtualenv:

        exit()
        deactivate

     On Windows:

        rd /s /q test

     On other OSs:

        rm -rf test

To create or update datasets you need an API token and the right to publish for an
organisation. See [Create a Dataset from a CSV](create-a-dataset.md) and
[Obtain your API Key](../how-to/api-key.md).
