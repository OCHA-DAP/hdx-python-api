# Create a Dataset from a CSV

In this tutorial, we go from a CSV file to a complete HDX dataset, stopping just before
the upload. Everything up to that point is a dry run that needs no account, so anyone
can follow it. Publishing for real requires an API token and the right to publish for an
organisation on HDX, which is covered at the end.

The data in this tutorial is made up. Do not upload it to HDX.

## Before you start

You need Python 3.11 or later. In a new folder, create and activate a virtual
environment, install the library and start Python:

    python -m venv hdx-tutorial
    source hdx-tutorial/bin/activate
    pip install hdx-python-api
    python

On Windows, activate the environment with `hdx-tutorial\Scripts\activate` instead. If
you already followed [Read a Dataset](getting-started.md), you can reuse that
environment.

## 1. Prepare the CSV

Save the following as **bel_example_indicators.csv** in the folder from which you launch
Python:

    date,admin1_name,indicator,value
    2025-01-31,Brussels,example_index,12.5
    2025-01-31,Flanders,example_index,9.1
    2025-01-31,Wallonia,example_index,10.4
    2025-02-28,Brussels,example_index,13.0
    2025-02-28,Flanders,example_index,9.4
    2025-02-28,Wallonia,example_index,10.2

CSV is the format HDX recommends for tabular data. HDX also asks that the resource name
and the file name are the same, in lowercase with underscores, and that country files
start with the ISO3 country code. See the
[HDX pipeline recommendations](https://docs.humdata.org/build/hdx-pipeline-recommendations)
for the full conventions, including the limit on file size.

## 2. Connect to HDX in read-only mode

Building a dataset needs HDX's lists of approved tags and valid locations, which the
library reads from HDX. A read-only connection to the production server is enough, and
it needs no API key:

    from hdx.utilities.easy_logging import setup_logging
    from hdx.api.configuration import Configuration
    from hdx.data.dataset import Dataset

    setup_logging()
    Configuration.create(hdx_site="prod", user_agent="tutorial_create_dataset", hdx_read_only=True)

The user agent identifies your code to HDX and must always be supplied. Keep this name
for the tutorial. In your own code, use one that describes your organisation and
project.

## 3. Build the dataset

    dataset = Dataset(
        {
            "name": "bel-example-indicators",
            "title": "Belgium - Example Indicators",
            "notes": "Made-up indicators used to demonstrate dataset creation.",
            "dataset_source": "Example source",
            "methodology": "Other",
            "license_id": "cc-by",
            "private": True,
            "owner_org": "00000000-0000-0000-0000-000000000000",
            "maintainer": "00000000-0000-0000-0000-000000000001",
        }
    )
    dataset.set_expected_update_frequency("Every month")
    dataset.add_country_location("BEL")
    dataset.add_tag("economics")

A few notes on these choices:

- The dataset name is the end of its web address: lowercase, with hyphens and the ISO3
  code for the location. The title starts with the country name followed by a hyphen
  and leaves out dates and the word "data".
- `owner_org` and `maintainer` are the identifiers of an HDX organisation and user. The
  zeros are placeholders, because you can only look up real ones with an account.
- Tags must come from HDX's approved list. Try `dataset.add_tag("not-a-real-tag")`: it
  returns the tag in its second list, which holds the tags that were rejected, and logs an
  error message. The error is expected here: the tag is dropped and nothing is raised.
- Avoid the update frequencies "As needed" and "Never" where you can, because HDX uses
  this field to judge how up to date a dataset is. See
  [Set Expected Update Frequency](../how-to/operate-datasets/update-frequency.md).

## 4. Generate the resource from the CSV

`generate_resource` writes the rows to a file and adds a resource to the dataset. Given
the name of the date column, it also sets the time period of the dataset:

    import csv
    import tempfile

    with open("bel_example_indicators.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    added, info = dataset.generate_resource(
        tempfile.mkdtemp(),
        "bel_example_indicators.csv",
        rows,
        {"name": "bel_example_indicators.csv", "description": "Example indicators"},
        headers=list(rows[0]),
        datecol="date",
    )
    print(added)
    print(dataset.get_time_period())
    print(dataset.get_resources())

The time period runs from the earliest date in the column to the end of the day of the
latest date, and the dates are treated as UTC. See
[Generate Resources](../how-to/operate-datasets/resource-generation.md) for the other
ways of generating resources.

## 5. Validate

Check that the metadata is complete:

    dataset.check_required_fields()

Nothing is printed when everything is present. To see what a failure looks like, remove a
field and check again:

    del dataset["dataset_source"]
    dataset.check_required_fields()

This raises `HDXError: Field dataset_source is missing in dataset!`. Put the field back
before continuing:

    dataset["dataset_source"] = "Example source"

## 6. Look at what would be sent

    print(dataset.data)
    print(dataset.get_resources())

This is everything the library would send to HDX: the dataset metadata and the resource
with its file. The dry run ends here, and nothing has been written to HDX.

## 7. Where a dry run stops

The call that publishes the dataset is:

    dataset.create_in_hdx()

You need an API token and the right to create datasets for your organisation (editor or
admin). Do not run it in this tutorial: the connection from step 2 has no token, and the
data is made up.

When a call to HDX fails, the library raises an error whose message hides the reason,
for example `HDXError: Failed when trying to create bel-example-indicators! (POST)`.
The reason is attached to it, and you can print it like this:

    try:
        dataset.create_in_hdx()
    except Exception as e:
        print(repr(e.__cause__))

For example, an account with a valid token that does not belong to an organisation was
refused on the HDX demo server with
`Access denied: User <username> not authorised to create packages`. See
[Obtain your API Key](../how-to/api-key.md).

## 8. For organisation members

The steps in this section need publishing rights, so they have not been run end to end
for this tutorial. If something here does not work for you, please
[open an issue](https://github.com/OCHA-DAP/hdx-python-api/issues). If you are a member
of an organisation:

1. Get your API token, as described in [Obtain your API Key](../how-to/api-key.md), and
   save it in a file named **.hdx_configuration.yaml** in your home directory. Note the
   space after the colon:

        hdx_key: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

2. Replace the placeholder `owner_org` and `maintainer` with real identifiers. For
   example:

        from hdx.data.user import User

        organisations = User.get_current_user_organization_dicts("create_dataset")
        dataset["owner_org"] = organisations[0]["id"]
        dataset["maintainer"] = User.get_current_user()["id"]

3. Rehearse on a test server before using the production server. Do not use
   `hdx_read_only=True`, and choose the site when you create the configuration, for
   example `hdx_site="demo"`. A key created on the production site works on `demo`
   about a week later, because `demo` is refreshed from production weekly (anything
   else on it is lost at that point). The `stage` and `feature` servers are refreshed
   rarely, and you must create a separate API key there, which also means asking HDX for
   access.

4. When the rehearsal behaves as expected, run the same code with `hdx_site="prod"`.
   Keep `private` set to `True` until you have checked how the dataset looks on HDX.

Running the same code again later updates the dataset instead of creating a second one.
To schedule it, see [Build a project Framework](../how-to/framework.md) and
[Build a Docker image](../how-to/docker.md).
