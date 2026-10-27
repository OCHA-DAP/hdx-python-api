# IDMC Example

A complete example can be found here: <https://github.com/OCHA-DAP/hdx-scraper-idmc>

The IDMC scraper creates a dataset per country in HDX, populating all the
required metadata. It then creates resources with files held on the HDX
filestore.

In particular, take a look at the files **run.py**, **idmc.py** and the
**config** folder. Do not run it unchanged as it may overwrite the existing
datasets in the IDMC organisation (although it will most probably fail as you
will not have permissions to modify anything in that organisation). You can use
it as a basis for your code renaming and modifying **idmc.py** as needed and
updating metadata in **config/hdx_dataset_static.yaml** appropriately.
