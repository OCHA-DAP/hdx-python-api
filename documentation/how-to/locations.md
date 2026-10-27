# Manage Locations

The **Location** class wraps HDX locations (CKAN groups eg. countries, "world"). Reading
locations does not require any special permissions, so these are the operations you will
typically use. Creating, updating and deleting locations is restricted to HDX sysadmins
and works the same way as for other HDX objects (see
[Use HDX Objects](objects.md)).

You can read a single location:

    location = Location.read_from_hdx("LOCATION_ID_OR_NAME")

You can get all location names in HDX:

    location_names = Location.get_all_location_names(**kwargs)

Passing `all_fields=True` (plus optionally `include_extras=True` etc.) returns the full
location dictionaries instead of just names. Various additional arguments (`**kwargs`)
can be supplied and are detailed in the API documentation.

You can fetch the countries that are currently active on HDX's Data Grid (a live lookup,
since Data Grid membership changes over time, rather than a fixed list):

    country_codes = Location.get_data_grid_countries()

This returns a sorted list of 3-letter location codes.

HDX sysadmins can get the Data Grid of a country and clear HDX's cached copy of
it:

    data_grid = Location.get_data_grid("AFG")
    Location.invalidate_data_grid("AFG")

**get_data_grid** returns a dictionary with keys including `title`, `date`,
`stats` and `categories` (or None if not found). Each category has
`subcategories`, each with a list of `datasets`. Complementary datasets have
`is_complementary` set to True.

You can get the datasets belonging to a location as follows:

    datasets = location.get_datasets(**kwargs)

Various additional arguments (`**kwargs`) can be supplied. These are detailed in the API
documentation.

You can autocomplete a location name:

    matches = Location.autocomplete("NAME")

For simple name/code lookups against a cached list of valid locations (eg. converting
between a country name and its HDX code) rather than the full **Location** object, see
the **Locations** helper class instead.
