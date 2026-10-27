# Set Location

Each HDX dataset must have at least one location associated with it.

If you wish to get the current location(s) as ISO 3 country codes, you can call the
method below:

    locations = dataset.get_location_iso3s()

If you wish to get the current location name(s), you can call the method below:

    locations = dataset.get_location_names()

If you want to add a country, you do as shown below. If you don't provide an ISO 3
country code, the text you give will
be parsed and converted to an ISO 3 code if it is a valid country name.

    dataset.add_country_location("ISO 3 COUNTRY CODE")

If you want to add a list of countries, the following method enables you to do it. If
you don"t provide ISO 3 country codes, conversion will take place where valid country
names are found.

    dataset.add_country_locations(["ISO 3","ISO 3","ISO 3"...])

If you want to add a region, you do it as follows. If you don't provide a three digit
[UNStats M49](https://unstats.un.org/unsd/methodology/m49/overview/) region code, then
parsing and conversion will
occur if a valid region name is supplied.

    dataset.add_region_location("M49 REGION CODE")

**add_region_location** accepts regions, intermediate regions or subregions as
specified on the
[UNStats M49](https://unstats.un.org/unsd/methodology/m49/overview/) website.

If you want to add any other kind of location (which must be in this
[list of valid locations](https://data.humdata.org/api/action/group_list?all_fields=true)),
you do as shown below.

    dataset.add_other_location("LOCATION")