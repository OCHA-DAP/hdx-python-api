# Set Expected Update Frequency

HDX datasets have a mandatory field, the expected update frequency. This is your best
guess of how often the dataset will be updated.

The HDX web interface uses set frequencies:

    Every day
    Every week
    Every two weeks
    Every month
    Every three months
    Every six months
    Every year
    As needed
    Never

Although the API allows much greater granularity (a number of days), you are encouraged
to use the options above (avoiding using **Never** and **As needed** if possible as
this field helps determine how up to date datasets are). To assist with this, you can
use certain Dataset methods outlined below.

The following method will return a textual expected update frequency corresponding to
what would be shown in the HDX web interface.

    update_frequency = dataset.get_expected_update_frequency()

The method below allows you to set the dataset's expected update frequency using one of
the set frequencies above. (It also allows you to pass a number of days as a string or
integer, but this is discouraged.)

    dataset.set_expected_update_frequency("UPDATE_FREQUENCY")

A list of valid update frequencies can be found using:

    Dataset.list_valid_update_frequencies()

Transforming backwards and forwards between representations can be achieved with this
function:

    update_frequency = Dataset.transform_update_frequency("UPDATE_FREQUENCY")