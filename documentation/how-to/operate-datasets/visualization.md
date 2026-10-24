# Set Custom Visualization

If you want to add a custom visualization to a dataset, you can do this:

        dataset.set_custom_viz(URL)

URL is a string containing the url of your visualization.

You can get any existing visualization like this:

        url = dataset.get_custom_viz()

The return value is a string if a visualization has been set on the dataset,
otherwise it is None.
