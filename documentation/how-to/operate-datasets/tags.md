# Set Tags

HDX datasets can have tags which help people to find them eg. "common operational
dataset - cod", "refugees". These tags come from a predefined set of
[approved tags](https://docs.google.com/spreadsheets/d/e/2PACX-1vQD3ba751XbWS5GVwdJmzOF9mc7dnm56hE2U8di12JnpYkdseILmjfGSn1W7UVQzmHKSd6p8FWaXdFL/pub?gid=1768359211&single=true&output=csv).
If you add tags that are not in the approved list, the library attempts to map them to
approved tags based on a spreadsheet of tag mappings.


If you wish to get the current tags, you can use this method:

    tags = dataset.get_tags()

If you want to add a tag, you do it like this:

    dataset.add_tag("TAG")

If you want to add a list of tags, you do it as follows:

    dataset.add_tags(["TAG","TAG","TAG"...])

To obtain the predefined set of approved tags:

    approved_tags = Vocabulary.approved_tags()
