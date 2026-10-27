# Manage Vocabularies

The **Vocabulary** class enables you to manage CKAN vocabularies, creating, deleting and
updating (as for other HDX objects) according to your permissions.

You can optionally initialise a Vocabulary with dictionary, name and tags:

    vocabulary = Vocabulary(name="myvocab", tags=["TAG","TAG","TAG"...])
    vocabulary = Vocabulary({"name": "myvocab", tags=[{"name": TAG"}, {"name": TAG"}...])

If you wish to get the current tags, you can use this method:

    tags = vocabulary.get_tags()

If you want to add a tag, you do it like this:

    vocabulary.add_tag("TAG")

If you want to add a list of tags, you do it as follows:

    vocabulary.add_tags(["TAG","TAG","TAG"...])
