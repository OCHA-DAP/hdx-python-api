# Use Activity Streams

The **Activity** class wraps CKAN activities, which record who changed what and when.
Activities are read only. You can get the activity stream of a dataset, organization,
location or user:

    activities = dataset.get_activities(**kwargs)
    activities = organization.get_activities(**kwargs)
    activities = location.get_activities(**kwargs)
    activities = user.get_activities(**kwargs)

These return a list of **Activity** objects, each with fields like `timestamp`,
`user_id`, `object_id` and `activity_type` (eg. "changed package"). One page of results
is returned. Its size is set by `limit` (default 31, HDX maximum 100) and where it starts
by `offset`. Passing `all_pages=True` gets every page. Other arguments (`**kwargs`) like
`after`, `before` and `activity_types` depend on the object type and are detailed in the
API documentation. Getting a user's activity stream requires authorisation to view the
user's profile.

You can get the activity stream of all recently added or changed datasets:

    activities = Activity.get_recently_changed_packages(limit=10)

With `all_pages=True`, this stream covers the whole history of HDX so it is capped by
`max_activities` (default 1000). Pass `max_activities=None` to remove the cap.

You can read a single activity, get its data (including a snapshot of the object at the
time of the activity) or get a diff against the object's previous version. HDX requires
permission to edit the object the activity relates to for these:

    activity = Activity.read_from_hdx("ACTIVITY_ID")
    data = activity.get_data()
    diff = activity.get_diff(diff_type="unified")  # or "context" or "html"
