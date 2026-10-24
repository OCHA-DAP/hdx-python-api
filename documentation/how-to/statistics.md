# Get HDX Statistics

General statistics about datasets, organizations and users on HDX can be
obtained as follows:

    from hdx.api.utilities.hdx_stats import get_general_statistics, push_general_stats

    stats = get_general_statistics()

Users with permission to manage basic scheduled tasks can compute the statistics
and push them to analytics:

    pushed_stats = push_general_stats()

HDX sysadmins can get the number of activities of each active user (optionally
between start and end dates) and lists of active and inactive organizations:

    from hdx.api.utilities.hdx_stats import get_organization_statistics, get_user_statistics

    user_stats = get_user_statistics(start_date="2026-01-01", end_date="2026-06-30")
    org_stats = get_organization_statistics()
