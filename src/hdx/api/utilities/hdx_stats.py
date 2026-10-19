"""HDX site-wide statistics"""

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError


def _call_stats_action(
    action: str, configuration: Configuration | None, data: dict | None = None
) -> dict:
    """Call an HDX statistics action

    Args:
        action: CKAN action name
        configuration: HDX configuration. Defaults to global configuration.
        data: Parameters to pass to the action. Defaults to None.

    Returns:
        Statistics dictionary
    """
    if configuration is None:
        configuration = Configuration.read()
    try:
        return configuration.call_remoteckan(action, data or {})
    except Exception as e:
        raise HDXError(f"Failed when trying to call {action}!") from e


def get_general_statistics(configuration: Configuration | None = None) -> dict:
    """Get general statistics about datasets, organizations and users on HDX

    Args:
        configuration: HDX configuration. Defaults to global configuration.

    Returns:
        Dictionary with keys datasets, organizations and users
    """
    return _call_stats_action("hdx_general_statistics", configuration)


def push_general_stats(configuration: Configuration | None = None) -> dict:
    """Compute general statistics on HDX and push them to analytics. Requires
    permission to manage basic scheduled tasks.

    Args:
        configuration: HDX configuration. Defaults to global configuration.

    Returns:
        Statistics that were pushed
    """
    return _call_stats_action("hdx_push_general_stats", configuration)


def get_user_statistics(
    start_date: str | None = None,
    end_date: str | None = None,
    configuration: Configuration | None = None,
) -> dict:
    """Get the number of activities of each active user on HDX, optionally
    between start and end dates. Requires sysadmin.

    Args:
        start_date: Count activities on or after this date eg. 2026-01-01. Defaults to None.
        end_date: Count activities on or before this date. Defaults to None.
        configuration: HDX configuration. Defaults to global configuration.

    Returns:
        Dictionary from user id to dictionary with keys id, name and no_of_activities
    """
    data = {}
    if start_date:
        data["start_date"] = start_date
    if end_date:
        data["end_date"] = end_date
    return _call_stats_action("hdx_user_statistics", configuration, data)


def get_organization_statistics(configuration: Configuration | None = None) -> dict:
    """Get active and inactive organizations on HDX. Requires sysadmin.

    Args:
        configuration: HDX configuration. Defaults to global configuration.

    Returns:
        Dictionary with keys active_organizations and inactive_organizations
    """
    return _call_stats_action("hdx_organization_statistics", configuration)
