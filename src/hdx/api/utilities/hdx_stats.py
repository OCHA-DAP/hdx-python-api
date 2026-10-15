"""HDX site-wide statistics"""

from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError


def _call_stats_action(action: str, configuration: Configuration | None) -> dict:
    """Call an HDX statistics action

    Args:
        action: CKAN action name
        configuration: HDX configuration. Defaults to global configuration.

    Returns:
        Statistics dictionary
    """
    if configuration is None:
        configuration = Configuration.read()
    try:
        return configuration.call_remoteckan(action, {})
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
