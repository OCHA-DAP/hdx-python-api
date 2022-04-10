"""Facade to simplify project setup that calls project main function with kwargs"""
import logging
from typing import Any, Callable, Optional

import defopt
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent

from hdx.api import __version__
from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)
setup_logging(log_file="errors.log")


def _create_configuration(
    user_agent: Optional[str] = None,
    user_agent_config_yaml: Optional[str] = None,
    user_agent_lookup: Optional[str] = None,
    hdx_url: Optional[str] = None,
    hdx_site: Optional[str] = None,
    hdx_read_only: bool = False,
    hdx_key: Optional[str] = None,
    hdx_config_json: Optional[str] = None,
    hdx_config_yaml: Optional[str] = None,
    project_config_json: Optional[str] = None,
    project_config_yaml: Optional[str] = None,
    hdx_base_config_json: Optional[str] = None,
    hdx_base_config_yaml: Optional[str] = None,
) -> str:
    """
    Create HDX configuration

    Args:
        user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
        user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
        user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
        hdx_url (Optional[str]): HDX url to use. Overrides hdx_site.
        hdx_site (Optional[str]): HDX site to use eg. prod, test.
        hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
        hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
        hdx_config_json (Optional[str]): Path to JSON HDX configuration OR
        hdx_config_yaml (Optional[str]): Path to YAML HDX configuration
        project_config_json (Optional[str]): Path to JSON Project configuration OR
        project_config_yaml (Optional[str]): Path to YAML Project configuration
        hdx_base_config_json (Optional[str]): Path to JSON HDX base configuration OR
        hdx_base_config_yaml (Optional[str]): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.

    Returns:
        str: HDX site url

    """
    arguments = locals()
    return Configuration._create(**arguments)


def facade(projectmainfn: Callable[[Any], None]):
    """Facade to simplify project setup that calls project main function. It infers
    command line arguments from the passed in function using defopt. The function passed
    in should have either type hints or a docstring from which to infer the command
    line arguments.

    Args:
        projectmainfn ((Any) -> None): main function of project

    Returns:
        None
    """

    #
    # Setting up configuration
    #
    func, argv = defopt.bind_known(projectmainfn, cli_options="all")
    site_url = defopt.run(_create_configuration, argv=argv, cli_options="all")

    logger.info("--------------------------------------------------")
    logger.info(f"> Using HDX Python API Library {__version__}")
    logger.info(f"> HDX Site: {site_url}")

    UserAgent.user_agent = Configuration.read().user_agent
    func()