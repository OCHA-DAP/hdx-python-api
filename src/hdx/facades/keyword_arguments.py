"""Facade to simplify project setup that calls project main function with kwargs"""
import logging
from typing import Callable, Any

from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent

from hdx.facades import logging_kwargs
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


def facade(projectmainfn: Callable[[Any], None], **kwargs: Any):
    """Facade to simplify project setup that calls project main function

    Args:
        projectmainfn ((Any) -> None): main function of project
        **kwargs: configuration parameters to pass to HDX Configuration & other parameters to pass to main function

    Returns:
        None
    """

    #
    # Setting up configuration
    #
    site_url = Configuration._create(**kwargs)

    logger.info("--------------------------------------------------")
    logger.info(f"> Using HDX Python API Library {Configuration.apiversion}")
    logger.info(f"> HDX Site: {site_url}")

    UserAgent.user_agent = Configuration.read().user_agent

    projectmainfn(**kwargs)
