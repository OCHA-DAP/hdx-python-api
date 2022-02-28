"""Facade to simplify project setup that calls project main function"""
import logging
from typing import Any, Callable

from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent

from hdx.api import __version__
from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)
setup_logging(log_file="errors.log")


def facade(projectmainfn: Callable[[None], None], **kwargs: Any):
    """Facade to simplify project setup that calls project main function

    Args:
        projectmainfn ((None) -> None): main function of project
        **kwargs: configuration parameters to pass to HDX Configuration class

    Returns:
        None
    """

    #
    # Setting up configuration
    #
    site_url = Configuration._create(**kwargs)

    logger.info("--------------------------------------------------")
    logger.info(f"> Using HDX Python API Library {__version__}")
    logger.info(f"> HDX Site: {site_url}")

    UserAgent.user_agent = Configuration.read().user_agent

    projectmainfn()
