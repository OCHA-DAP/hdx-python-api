# -*- coding: utf-8 -*-
"""Facade to simplify project setup that calls project main function"""
import logging

from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent

from hdx.facades import logging_kwargs
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


def facade(projectmainfn, **kwargs):
    # (Callable[[None], None], Any) -> None
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

    logger.info('--------------------------------------------------')
    logger.info('> Using HDX Python API Library %s' % Configuration.apiversion)
    logger.info('> HDX Site: %s' % site_url)

    UserAgent.user_agent = Configuration.read().user_agent

    projectmainfn()
