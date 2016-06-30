#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Facade to simplify project setup that calls project main function"""
import logging

from typing import Callable

from hdx.configuration import Configuration
from hdx.facades import logging_kwargs
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


def facade(projectmainfn: Callable[[Configuration], None], **kwargs) -> None:
    """Facade to simplify project setup that calls project main function

    Args:
        projectmainfn ((configuration) -> None): main function of project
        **kwargs: configuration parameters to pass to HDX Configuration class

    Returns:
        None
    """

    #
    # Setting up configuration
    #
    configuration = Configuration(**kwargs)

    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % configuration.get_hdx_site())

    projectmainfn(configuration)
