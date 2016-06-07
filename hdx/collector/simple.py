#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Wrapper to simplify collector setup that calls collector main function"""
import logging

from typing import Callable

from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


def wrapper(collectormainfn: Callable[[Configuration], None], **kwargs) -> None:
    """Wrapper to simplify collector setup that calls collector main function

    Args:
        collectormainfn ((configuration) -> None): main function of collector
        **kwargs: configuration parameters to pass to HDX Configuration class

    Returns:
        None
    """

    #
    # Setting up configuration
    #
    configuration = Configuration(**kwargs)

    hdx_site = configuration['hdx_site']
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % hdx_site)

    collectormainfn(configuration)
