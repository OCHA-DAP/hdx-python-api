#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Wrapper to simplify collector setup that calls collector main function"""
import logging

from typing import Callable

from hdx.collector import logging_kwargs
from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


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

    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % configuration.get_hdx_site())

    collectormainfn(configuration)
