#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Wrapper that handles ScraperWiki and calls collector main function"""
import logging
import sys

import scraperwiki
from typing import Callable

from hdx.collector import logging_kwargs
from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


def wrapper(collectormainfn: Callable[[Configuration], None], **kwargs) -> bool:
    """Wrapper that handles ScraperWiki and calls collector main function

    Args:
        collectormainfn ((configuration) -> None): main function of collector
        **kwargs: configuration parameters to pass to HDX Configuration class

    Returns:
        bool: True = success, False = failure
    """

    try:
        #
        # Setting up configuration
        #
        configuration = Configuration(**kwargs)

        logger.info('--------------------------------------------------')
        logger.info('> HDX Site: %s' % configuration.get_hdx_site())

        collectormainfn(configuration)

    except Exception as e:
        logger.critical(e, exc_info=True)
        scraperwiki.status('error', 'Collector failed: %s' % sys.exc_info()[0])
        return False
    logger.info('Collector completed successfully.\n')
    scraperwiki.status('ok')
    return True
