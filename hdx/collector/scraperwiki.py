#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Wrapper that handles ScraperWiki and calls scraper main function"""
import logging
import sys

import scraperwiki
from typing import Callable

from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


def wrapper(scrapermainfn: Callable[[Configuration], None], **kwargs) -> bool:
    """Wrapper that handles ScraperWiki and calls scraper main function

    Args:
        scrapermainfn ((configuration) -> None): main function of scraper
        **kwargs: configuration parameters to pass to HDX Configuration class

    Returns:
        bool: True = success, False = failure
    """

    try:
        #
        # Setting up configuration
        #
        configuration = Configuration(**kwargs)

        hdx_site = configuration['hdx_site']
        logger.info('--------------------------------------------------')
        logger.info('> HDX Site: %s' % hdx_site)

        scrapermainfn(configuration)

    except Exception as e:
        logger.critical(e, exc_info=True)
        scraperwiki.status('error', 'Scraper failed: %s' % sys.exc_info()[0])
        return False
    logger.info('Scraper completed successfully.\n')
    scraperwiki.status('ok')
    return True
