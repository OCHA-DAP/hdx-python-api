#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
ScraperWiki:
---------

ScraperWiki runner.

"""
import logging
import sys

import scraperwiki

from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


def wrapper(scrapermainfn, **kwargs):
    """Wrapper"""

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
