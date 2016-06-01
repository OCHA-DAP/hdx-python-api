#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Runner:
---------

Simple runner.

"""
import logging

from hdx.configuration import Configuration
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging()


def wrapper(scrapermainfn, **kwargs):
    """Wrapper"""

    #
    # Setting up configuration
    #
    configuration = Configuration(**kwargs)

    hdx_site = configuration['hdx_site']
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % hdx_site)

    scrapermainfn(configuration)
