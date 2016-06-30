#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Facade that handles ScraperWiki and calls project main function"""
import logging
import sys

import scraperwiki
from typing import Callable

from hdx.configuration import Configuration
from hdx.facades import logging_kwargs
from hdx.logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(**logging_kwargs)


def facade(projectmainfn: Callable[[Configuration], None], **kwargs) -> bool:
    """Facade that handles ScraperWiki and calls project main function

    Args:
        projectmainfn ((configuration) -> None): main function of project
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

        projectmainfn(configuration)

    except Exception as e:
        logger.critical(e, exc_info=True)
        scraperwiki.status('error', 'Run failed: %s' % sys.exc_info()[0])
        return False
    logger.info('Run completed successfully.\n')
    scraperwiki.status('ok')
    return True
