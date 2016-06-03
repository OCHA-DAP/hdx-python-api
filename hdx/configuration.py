#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Configuration for HDX"""
import logging
from collections import UserDict
from os.path import expanduser, join

from typing import Optional

from hdx.utilities.loader import load_yaml, load_json, script_dir_plus_file
from .utilities.dictionary import merge_two_dictionaries

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class Configuration(UserDict):
    """Configuration for HDX

    Args:
        hdx_key_file (Optional[str]): Path to HDX key file. Defaults to ~/.hdxkey
        **kwargs: See below
        hdx_config_dict (dict): HDX configuration dictionary OR
        hdx_config_json (str): Path to JSON HDX configuration OR
        hdx_config_yaml (str): Path to YAML HDX configuration. Defaults to internal hdx_configuration.yml.
        scraper_config_dict (dict): Scraper configuration dictionary OR
        scraper_config_json (str): Path to JSON Scraper configuration OR
        scraper_config_yaml (str): Path to YAML Scraper configuration. Defaults to internal scraper_configuration.yml.
    """

    def __init__(self, hdx_key_file: Optional[str] = join('%s' % expanduser("~"), '.hdxkey'), **kwargs):
        super(Configuration, self).__init__()
        hdx_config_found = False
        hdx_config_dict = kwargs.get('hdx_config_dict', None)
        if hdx_config_dict:
            hdx_config_found = True
            logger.info('Loading HDX configuration from dictionary')

        hdx_config_json = kwargs.get('hdx_config_json', '')
        if hdx_config_json:
            if hdx_config_found:
                raise ConfigurationError('More than one HDX configuration file given!')
            hdx_config_found = True
            logger.info('Loading HDX configuration from: %s' % hdx_config_json)
            hdx_config_dict = load_json(hdx_config_json)

        hdx_config_yaml = kwargs.get('hdx_config_yaml', '')
        if hdx_config_found:
            if hdx_config_yaml:
                raise ConfigurationError('More than one HDX configuration file given!')
        else:
            if not hdx_config_yaml:
                logger.info('No HDX configuration parameter. Using default.')
                hdx_config_yaml = script_dir_plus_file('hdx_configuration.yml', Configuration)
            logger.info('Loading HDX configuration from: %s' % hdx_config_yaml)
            hdx_config_dict = load_yaml(hdx_config_yaml)

        scraper_config_found = False
        scraper_config_dict = kwargs.get('scraper_config_dict', '')
        if scraper_config_dict:
            scraper_config_found = True
            logger.info('Loading scraper configuration from dictionary')

        scraper_config_json = kwargs.get('scraper_config_json', '')
        if scraper_config_json:
            if scraper_config_found:
                raise ConfigurationError('More than one scraper configuration file given!')
            scraper_config_found = True
            logger.info('Loading scraper configuration from: %s' % scraper_config_json)
            scraper_config_dict = load_json(scraper_config_json)

        scraper_config_yaml = kwargs.get('scraper_config_yaml', '')
        if scraper_config_found:
            if scraper_config_yaml:
                raise ConfigurationError('More than one scraper configuration file given!')
        else:
            if not scraper_config_yaml:
                logger.info('No scraper configuration parameter. Using default.')
                scraper_config_yaml = join('config', 'scraper_configuration.yml')
            logger.info('Loading scraper configuration from: %s' % scraper_config_yaml)
            scraper_config_dict = load_yaml(scraper_config_yaml)

        self.data = merge_two_dictionaries(hdx_config_dict, scraper_config_dict)

        if 'hdx_site' not in self.data:
            raise ConfigurationError('hdx_site not defined in configuration!')

        self.data['api_key'] = self.load_api_key(hdx_key_file)

    def get_api_key(self) -> str:
        """

        Returns:
            str: HDX api key

        """
        return self.data['api_key']

    def get_hdx_site(self) -> str:
        """

        Returns:
            str: HDX web site url

        """
        return self.data['hdx_site']

    @staticmethod
    def load_api_key(path: str) -> str:
        """
        Load configuration parameters.

        Args:
            path (str): Path to HDX key

        Returns:
            str: HDX api key

        """
        with open(path, 'rt') as f:
            apikey = f.read().replace('\n', '')
        if not apikey:
            raise (ValueError('HDX api key is empty!'))
        return apikey
