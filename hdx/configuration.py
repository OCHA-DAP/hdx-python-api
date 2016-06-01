#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
LOAD CONFIGURATION:
------------------

Script designed to load the configuration file
from disk.

"""
import collections
from os.path import expanduser, join
import logging

from hdx.utilities.loader import load_yaml, load_and_merge_yaml, load_json, load_and_merge_json, script_dir_plus_file
from .utilities.dictionary import merge_two_dictionaries

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class Configuration(collections.UserDict):
    def __init__(self, hdx_key_file: str = '%s/.hdxkey' % expanduser("~"), **kwargs):
        super(Configuration, self).__init__()

        hdx_config_found = False
        hdx_config_dict = kwargs.get('hdx_config_dict', None)
        if hdx_config_dict:
            hdx_config_found = True
            logger.info('Loading HDX configuration from dictionary')

        hdx_config_json = kwargs.get('hdx_config_json', None)
        if hdx_config_json:
            if hdx_config_found:
                raise ConfigurationError('More than one HDX configuration file given!')
            hdx_config_found = True
            logger.info('Loading HDX configuration from: %s' % hdx_config_json)
            hdx_config_dict = load_json(hdx_config_json)

        hdx_config_yaml = kwargs.get('hdx_config_yaml', None)
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
        scraper_config_dict = kwargs.get('scraper_config_dict', None)
        if scraper_config_dict:
            scraper_config_found = True
            logger.info('Loading scraper configuration from dictionary')

        scraper_config_json = kwargs.get('scraper_config_json', None)
        if scraper_config_json:
            if scraper_config_found:
                raise ConfigurationError('More than one scraper configuration file given!')
            scraper_config_found = True
            logger.info('Loading scraper configuration from: %s' % scraper_config_json)
            scraper_config_dict = load_json(scraper_config_json)

        scraper_config_yaml = kwargs.get('scraper_config_yaml', None)
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

    def get_api_key(self):
        return self.data['api_key']

    def get_hdx_site(self):
        return self.data['hdx_site']

    @staticmethod
    def load_api_key(path: str):
        """
        Load configuration parameters.

        """
        apikey = None
        with open(path, 'rt') as f:
            apikey = f.read().replace('\n', '')
        if not apikey:
            raise (ValueError('HDX api key is empty!'))
        return apikey
