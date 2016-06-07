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
        **kwargs: See below
        hdx_key_file (Optional[str]): Path to HDX key file. Defaults to ~/.hdxkey
        hdx_config_dict (dict): HDX configuration dictionary OR
        hdx_config_json (str): Path to JSON HDX configuration OR
        hdx_config_yaml (str): Path to YAML HDX configuration. Defaults to library's internal hdx_configuration.yml.
        collector_config_dict (dict): Collector configuration dictionary OR
        collector_config_json (str): Path to JSON Collector configuration OR
        collector_config_yaml (str): Path to YAML Collector configuration. Defaults to config/collector_configuration.yml.
    """

    def __init__(self, **kwargs):
        super(Configuration, self).__init__()

        hdx_key_file = kwargs.get('hdx_key_file', join('%s' % expanduser("~"), '.hdxkey'))
        self.data['api_key'] = self.load_api_key(hdx_key_file)

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

        collector_config_found = False
        collector_config_dict = kwargs.get('collector_config_dict', None)
        if collector_config_dict is not None:
            collector_config_found = True
            logger.info('Loading collector configuration from dictionary')

        collector_config_json = kwargs.get('collector_config_json', '')
        if collector_config_json:
            if collector_config_found:
                raise ConfigurationError('More than one collector configuration file given!')
            collector_config_found = True
            logger.info('Loading collector configuration from: %s' % collector_config_json)
            collector_config_dict = load_json(collector_config_json)

        collector_config_yaml = kwargs.get('collector_config_yaml', '')
        if collector_config_found:
            if collector_config_yaml:
                raise ConfigurationError('More than one collector configuration file given!')
        else:
            if not collector_config_yaml:
                logger.info('No collector configuration parameter. Using default.')
                collector_config_yaml = join('config', 'collector_configuration.yml')
            logger.info('Loading collector configuration from: %s' % collector_config_yaml)
            collector_config_dict = load_yaml(collector_config_yaml)

        self.data = merge_two_dictionaries(hdx_config_dict, collector_config_dict)

        if 'hdx_site' not in self.data:
            raise ConfigurationError('hdx_site not defined in configuration!')

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
        logger.info('Loaded HDX api key from: %s' % path)
        return apikey
