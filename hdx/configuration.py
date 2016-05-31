#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
LOAD CONFIGURATION:
------------------

Script designed to load the configuration file
from disk.

"""
import collections
from os.path import expanduser

from hdx.utilities.loader import load_data, load_and_merge_data


class ConfigurationError(Exception):
    pass


class Configuration(collections.UserDict):
    def __init__(self, hdx_key_file: str = '%s/.hdxkey' % expanduser("~"), input_type: str = 'yaml',
                 hdx_config_file='config/hdx_configuration.yml', scraper_config_file=None):
        super(Configuration, self).__init__()
        if scraper_config_file:
            self.data = load_and_merge_data(input_type, [hdx_config_file, scraper_config_file])
        else:
            self.data = load_data(input_type, hdx_config_file)
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
