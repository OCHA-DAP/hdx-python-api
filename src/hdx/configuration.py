# -*- coding: utf-8 -*-
"""Configuration for HDX"""
import six

if six.PY2:
    from UserDict import IterableUserDict as UserDict
else:
    from collections import UserDict


import logging
from base64 import b64decode
from os.path import expanduser, join
from typing import Optional, Any

import ckanapi

import hdx
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import load_yaml, load_json
from hdx.utilities.path import script_dir_plus_file

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class Configuration(UserDict, object):
    """Configuration for HDX

    Args:
        **kwargs: See below
        hdx_site (Optional[str]): HDX site to use eg. prod, test. Defaults to test.
        hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
        hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
        hdx_key_file (Optional[str]): Path to HDX key file. Ignored if hdx_read_only = True or hdx_key supplied. Defaults to ~/.hdxkey.
        hdx_config_dict (dict): HDX configuration dictionary OR
        hdx_config_json (str): Path to JSON HDX configuration OR
        hdx_config_yaml (str): Path to YAML HDX configuration. Defaults to library's internal hdx_configuration.yml.
        project_config_dict (dict): Project configuration dictionary OR
        project_config_json (str): Path to JSON Project configuration OR
        project_config_yaml (str): Path to YAML Project configuration
    """

    def __init__(self, **kwargs):
        # type: (Any) -> None
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

        project_config_found = False
        project_config_dict = kwargs.get('project_config_dict', None)
        if project_config_dict is not None:
            project_config_found = True
            logger.info('Loading project configuration from dictionary')

        project_config_json = kwargs.get('project_config_json', '')
        if project_config_json:
            if project_config_found:
                raise ConfigurationError('More than one project configuration file given!')
            project_config_found = True
            logger.info('Loading project configuration from: %s' % project_config_json)
            project_config_dict = load_json(project_config_json)

        project_config_yaml = kwargs.get('project_config_yaml', '')
        if project_config_found:
            if project_config_yaml:
                raise ConfigurationError('More than one project configuration file given!')
        else:
            if project_config_yaml:
                logger.info('Loading project configuration from: %s' % project_config_yaml)
                project_config_dict = load_yaml(project_config_yaml)
            else:
                project_config_dict = dict()

        self.data = merge_two_dictionaries(hdx_config_dict, project_config_dict)

        self.hdx_read_only = kwargs.get('hdx_read_only', False)
        if not self.hdx_read_only:
            if 'hdx_key' in kwargs:
                self.data['api_key'] = kwargs.get('hdx_key')
            else:
                hdx_key_file = kwargs.get('hdx_key_file', join(expanduser('~'), '.hdxkey'))
                """ :type : str"""
                self.data['api_key'] = self.load_api_key(hdx_key_file)

        self.hdx_site = 'hdx_%s_site' % kwargs.get('hdx_site', 'test')
        if self.hdx_site not in self.data:
            raise ConfigurationError('%s not defined in configuration!' % self.hdx_site)

    def get_api_key(self):
        # type: () -> Optional[str]
        """

        Returns:
            Optional[str]: HDX api key or None if read only

        """
        if self.hdx_read_only:
            return None
        return self.data['api_key']

    def get_hdx_site_url(self):
        # type: () -> str
        """

        Returns:
            str: HDX web site url

        """
        return self.data[self.hdx_site]['url']

    def _get_credentials(self):
        # type: () -> tuple
        """

        Returns:
            tuple: HDX site username and password

        """
        site = self.data[self.hdx_site]
        username = site['username']
        if username:
            return b64decode(username).decode('utf-8'), b64decode(site['password']).decode('utf-8')
        else:
            return '', ''

    @staticmethod
    def load_api_key(path):
        # type: (str) -> str
        """
        Load HDX api key

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

    @staticmethod
    def read():
        # type: () -> 'Configuration'
        """
        Read the HDX configuration

        Returns:
            Configuration: The HDX configuration

        """
        if hdx.configuration is None:
            raise ConfigurationError('There is no HDX configuration! Use Configuration.create(**kwargs)')
        return hdx.configuration

    @staticmethod
    def remoteckan():
        # type: () -> ckanapi.RemoteCKAN
        """
        Return the remote CKAN object (see ckanapi library)

        Returns:
            ckanapi.RemoteCKAN: The remote CKAN object

        """
        if hdx.remoteckan is None:
            if hdx.configuration is None:
                raise ConfigurationError('There is no HDX configuration! Use Configuration.create(**kwargs)')
            raise ConfigurationError('There is no remote CKAN set up! Use Configuration.create(**kwargs)')
        return hdx.remoteckan

    @staticmethod
    def create(**kwargs):
        # type: (...) -> 'Configuration'
        """
        Create HDX configuration

        Args:
            **kwargs: See below
            hdx_site (Optional[str]): HDX site to use eg. prod, test. Defaults to test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
            hdx_key_file (Optional[str]): Path to HDX key file. Ignored if hdx_read_only = True or hdx_key supplied. Defaults to ~/.hdxkey.
            hdx_config_dict (dict): HDX configuration dictionary OR
            hdx_config_json (str): Path to JSON HDX configuration OR
            hdx_config_yaml (str): Path to YAML HDX configuration. Defaults to library's internal hdx_configuration.yml.
            project_config_dict (dict): Project configuration dictionary OR
            project_config_json (str): Path to JSON Project configuration OR
            project_config_yaml (str): Path to YAML Project configuration

        Returns:
            str: HDX site url

        """
        hdx.configuration = Configuration(**kwargs)
        version_file = open(script_dir_plus_file('version.txt', Configuration))
        version = version_file.read().strip()
        hdx.remoteckan = ckanapi.RemoteCKAN(hdx.configuration.get_hdx_site_url(),
                                            apikey=hdx.configuration.get_api_key(),
                                            user_agent='HDXPythonLibrary/%s' % version)
        hdx.validlocations = hdx.get_validlocations()
        return hdx.configuration.get_hdx_site_url()
