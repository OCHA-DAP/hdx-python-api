# -*- coding: utf-8 -*-
"""Configuration for HDX"""
import os

import requests
import six
from hdx.utilities.email import Email
from hdx.utilities.session import get_session
from hdx.utilities.useragent import UserAgent, UserAgentError

from hdx.version import get_api_version

if six.PY2:
    from UserDict import IterableUserDict as UserDict
else:
    from collections import UserDict


import logging
from base64 import b64decode
from os.path import expanduser, join, isfile
from typing import Optional, Any, Dict, Tuple

import ckanapi

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
        user_agent (str): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
        user_agent_config_yaml (str): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
        user_agent_lookup (str): Lookup key for YAML. Ignored if user_agent supplied.
        hdx_url (str): HDX url to use. Overrides hdx_site.
        hdx_site (str): HDX site to use eg. prod, test.
        hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
        hdx_key (str): Your HDX key. Ignored if hdx_read_only = True.
        hdx_config_dict (dict): HDX configuration dictionary to use instead of above 3 parameters OR
        hdx_config_json (str): Path to JSON HDX configuration OR
        hdx_config_yaml (str): Path to YAML HDX configuration
        project_config_dict (dict): Project configuration dictionary OR
        project_config_json (str): Path to JSON Project configuration OR
        project_config_yaml (str): Path to YAML Project configuration
        hdx_base_config_dict (dict): HDX base configuration dictionary OR
        hdx_base_config_json (str): Path to JSON HDX base configuration OR
        hdx_base_config_yaml (str): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.
    """

    _configuration = None
    home_folder = expanduser('~')
    default_hdx_base_config_yaml = script_dir_plus_file('hdx_base_configuration.yml', ConfigurationError)
    default_hdx_config_yaml = join(home_folder, '.hdx_configuration.yml')

    apiversion = get_api_version()
    prefix = 'HDXPythonLibrary/%s' % apiversion

    def __init__(self, **kwargs):
        # type: (Any) -> None
        super(Configuration, self).__init__()

        self._remoteckan = None
        self._emailer = None

        hdx_base_config_found = False
        hdx_base_config_dict = kwargs.get('hdx_base_config_dict', None)
        if hdx_base_config_dict:
            hdx_base_config_found = True
            logger.info('Loading HDX base configuration from dictionary')

        hdx_base_config_json = kwargs.get('hdx_base_config_json', '')
        if hdx_base_config_json:
            if hdx_base_config_found:
                raise ConfigurationError('More than one HDX base configuration given!')
            hdx_base_config_found = True
            logger.info('Loading HDX base configuration from: %s' % hdx_base_config_json)
            hdx_base_config_dict = load_json(hdx_base_config_json)

        hdx_base_config_yaml = kwargs.get('hdx_base_config_yaml', '')
        if hdx_base_config_found:
            if hdx_base_config_yaml:
                raise ConfigurationError('More than one HDX base configuration given!')
        else:
            if not hdx_base_config_yaml:
                hdx_base_config_yaml = Configuration.default_hdx_base_config_yaml
                logger.info('No HDX base configuration parameter. Using default base configuration file: %s.' % hdx_base_config_yaml)
            logger.info('Loading HDX base configuration from: %s' % hdx_base_config_yaml)
            hdx_base_config_dict = load_yaml(hdx_base_config_yaml)

        hdx_config_found = False
        hdx_config_dict = kwargs.get('hdx_config_dict', None)
        if hdx_config_dict:
            hdx_config_found = True
            logger.info('Loading HDX configuration from dictionary')

        hdx_config_json = kwargs.get('hdx_config_json', '')
        if hdx_config_json:
            if hdx_config_found:
                raise ConfigurationError('More than one HDX configuration given!')
            hdx_config_found = True
            logger.info('Loading HDX configuration from: %s' % hdx_config_json)
            hdx_config_dict = load_json(hdx_config_json)

        hdx_config_yaml = kwargs.get('hdx_config_yaml', '')
        if hdx_config_found:
            if hdx_config_yaml:
                raise ConfigurationError('More than one HDX configuration given!')
        else:
            if not hdx_config_yaml:
                hdx_config_yaml = Configuration.default_hdx_config_yaml
                if isfile(hdx_config_yaml):
                    logger.info('No HDX configuration parameter. Using default configuration file: %s.' % hdx_config_yaml)
                else:
                    logger.info('No HDX configuration parameter and no configuration file at default path: %s.' % hdx_config_yaml)
                    hdx_config_yaml = None
                    hdx_config_dict = dict()
            if hdx_config_yaml:
                logger.info('Loading HDX configuration from: %s' % hdx_config_yaml)
                hdx_config_dict = load_yaml(hdx_config_yaml)

        self.data = merge_two_dictionaries(hdx_base_config_dict, hdx_config_dict)

        project_config_found = False
        project_config_dict = kwargs.get('project_config_dict', None)
        if project_config_dict is not None:
            project_config_found = True
            logger.info('Loading project configuration from dictionary')

        project_config_json = kwargs.get('project_config_json', '')
        if project_config_json:
            if project_config_found:
                raise ConfigurationError('More than one project configuration given!')
            project_config_found = True
            logger.info('Loading project configuration from: %s' % project_config_json)
            project_config_dict = load_json(project_config_json)

        project_config_yaml = kwargs.get('project_config_yaml', '')
        if project_config_found:
            if project_config_yaml:
                raise ConfigurationError('More than one project configuration given!')
        else:
            if project_config_yaml:
                logger.info('Loading project configuration from: %s' % project_config_yaml)
                project_config_dict = load_yaml(project_config_yaml)
            else:
                project_config_dict = dict()

        self.data = merge_two_dictionaries(hdx_base_config_dict, project_config_dict)

        ua = kwargs.get('full_agent')
        if ua:
            self.user_agent = ua
        else:
            try:
                self.user_agent = UserAgent.get(prefix=Configuration.prefix, **kwargs)
            except UserAgentError:
                self.user_agent = UserAgent.get(prefix=Configuration.prefix, **self.data)
        self.hdx_read_only = kwargs.get('hdx_read_only', self.data.get('hdx_read_only', False))
        logger.info('Read only access to HDX: %s' % str(self.hdx_read_only))
        self.hdx_key = kwargs.get('hdx_key', self.data.get('hdx_key'))
        if not self.hdx_key and not self.hdx_read_only:
            raise ConfigurationError('No HDX API key supplied as a parameter or in configuration!')
        hdx_url = kwargs.get('hdx_url', self.data.get('hdx_url'))
        if hdx_url:
            self.hdx_site = 'hdx_custom_site'
            hdx_url = hdx_url.rstrip('/')
            self.data[self.hdx_site] = {'url': hdx_url}
        else:
            self.hdx_site = 'hdx_%s_site' % kwargs.get('hdx_site', self.data.get('hdx_site', 'stage'))
            if self.hdx_site not in self.data:
                raise ConfigurationError('%s not defined in configuration!' % self.hdx_site)

    def set_read_only(self, read_only=True):
        # type: (bool) -> None
        """
        Set HDX read only flag

        Args:
            read_only (bool): Value to set HDX read only flag. Defaults to True.
        Returns:
            None

        """
        self.hdx_read_only = read_only

    def set_api_key(self, apikey):
        # type: (str) -> None
        """
        Set HDX api key

        Args:
            apikey (str): Value to set api key.
        Returns:
            None

        """
        self.hdx_key = apikey

    def get_api_key(self):
        # type: () -> Optional[str]
        """
        Return HDX api key or None if read only

        Returns:
            Optional[str]: HDX api key or None if read only

        """
        if self.hdx_read_only:
            return None
        return self.hdx_key

    def get_user_agent(self):
        # type: () -> str
        """
        Return user agent

        Returns:
            str: User agent

        """
        return self.user_agent

    def get_hdx_site_url(self):
        # type: () -> str
        """
        Return HDX web site url

        Returns:
            str: HDX web site url

        """
        return self.data[self.hdx_site]['url']

    def get_dataset_url(self, name):
        # type: (str) -> str
        """
        Return HDX dataset url given dataset name

        Args:
            name: Dataset name

        Returns:
            str: HDX dataset url

        """
        return '%s/dataset/%s' % (self.get_hdx_site_url(), name)

    def _get_credentials(self):
        # type: () -> Optional[Tuple[str, str]]
        """
        Return HDX site username and password

        Returns:
            Optional[Tuple[str, str]]: HDX site username and password or None

        """
        site = self.data[self.hdx_site]
        username = site.get('username')
        if username:
            return b64decode(username).decode('utf-8'), b64decode(site['password']).decode('utf-8')
        else:
            return None

    def remoteckan(self):
        # type: () -> ckanapi.RemoteCKAN
        """
        Return the remote CKAN object (see ckanapi library)

        Returns:
            ckanapi.RemoteCKAN: The remote CKAN object

        """
        if self._remoteckan is None:
            raise ConfigurationError('There is no remote CKAN set up! Use Configuration.create(**kwargs)')
        return self._remoteckan

    def call_remoteckan(self, *args, **kwargs):
        # type: (Any, Any) -> Dict
        """
        Calls the remote CKAN

        Args:
            *args: Arguments to pass to remote CKAN call_action method
            **kwargs: Keyword arguments to pass to remote CKAN call_action method

        Returns:
            Dict: The response from the remote CKAN call_action method

        """
        requests_kwargs = kwargs.get('requests_kwargs', dict())
        credentials = self._get_credentials()
        if credentials:
            requests_kwargs['auth'] = credentials
        kwargs['requests_kwargs'] = requests_kwargs
        apikey = kwargs.get('apikey', self.get_api_key())
        kwargs['apikey'] = apikey
        return self.remoteckan().call_action(*args, **kwargs)

    @staticmethod
    def _environment_variables(**kwargs):
        # type: (Any) -> Any
        """
        Overwrite keyword arguments with environment variables

        Args:
            **kwargs: See below
            hdx_url (str): HDX url to use. Overrides hdx_site.
            hdx_site (str): HDX site to use eg. prod, test. Defaults to test.
            hdx_key (str): Your HDX key. Ignored if hdx_read_only = True.

        Returns:
            kwargs: Changed keyword arguments

        """

        hdx_key = os.getenv('HDX_KEY')
        if hdx_key is not None:
            kwargs['hdx_key'] = hdx_key
        hdx_url = os.getenv('HDX_URL')
        if hdx_url is not None:
            kwargs['hdx_url'] = hdx_url
        else:
            hdx_site = os.getenv('HDX_SITE')
            if hdx_site is not None:
                kwargs['hdx_site'] = hdx_site
        return kwargs

    @classmethod
    def create_remoteckan(cls, site_url, user_agent=None, user_agent_config_yaml=None, user_agent_lookup=None,
                          use_env=False, session=None, **kwargs):
        # type: (str, Optional[str], Optional[str], Optional[str], bool, requests.Session, Any) -> ckanapi.RemoteCKAN
        """
        Create remote CKAN instance from configuration

        Args:
            site_url (str): Site url.
            user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed.
            user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
            use_env (bool): Whether to read environment variables. Defaults to False.
            session (requests.Session): requests Session object to use. Defaults to calling hdx.utilities.session.get_session()

        Returns:
            ckanapi.RemoteCKAN: Remote CKAN instance

        """
        if not session:
            whitelist = frozenset(['HEAD', 'TRACE', 'GET', 'POST', 'PUT', 'OPTIONS', 'DELETE'])
            session = get_session(user_agent, user_agent_config_yaml, user_agent_lookup, use_env,
                                  prefix=Configuration.prefix, method_whitelist=whitelist, **kwargs)
            ua = session.headers['User-Agent']
        else:
            ua = kwargs.get('full_agent')
            if not ua:
                ua = UserAgent.get(user_agent, user_agent_config_yaml, user_agent_lookup, prefix=Configuration.prefix,
                                   **kwargs)
        return ckanapi.RemoteCKAN(site_url, user_agent=ua, session=session)

    def setup_remoteckan(self, remoteckan=None, **kwargs):
        # type: (Optional[ckanapi.RemoteCKAN], Any) -> None
        """
        Set up remote CKAN from provided CKAN or by creating from configuration

        Args:
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.

        Returns:
            None

        """
        if remoteckan is None:
            self._remoteckan = self.create_remoteckan(self.get_hdx_site_url(), full_agent=self.get_user_agent(),
                                                      **kwargs)
        else:
            self._remoteckan = remoteckan

    def emailer(self):
        # type: () -> Email
        """
        Return the Email object (see :any:`Email`)

        Returns:
            Email: The email object

        """
        if self._emailer is None:
            raise ConfigurationError('There is no emailer set up! Use setup_emailer(Any)')
        return self._emailer

    def setup_emailer(self, **kwargs):
        # type: (Any) -> None
        """
        Set up emailer. Parameters in dictionary or file (eg. yaml below):
        | connection_type: "ssl"   ("ssl" for smtp ssl or "lmtp", otherwise basic smtp is assumed)
        | host: "localhost"
        | port: 123
        | local_hostname: "mycomputer.fqdn.com"
        | timeout: 3
        | username: "user"
        | password: "pass"

        Args:
            **kwargs: See below
            email_config_dict (dict): HDX configuration dictionary OR
            email_config_json (str): Path to JSON HDX configuration OR
            email_config_yaml (str): Path to YAML HDX configuration. Defaults to ~/hdx_email_configuration.yml.

        Returns:
            None
        """
        self._emailer = Email(**kwargs)

    @classmethod
    def read(cls):
        # type: () -> 'Configuration'
        """
        Read the HDX configuration

        Returns:
            Configuration: The HDX configuration

        """
        if cls._configuration is None:
            raise ConfigurationError('There is no HDX configuration! Use Configuration.create(**kwargs)')
        return cls._configuration

    @classmethod
    def setup(cls, configuration=None, **kwargs):
        # type: (Optional['Configuration'], Any) -> None
        """
        Set up the HDX configuration

        Args:
            configuration (Optional[Configuration]): Configuration instance. Defaults to setting one up from passed arguments.
            **kwargs: See below
            user_agent (str): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (str): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (str): Lookup key for YAML. Ignored if user_agent supplied.
            hdx_url (str): HDX url to use. Overrides hdx_site.
            hdx_site (str): HDX site to use eg. prod, test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (str): Your HDX key. Ignored if hdx_read_only = True.
            hdx_config_dict (dict): HDX configuration dictionary to use instead of above 3 parameters OR
            hdx_config_json (str): Path to JSON HDX configuration OR
            hdx_config_yaml (str): Path to YAML HDX configuration
            project_config_dict (dict): Project configuration dictionary OR
            project_config_json (str): Path to JSON Project configuration OR
            project_config_yaml (str): Path to YAML Project configuration
            hdx_base_config_dict (dict): HDX base configuration dictionary OR
            hdx_base_config_json (str): Path to JSON HDX base configuration OR
            hdx_base_config_yaml (str): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.

        Returns:
            None

        """
        if configuration is None:
            cls._configuration = Configuration(**kwargs)
        else:
            cls._configuration = configuration

    @classmethod
    def _create(cls, configuration=None, remoteckan=None, **kwargs):
        # type: (Optional['Configuration'], Optional[ckanapi.RemoteCKAN], Any) -> str
        """
        Create HDX configuration

        Args:
            configuration (Optional[Configuration]): Configuration instance. Defaults to setting one up from passed arguments.
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.
            **kwargs: See below
            user_agent (str): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (str): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (str): Lookup key for YAML. Ignored if user_agent supplied.
            hdx_url (str): HDX url to use. Overrides hdx_site.
            hdx_site (str): HDX site to use eg. prod, test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (str): Your HDX key. Ignored if hdx_read_only = True.
            hdx_config_dict (dict): HDX configuration dictionary to use instead of above 3 parameters OR
            hdx_config_json (str): Path to JSON HDX configuration OR
            hdx_config_yaml (str): Path to YAML HDX configuration
            project_config_dict (dict): Project configuration dictionary OR
            project_config_json (str): Path to JSON Project configuration OR
            project_config_yaml (str): Path to YAML Project configuration
            hdx_base_config_dict (dict): HDX base configuration dictionary OR
            hdx_base_config_json (str): Path to JSON HDX base configuration OR
            hdx_base_config_yaml (str): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.

        Returns:
            str: HDX site url

        """
        kwargs = cls._environment_variables(**kwargs)
        cls.setup(configuration, **kwargs)
        cls._configuration.setup_remoteckan(remoteckan, **kwargs)
        return cls._configuration.get_hdx_site_url()

    @classmethod
    def create(cls, configuration=None, remoteckan=None, **kwargs):
        # type: (Optional['Configuration'], Optional[ckanapi.RemoteCKAN], Any) -> str
        """
        Create HDX configuration. Can only be called once (will raise an error if called more than once).

        Args:
            configuration (Optional[Configuration]): Configuration instance. Defaults to setting one up from passed arguments.
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.
            **kwargs: See below
            user_agent (str): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (str): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (str): Lookup key for YAML. Ignored if user_agent supplied.
            hdx_url (str): HDX url to use. Overrides hdx_site.
            hdx_site (str): HDX site to use eg. prod, test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (str): Your HDX key. Ignored if hdx_read_only = True.
            hdx_config_dict (dict): HDX configuration dictionary to use instead of above 3 parameters OR
            hdx_config_json (str): Path to JSON HDX configuration OR
            hdx_config_yaml (str): Path to YAML HDX configuration
            project_config_dict (dict): Project configuration dictionary OR
            project_config_json (str): Path to JSON Project configuration OR
            project_config_yaml (str): Path to YAML Project configuration
            hdx_base_config_dict (dict): HDX base configuration dictionary OR
            hdx_base_config_json (str): Path to JSON HDX base configuration OR
            hdx_base_config_yaml (str): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.

        Returns:
            str: HDX site url

        """
        if cls._configuration is not None:
            raise ConfigurationError('Configuration already created!')
        return cls._create(configuration=configuration, remoteckan=remoteckan, **kwargs)

    @classmethod
    def delete(cls):
        # type: () -> None
        """
        Delete global HDX configuration.

        Returns:
            None

        """
        cls._configuration = None
