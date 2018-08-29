# -*- coding: utf-8 -*-
"""Configuration for HDX"""
import requests
import six

from hdx.utilities.email import Email
from hdx.utilities.session import get_session

if six.PY2:
    from UserDict import IterableUserDict as UserDict
else:
    from collections import UserDict


import logging
from base64 import b64decode
from os.path import expanduser, join, isfile
from typing import Optional, Any, Dict

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
        hdx_site (Optional[str]): HDX site to use eg. prod, test. Defaults to test.
        hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
        hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
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
    default_user_agent_config_yaml = join(home_folder, '.useragent.yml')

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

        self.hdx_read_only = kwargs.get('hdx_read_only', self.data.get('hdx_read_only', False))
        logger.info('Read only access to HDX: %s' % str(self.hdx_read_only))
        self.hdx_key = kwargs.get('hdx_key', self.data.get('hdx_key'))
        if not self.hdx_key and not self.hdx_read_only:
            raise ConfigurationError('No HDX API key supplied as a parameter or in configuration!')
        self.hdx_site = 'hdx_%s_site' % kwargs.get('hdx_site', self.data.get('hdx_site', 'test'))
        if self.hdx_site not in self.data:
            raise ConfigurationError('%s not defined in configuration!' % self.hdx_site)

    @staticmethod
    def get_version():
        version_file = open(script_dir_plus_file('version.txt', Configuration))
        return version_file.read().strip()

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

    def get_hdx_site_url(self):
        # type: () -> str
        """
        Return HDX web site url

        Returns:
            str: HDX web site url

        """
        return self.data[self.hdx_site]['url']

    def _get_credentials(self):
        # type: () -> tuple
        """
        Return HDX site username and password

        Returns:
            tuple: HDX site username and password

        """
        site = self.data[self.hdx_site]
        username = site['username']
        if username:
            return b64decode(username).decode('utf-8'), b64decode(site['password']).decode('utf-8')
        else:
            return '', ''

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
        # type: (Any) -> Dict
        """
        Calls the remote CKAN

        Args:
            *args: Arguments to pass to remote CKAN call_action method
            **kwargs: Keyword arguments to pass to remote CKAN call_action method

        Returns:
            Dict: The response from the remote CKAN call_action method

        """
        requests_kwargs = kwargs.get('requests_kwargs', dict())
        requests_kwargs['auth'] = self._get_credentials()
        kwargs['requests_kwargs'] = requests_kwargs
        apikey = kwargs.get('apikey', self.get_api_key())
        kwargs['apikey'] = apikey
        return self.remoteckan().call_action(*args, **kwargs)

    @classmethod
    def create_remoteckan(cls, site_url, user_agent=None, user_agent_config_yaml=None, user_agent_lookup=None,
                          session=get_session(method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'POST', 'PUT',
                                                                          'OPTIONS', 'DELETE'])), **kwargs):
        # type: (str, Optional[str], Optional[str], Optional[str], requests.Session, Any) -> ckanapi.RemoteCKAN
        """
        Create remote CKAN instance from configuration

        Args:
            site_url (str): Site url.
            user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed.
            user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
            session (requests.Session): requests Session object to use. Defaults to calling hdx.utilities.session.get_session()

        Returns:
            ckanapi.RemoteCKAN: Remote CKAN instance

        """
        prefix = 'HDXPythonLibrary/%s' % Configuration.get_version()
        if not user_agent:
            ua = Configuration.load_user_agent(prefix, user_agent_config_yaml, user_agent_lookup)
        else:
            ua = cls._construct_user_agent(kwargs, prefix, user_agent)
        return ckanapi.RemoteCKAN(site_url, user_agent=ua, session=session)

    def setup_remoteckan(self, user_agent=None, user_agent_config_yaml=None, user_agent_lookup=None,
                         remoteckan=None, **kwargs):
        # type: (Optional[str], Optional[str], Optional[str], Optional[ckanapi.RemoteCKAN], Any) -> None
        """
        Set up remote CKAN from provided CKAN or by creating from configuration

        Args:
            user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.

        Returns:
            None

        """
        if remoteckan is None:
            self._remoteckan = self.create_remoteckan(self.get_hdx_site_url(), user_agent, user_agent_config_yaml,
                                                      user_agent_lookup, **kwargs)
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

    @staticmethod
    def _construct_user_agent(configdict, prefix, ua):
        # type: (Dict, str, str) -> str
        """
        Construct user agent

        Args:
            configdict (str): Additional configuration for user agent
            prefix (str): Text to put at start of user agent
            ua (str): Custom user agent text

        Returns:
            str: Full user agent string

        """
        if not ua:
            raise ConfigurationError("user_agent should be supplied in a YAML config file or directly as a parameter. It can be your project's name for example.")
        preprefix = configdict.get('preprefix')
        if preprefix:
            user_agent = '%s:' % preprefix
        else:
            user_agent = ''
        if prefix:
            user_agent = '%s%s-' % (user_agent, prefix)
        user_agent = '%s%s' % (user_agent, ua)
        return user_agent

    @classmethod
    def load_user_agent(cls, prefix, user_agent_config_yaml, user_agent_lookup=None):
        # type: (str, str, Optional[str]) -> str
        """
        Load user agent YAML file

        Args:
            prefix (str): Text to put at start of user agent
            user_agent_config_yaml (str): Path to user agent YAML file
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.

        Returns:
            str: user agent

        """
        if not user_agent_config_yaml:
            user_agent_config_yaml = Configuration.default_user_agent_config_yaml
            logger.info('No user agent or user agent config file given. Using default user agent config file: %s.' % user_agent_config_yaml)
        if not isfile(user_agent_config_yaml):
            raise ConfigurationError("User_agent should be supplied in a YAML config file or directly as a parameter. It can be your project's name for example.")
        logger.info('Loading user agent config from: %s' % user_agent_config_yaml)
        user_agent_config_dict = load_yaml(user_agent_config_yaml)
        if user_agent_lookup:
            user_agent_config_dict = user_agent_config_dict.get(user_agent_lookup)
        if not user_agent_config_dict:
            raise ConfigurationError("No user agent information read from: %s" % user_agent_config_yaml)
        ua = user_agent_config_dict.get('user_agent')
        return cls._construct_user_agent(user_agent_config_dict, prefix, ua)

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
            hdx_site (Optional[str]): HDX site to use eg. prod, test. Defaults to test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
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
    def _create(cls, configuration=None, user_agent=None, user_agent_config_yaml=None, user_agent_lookup=None,
                remoteckan=None, **kwargs):
        # type: (Optional['Configuration'], Optional[str], Optional[str], Optional[str], Optional[ckanapi.RemoteCKAN], Any) -> str
        """
        Create HDX configuration

        Args:
            configuration (Optional[Configuration]): Configuration instance. Defaults to setting one up from passed arguments.
            user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.
            **kwargs: See below
            hdx_site (Optional[str]): HDX site to use eg. prod, test. Defaults to test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
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
        cls.setup(configuration, **kwargs)
        cls._configuration.setup_remoteckan(user_agent, user_agent_config_yaml, user_agent_lookup, remoteckan, **kwargs)
        return cls._configuration.get_hdx_site_url()

    @classmethod
    def create(cls, configuration=None, user_agent=None, user_agent_config_yaml=None, user_agent_lookup=None,
               remoteckan=None, **kwargs):
        # type: (Optional['Configuration'], Optional[str], Optional[str], Optional[str], Optional[ckanapi.RemoteCKAN], Any) -> str
        """
        Create HDX configuration. Can only be called once (will raise an error if called more than once).

        Args:
            configuration (Optional[Configuration]): Configuration instance. Defaults to setting one up from passed arguments.
            user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
            user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
            user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
            remoteckan (Optional[ckanapi.RemoteCKAN]): CKAN instance. Defaults to setting one up from configuration.
            **kwargs: See below
            hdx_site (str): HDX site to use eg. prod, test.
            hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
            hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
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
        return cls._create(configuration=configuration, user_agent=user_agent,
                           user_agent_config_yaml=user_agent_config_yaml, user_agent_lookup=user_agent_lookup,
                           remoteckan=remoteckan, **kwargs)

    @classmethod
    def delete(cls):
        # type: () -> None
        """
        Delete global HDX configuration.

        Returns:
            None

        """
        cls._configuration = None
