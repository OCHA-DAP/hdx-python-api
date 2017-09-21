# -*- coding: utf-8 -*-
"""Session utilities for urls"""
import logging

import requests
from basicauth import decode
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry

from hdx.utilities.loader import load_file_to_str, load_json, load_yaml

logger = logging.getLogger(__name__)


class SessionError(Exception):
    pass


def get_session(**kwargs):
    # type: (...) -> requests.Session
    """Set up and return Session object that is set up with retrying

    Args:
        **kwargs: See below
        auth (Tuple[str, str]): Authorisation information in tuple form (user, pass) OR
        basic_auth (str): Authorisation information in basic auth string form (Basic xxxxxxxxxxxxxxxx) OR
        basic_auth_file (str): Path to file containing authorisation information in basic auth string form (Basic xxxxxxxxxxxxxxxx)
        extra_params_dict (Dict[str]): Extra parameters to put on end of url as a dictionary OR
        extra_params_json (str): Path to JSON file containing extra parameters to put on end of url OR
        extra_params_yaml (str): Path to YAML file containing extra parameters to put on end of url
        status_forcelist (List[int]): HTTP statuses for which to force retry
    """
    s = requests.Session()
    auth_found = False
    auth = kwargs.get('auth')
    if auth:
        auth_found = True
        logger.info('Loading authorisation from auth argument')
    basic_auth = kwargs.get('basic_auth')
    if basic_auth:
        if auth_found:
            raise SessionError('More than one authorisation given!')
        auth_found = True
        logger.info('Loading authorisation from basic_auth argument')
        auth = decode(basic_auth)
    basic_auth_file = kwargs.get('basic_auth_file')
    if basic_auth_file:
        if auth_found:
            raise SessionError('More than one authorisation given!')
        logger.info('Loading authorisation from: %s' % basic_auth_file)
        basic_auth = load_file_to_str(basic_auth_file)
        auth = decode(basic_auth)
    s.auth = auth

    extra_params_found = False
    extra_params_dict = kwargs.get('extra_params_dict', None)
    if extra_params_dict:
        extra_params_found = True
        logger.info('Loading extra parameters from dictionary')

    extra_params_json = kwargs.get('extra_params_json', '')
    if extra_params_json:
        if extra_params_found:
            raise SessionError('More than one set of extra parameters given!')
        extra_params_found = True
        logger.info('Loading extra parameters from: %s' % extra_params_json)
        extra_params_dict = load_json(extra_params_json)

    extra_params_yaml = kwargs.get('extra_params_yaml', '')
    if extra_params_found:
        if extra_params_yaml:
            raise SessionError('More than one set of extra parameters given!')
    else:
        if extra_params_yaml:
            logger.info('Loading extra parameters from: %s' % extra_params_yaml)
            extra_params_dict = load_yaml(extra_params_yaml)
        else:
            extra_params_dict = dict()
    s.params = extra_params_dict

    status_forcelist = kwargs.get('status_forcelist', [429, 500, 502, 503, 504])

    retries = Retry(total=5, backoff_factor=0.4, status_forcelist=status_forcelist, raise_on_redirect=True,
                    raise_on_status=True)
    s.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
    s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
    return s
