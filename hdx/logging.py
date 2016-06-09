#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""Configuration of logging"""
import logging.config
import os

from hdx.utilities.loader import load_yaml, load_json, script_dir_plus_file
from .utilities.dictionary import merge_two_dictionaries


class LoggingError(Exception):
    pass


def setup_logging(**kwargs) -> None:
    """Setup logging configuration

    Args:
        **kwargs: See below
        logging_config_dict (dict): Logging configuration dictionary OR
        logging_config_json (str): Path to JSON Logging configuration OR
        logging_config_yaml (str): Path to YAML Logging configuration. Defaults to internal logging_configuration.yml.
        smtp_config_dict (dict): Email Logging configuration dictionary OR
        smtp_config_json (str): Path to JSON Email Logging configuration OR
        smtp_config_yaml (str): Path to YAML Email Logging configuration. Defaults to config/smtp_configuration.yml.

    Returns:
        None
    """
    logging_config_found = False
    logging_config_dict = kwargs.get('logging_config_dict', None)
    if logging_config_dict:
        logging_config_found = True
        print('Loading logging configuration from dictionary')

    logging_config_json = kwargs.get('logging_config_json', '')
    if logging_config_json:
        if logging_config_found:
            raise LoggingError('More than one logging configuration file given!')
        logging_config_found = True
        print('Loading logging configuration from: %s' % logging_config_json)
        logging_config_dict = load_json(logging_config_json)

    logging_config_yaml = kwargs.get('logging_config_yaml', '')
    if logging_config_found:
        if logging_config_yaml:
            raise LoggingError('More than one logging configuration file given!')
    else:
        if not logging_config_yaml:
            print('No logging configuration parameter. Using default.')
            logging_config_yaml = script_dir_plus_file('logging_configuration.yml', setup_logging)
        print('Loading logging configuration from: %s' % logging_config_yaml)
        logging_config_dict = load_yaml(logging_config_yaml)

    smtp_config_in_logging = False
    for _, handler in logging_config_dict['handlers'].items():
        if 'SMTP' in handler['class']:
            smtp_config_in_logging = True
            break
    if not smtp_config_in_logging:
        logging.config.dictConfig(logging_config_dict)
        return

    smtp_config_found = False
    smtp_config_dict = kwargs.get('smtp_config_dict', None)
    if smtp_config_dict:
        smtp_config_found = True
        print('Loading smtp configuration from dictionary')

    smtp_config_json = kwargs.get('smtp_config_json', '')
    if smtp_config_json:
        if smtp_config_found:
            raise LoggingError('More than one smtp configuration file given!')
        smtp_config_found = True
        print('Loading smtp configuration from: %s' % smtp_config_json)
        smtp_config_dict = load_json(smtp_config_json)

    smtp_config_yaml = kwargs.get('smtp_config_yaml', '')
    if smtp_config_found:
        if smtp_config_yaml:
            raise LoggingError('More than one smtp configuration file given!')
    else:
        if not smtp_config_yaml:
            print('No smtp configuration parameter. Using default.')
            smtp_config_yaml = os.path.join('config', 'smtp_configuration.yml')
        print('Loading smtp configuration from: %s' % smtp_config_yaml)
        smtp_config_dict = load_yaml(smtp_config_yaml)

    config = merge_two_dictionaries(logging_config_dict, smtp_config_dict)
    logging.config.dictConfig(config)
