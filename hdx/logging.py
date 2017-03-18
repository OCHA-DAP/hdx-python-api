# -*- coding: UTF-8 -*-
"""Configuration of logging"""
import logging.config

from hdx.utilities.dictandlist import merge_dictionaries
from hdx.utilities.loader import load_yaml, load_json
from hdx.utilities.path import script_dir_plus_file


class LoggingError(Exception):
    pass


def setup_logging(**kwargs):
    # type: (...) -> None
    """Setup logging configuration

    Args:
        **kwargs: See below
        logging_config_dict (dict): Logging configuration dictionary OR
        logging_config_json (str): Path to JSON Logging configuration OR
        logging_config_yaml (str): Path to YAML Logging configuration. Defaults to internal logging_configuration.yml.
        smtp_config_dict (dict): Email Logging configuration dictionary if using default logging configuration OR
        smtp_config_json (str): Path to JSON Email Logging configuration if using default logging configuration OR
        smtp_config_yaml (str): Path to YAML Email Logging configuration if using default logging configuration

    Returns:
        None
    """
    smtp_config_found = False
    smtp_config_dict = kwargs.get('smtp_config_dict', None)
    if smtp_config_dict:
        smtp_config_found = True
        print('Loading smtp configuration customisations from dictionary')

    smtp_config_json = kwargs.get('smtp_config_json', '')
    if smtp_config_json:
        if smtp_config_found:
            raise LoggingError('More than one smtp configuration file given!')
        smtp_config_found = True
        print('Loading smtp configuration customisations from: %s' % smtp_config_json)
        smtp_config_dict = load_json(smtp_config_json)

    smtp_config_yaml = kwargs.get('smtp_config_yaml', '')
    if smtp_config_yaml:
        if smtp_config_found:
            raise LoggingError('More than one smtp configuration file given!')
        smtp_config_found = True
        print('Loading smtp configuration customisations from: %s' % smtp_config_yaml)
        smtp_config_dict = load_yaml(smtp_config_yaml)

    logging_smtp_config_dict = None
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
            if smtp_config_found:
                logging_smtp_config_yaml = script_dir_plus_file('logging_smtp_configuration.yml', setup_logging)
                print('Loading base SMTP logging configuration from: %s' % logging_smtp_config_yaml)
                logging_smtp_config_dict = load_yaml(logging_smtp_config_yaml)
        print('Loading logging configuration from: %s' % logging_config_yaml)
        logging_config_dict = load_yaml(logging_config_yaml)

    if smtp_config_found:
        if logging_smtp_config_dict:
            logging_config_dict = merge_dictionaries([logging_config_dict, logging_smtp_config_dict, smtp_config_dict])
        else:
            raise LoggingError('SMTP logging configuration file given but not using default logging configuration!')
    logging.config.dictConfig(logging_config_dict)
