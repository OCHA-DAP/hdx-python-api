import os
import logging.config

from hdx.utilities.loader import load_yaml, load_and_merge_yaml, load_json, load_and_merge_json, script_dir_plus_file
from .utilities.dictionary import merge_two_dictionaries


class loggingError(Exception):
    pass


def setup_logging(**kwargs):
    """Setup logging configuration

    """
    logging_config_found = False
    logging_config_dict = kwargs.get('logging_config_dict', None)
    if logging_config_dict:
        logging_config_found = True
        print('Loading logging configuration from dictionary')

    logging_config_json = kwargs.get('logging_config_json', None)
    if logging_config_json:
        if logging_config_found:
            raise loggingError('More than one logging configuration file given!')
        logging_config_found = True
        print('Loading logging configuration from: %s' % logging_config_json)
        logging_config_dict = load_json(logging_config_json)

    logging_config_yaml = kwargs.get('logging_config_yaml', None)
    if logging_config_found:
        if logging_config_yaml:
            raise loggingError('More than one logging configuration file given!')
    else:
        if not logging_config_yaml:
            print('No logging configuration parameter. Using default.')
            logging_config_yaml = script_dir_plus_file('logging_configuration.yml', setup_logging)
        print('Loading logging configuration from: %s' % logging_config_yaml)
        logging_config_dict = load_yaml(logging_config_yaml)

    smtp_config_found = False
    smtp_config_dict = kwargs.get('smtp_config_dict', None)
    if smtp_config_dict:
        smtp_config_found = True
        print('Loading smtp configuration from dictionary')

    smtp_config_json = kwargs.get('smtp_config_json', None)
    if smtp_config_json:
        if smtp_config_found:
            raise loggingError('More than one smtp configuration file given!')
        smtp_config_found = True
        print('Loading smtp configuration from: %s' % smtp_config_json)
        smtp_config_dict = load_json(smtp_config_json)

    smtp_config_yaml = kwargs.get('smtp_config_yaml', None)
    if smtp_config_found:
        if smtp_config_yaml:
            raise loggingError('More than one smtp configuration file given!')
    else:
        if not smtp_config_yaml:
            print('No smtp configuration parameter. Using default.')
            smtp_config_yaml = os.path.join('config', 'smtp_configuration.yml')
        print('Loading smtp configuration from: %s' % smtp_config_yaml)
        smtp_config_dict = load_yaml(smtp_config_yaml)

    config = merge_two_dictionaries(logging_config_dict, smtp_config_dict)
    logging.config.dictConfig(config)
