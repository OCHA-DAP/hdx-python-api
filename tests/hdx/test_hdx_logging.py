# -*- coding: UTF-8 -*-
"""Logging Tests"""
from os.path import join

import pytest
from logging_tree import format

from hdx.hdx_logging import setup_logging, LoggingError

try:
    FILENOTFOUND_EXCTYPE = FileNotFoundError
except:
    FILENOTFOUND_EXCTYPE = IOError

class TestLogging:
    @pytest.fixture(scope='class')
    def logging_config_yaml(self):
        return join('tests', 'fixtures', 'config', 'logging_config.yml')

    @pytest.fixture(scope='class')
    def logging_config_json(self):
        return join('tests', 'fixtures', 'config', 'logging_config.json')

    @pytest.fixture(scope='class')
    def smtp_config_yaml(self):
        return join('tests', 'fixtures', 'config', 'smtp_config.yml')

    @pytest.fixture(scope='class')
    def smtp_config_json(self):
        return join('tests', 'fixtures', 'config', 'smtp_config.json')

    def test_setup_logging(self, logging_config_json, logging_config_yaml, smtp_config_json, smtp_config_yaml):
        with pytest.raises(FILENOTFOUND_EXCTYPE):
            setup_logging(smtp_config_json='NOT_EXIST')

        with pytest.raises(FILENOTFOUND_EXCTYPE):
            setup_logging(logging_config_json='NOT_EXIST', smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(FILENOTFOUND_EXCTYPE):
            setup_logging(logging_config_yaml='NOT_EXIST', smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(logging_config_yaml=logging_config_yaml, smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(logging_config_dict={'la': 'la'}, logging_config_json=logging_config_json,
                          smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(logging_config_dict={'la': 'la'}, logging_config_yaml=logging_config_yaml,
                          smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(logging_config_json=logging_config_json, logging_config_yaml=logging_config_yaml,
                          smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(smtp_config_dict={'la': 'la'}, smtp_config_json=smtp_config_json)

        with pytest.raises(LoggingError):
            setup_logging(smtp_config_dict={'la': 'la'}, smtp_config_yaml=smtp_config_yaml)

        with pytest.raises(LoggingError):
            setup_logging(smtp_config_json=smtp_config_json, smtp_config_yaml=smtp_config_yaml)

    def test_setup_logging_dict(self, smtp_config_yaml):
        logging_config_dict = {'version': 1,
                               'handlers': {
                                   'error_file_handler': {
                                       'class': 'logging.FileHandler',
                                       'level': 'ERROR',
                                       'filename': 'errors.log',
                                       'encoding': 'utf8',
                                       'mode': 'w'
                                   },
                                   'error_mail_handler': {
                                       'class': 'logging.handlers.SMTPHandler',
                                       'level': 'CRITICAL',
                                       'mailhost': 'localhost',
                                       'fromaddr': 'noreply@localhost',
                                       'toaddrs': 'abc@abc.com',
                                       'subject': 'SCRAPER FAILED'
                                   }
                               },
                               'root': {
                                   'level': 'INFO',
                                   'handlers': ['error_file_handler', 'error_mail_handler']
                               }}
        setup_logging(logging_config_dict=logging_config_dict)
        actual_logging_tree = format.build_description()
        for handler in ['File', 'SMTP']:
            assert handler in actual_logging_tree
        assert 'abc@abc.com' in actual_logging_tree

    def test_setup_logging_json(self, logging_config_json):
        setup_logging(logging_config_json=logging_config_json)
        actual_logging_tree = format.build_description()
        for handler in ['Stream', 'File']:
            assert handler in actual_logging_tree

    def test_setup_logging_yaml(self, logging_config_yaml):
        setup_logging(logging_config_yaml=logging_config_yaml)
        actual_logging_tree = format.build_description()
        for handler in ['Stream', 'SMTP']:
            assert handler in actual_logging_tree
        assert 'abc@abc.com' in actual_logging_tree

    def test_setup_logging_smtp_dict(self):
        smtp_config_dict = {'handlers': {'error_mail_handler': {'toaddrs': 'lalala@la.com', 'subject': 'lala'}}}
        setup_logging(smtp_config_dict=smtp_config_dict)
        actual_logging_tree = format.build_description()
        for handler in ['Stream', 'File', 'SMTP']:
            assert handler in actual_logging_tree
        assert 'lalala@la.com' in actual_logging_tree

    def test_setup_logging_smtp_json(self, smtp_config_json):
        setup_logging(smtp_config_json=smtp_config_json)
        actual_logging_tree = format.build_description()
        for handler in ['Stream', 'File', 'SMTP']:
            assert handler in actual_logging_tree
        assert '123@123.com' in actual_logging_tree

    def test_setup_logging_smtp_yaml(self, smtp_config_yaml):
        setup_logging(smtp_config_yaml=smtp_config_yaml)
        actual_logging_tree = format.build_description()
        for handler in ['Stream', 'File', 'SMTP']:
            assert handler in actual_logging_tree
        assert 'abc@abc.com' in actual_logging_tree
