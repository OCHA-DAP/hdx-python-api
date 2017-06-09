# -*- coding: UTF-8 -*-
"""Email Tests"""
import smtplib
from os.path import join

import pytest

from hdx.configuration import ConfigurationError
from hdx.utilities.email import Email


class TestEmail:
    @pytest.fixture(scope='class')
    def email_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_email_configuration.json')

    @pytest.fixture(scope='class')
    def email_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_email_configuration.yml')

    @pytest.fixture(scope='function')
    def mocksmtp(self, monkeypatch):
        class MockSMTPBase(object):
            type = None

            def __init__(self, **kwargs):
                self.initargs = kwargs

            def login(self, username, password):
                self.username = username
                self.password = password

            def sendmail(self, sender, recipients, msg, **kwargs):
                self.sender = sender
                self.recipients = recipients
                self.msg = msg
                self.send_args = kwargs

            @staticmethod
            def quit():
                self.quit = True

        class MockSMTPSSL(MockSMTPBase):
            type = 'smtpssl'

        class MockLMTP(MockSMTPBase):
            type = 'lmtp'

        class MockSMTP(MockSMTPBase):
            type = 'smtp'

        monkeypatch.setattr(smtplib, 'SMTP_SSL', MockSMTPSSL)
        monkeypatch.setattr(smtplib, 'LMTP', MockLMTP)
        monkeypatch.setattr(smtplib, 'SMTP', MockSMTP)

    def test_mail(self, mocksmtp):
        smtp_initargs = {
            'host': 'localhost',
            'port': 123,
            'local_hostname': 'mycomputer.fqdn.com',
            'timeout': 3,
            'source_address': ('machine', 456),
        }
        username = 'user'
        password = 'pass'
        email_config_dict = {
            'connection_type': 'ssl',
            'username': username,
            'password': password
        }
        email_config_dict.update(smtp_initargs)

        recipients = ['larry@gmail.com', 'moe@gmail.com', 'curly@gmail.com']
        subject = 'hello'
        body = 'hello there'
        sender = 'me@gmail.com'
        mail_options = ['a', 'b']
        rcpt_options = [1, 2]

        with Email(email_config_dict=email_config_dict) as email:
            assert email.server.type == 'smtpssl'
            assert email.server.initargs == smtp_initargs
            assert email.server.username == username
            assert email.server.password == password
            email.send(recipients, subject, body, sender=sender, mail_options=mail_options, rcpt_options=rcpt_options)
            assert email.server.sender == sender
            assert email.server.recipients == recipients
            assert email.server.msg == '''Content-Type: text/plain; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Subject: hello
From: me@gmail.com
To: larry@gmail.com, moe@gmail.com, curly@gmail.com

hello there'''
            assert email.server.send_args == {'mail_options': ['a', 'b'], 'rcpt_options': [1, 2]}
        assert self.quit is True

    def test_json(self, mocksmtp, email_json):
        with Email(email_config_json=email_json) as email:
            assert email.server.type == 'lmtp'

    def test_yaml(self, mocksmtp, email_yaml):
        with Email(email_config_yaml=email_yaml) as email:
            assert email.server.type == 'smtp'

    def test_fail(self, mocksmtp, email_json, email_yaml):
        email_config_dict = {
            'connection_type': 'ssl',
        }
        with pytest.raises(ConfigurationError):
            with Email(email_config_dict=email_config_dict, email_config_json=email_json) as email:
                pass
        with pytest.raises(ConfigurationError):
            with Email(email_config_dict=email_config_dict, email_config_yaml=email_yaml) as email:
                pass
