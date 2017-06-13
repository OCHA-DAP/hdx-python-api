# -*- coding: UTF-8 -*-
"""Email Tests"""
from os.path import join

import pytest

from hdx.utilities.email import Email, EmailConfigurationError


class TestEmail:
    @pytest.fixture(scope='class')
    def email_json(self):
        return join('tests', 'fixtures', 'config', 'hdx_email_configuration.json')

    @pytest.fixture(scope='class')
    def email_yaml(self):
        return join('tests', 'fixtures', 'config', 'hdx_email_configuration.yml')

    def test_mail(self, mocksmtp):
        smtp_initargs = {
            'host': 'localhost',
            'port': 123,
            'local_hostname': 'mycomputer.fqdn.com',
            'timeout': 3,
            'source_address': ('machine', 456),
        }
        username = 'user@user.com'
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
            email.send(recipients, subject, body, sender=sender, mail_options=mail_options, rcpt_options=rcpt_options)
            assert email.server.type == 'smtpssl'
            assert email.server.initargs == smtp_initargs
            assert email.server.username == username
            assert email.server.password == password
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
            email.send(recipients, subject, body, mail_options=mail_options, rcpt_options=rcpt_options)
            assert email.server.sender == username

    def test_json(self, mocksmtp, email_json):
        with Email(email_config_json=email_json) as email:
            email.connect()
            assert email.server.type == 'lmtp'

    def test_yaml(self, mocksmtp, email_yaml):
        with Email(email_config_yaml=email_yaml) as email:
            email.connect()
            assert email.server.type == 'smtp'

    def test_fail(self, mocksmtp, email_json, email_yaml):
        email_config_dict = {
            'connection_type': 'ssl',
        }
        default_email_config_yaml = Email.default_email_config_yaml
        Email.default_email_config_yaml = 'NOT EXIST'
        with pytest.raises(IOError):
            with Email() as email:
                pass
        Email.default_email_config_yaml = default_email_config_yaml
        with pytest.raises(EmailConfigurationError):
            with Email(email_config_dict=email_config_dict, email_config_json=email_json) as email:
                pass
        with pytest.raises(EmailConfigurationError):
            with Email(email_config_dict=email_config_dict, email_config_yaml=email_yaml) as email:
                pass
