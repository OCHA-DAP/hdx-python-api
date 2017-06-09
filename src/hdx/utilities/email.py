# -*- coding: utf-8 -*-
'''
Emailer:
----------------------

Utility to simplify sending emails

'''
import logging
import smtplib
from email.mime.text import MIMEText
from os.path import join, expanduser

from hdx.configuration import ConfigurationError
from hdx.utilities.loader import load_yaml, load_json

logger = logging.getLogger(__name__)


class Email:
    """Emailer utility. Parameters in dictionary or file:
    connection_type: "ssl"   ("ssl" for smtp ssl or "lmtp", otherwise basic smtp is assumed)
    host: "localhost"
    port: 123
    local_hostname: "mycomputer.fqdn.com"
    timeout: 3
    username: "user"
    password: "pass"

    Args:
        **kwargs: See below
        email_config_dict (dict): HDX configuration dictionary OR
        email_config_json (str): Path to JSON HDX configuration OR
        email_config_yaml (str): Path to YAML HDX configuration. Defaults to ~/hdx_email_configuration.yml.
    """
    def __init__(self, **kwargs):
        # type: (...) -> None
        email_config_found = False
        email_config_dict = kwargs.get('email_config_dict', None)
        if email_config_dict:
            email_config_found = True
            logger.info('Loading email configuration from dictionary')

        email_config_json = kwargs.get('email_config_json', '')
        if email_config_json:
            if email_config_found:
                raise ConfigurationError('More than one email configuration file given!')
            email_config_found = True
            logger.info('Loading email configuration from: %s' % email_config_json)
            email_config_dict = load_json(email_config_json)

        email_config_yaml = kwargs.get('email_config_yaml', None)
        if email_config_found:
            if email_config_yaml:
                raise ConfigurationError('More than one email configuration file given!')
        else:
            if not email_config_yaml:
                logger.info('No email configuration parameter. Using default email configuration path.')
                email_config_yaml = join(expanduser('~'), 'hdx_email_configuration.yml')
            logger.info('Loading email configuration from: %s' % email_config_yaml)
            email_config_dict = load_yaml(email_config_yaml)
        # Create server object
        connection_type = email_config_dict.get('connection_type', 'smtp')
        host = email_config_dict.get('host', '')
        port = email_config_dict.get('port', 0)
        local_hostname = email_config_dict.get('local_hostname', None)
        timeout = email_config_dict.get('timeout', None)
        source_address = email_config_dict.get('source_address', None)
        if connection_type.lower() == 'ssl':
            self.server = smtplib.SMTP_SSL(host=host, port=port, local_hostname=local_hostname,
                                           timeout=timeout, source_address=source_address)
        elif connection_type.lower() == 'lmtp':
            self.server = smtplib.LMTP(host=host, port=port, local_hostname=local_hostname,
                                       source_address=source_address)
        else:
            self.server = smtplib.SMTP(host=host, port=port, local_hostname=local_hostname,
                                       timeout=timeout, source_address=source_address)
        self.server.login(email_config_dict['username'], email_config_dict['password'])

    def __enter__(self):
        # type: () -> 'Email'
        """
        Return Email object for with statement

        Returns:
            None

        """
        return self

    def __exit__(self, *args):
        # type: (...) -> None
        """
        Close Email object for end of with statement

        Args:
            *args: Not used

        Returns:
            None

        """
        self.close()

    def send(self, recipients, subject, body, sender, **kwargs):
        # type: (str, str, str, str, ...) -> None
        """
        Send email

        Args:
            recipient (str): email recipient
            subject (str): email subject
            body (str): email body
            sender (str): email sender
            **kwargs: See below
            mail_options (list): mail options
            rcpt_options (list): recipient options

        Returns:
            None

        """
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)

        # Perform operations via server
        self.server.sendmail(sender, recipients, msg.as_string(), **kwargs)

    def close(self):
        # type: () -> None
        """
        Close connection to email server

        Returns:
            None

        """
        self.server.quit()
