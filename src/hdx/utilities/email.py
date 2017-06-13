# -*- coding: utf-8 -*-
'''
Emailer:
----------------------

Utility to simplify sending emails

'''
import logging
import smtplib
from os.path import join, expanduser

from email_validator import validate_email
from six.moves.email_mime_text import MIMEText

from hdx.utilities.loader import load_yaml, load_json

logger = logging.getLogger(__name__)


class EmailConfigurationError(Exception):
    pass


class Email:
    """
    Emailer utility. Parameters in dictionary or file (eg. yaml below):
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
    """

    default_email_config_yaml = join(expanduser('~'), 'hdx_email_configuration.yml')

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
                raise EmailConfigurationError('More than one email configuration file given!')
            email_config_found = True
            logger.info('Loading email configuration from: %s' % email_config_json)
            email_config_dict = load_json(email_config_json)

        email_config_yaml = kwargs.get('email_config_yaml', None)
        if email_config_found:
            if email_config_yaml:
                raise EmailConfigurationError('More than one email configuration file given!')
        else:
            if not email_config_yaml:
                logger.info('No email configuration parameter. Using default email configuration path.')
                email_config_yaml = Email.default_email_config_yaml
            logger.info('Loading email configuration from: %s' % email_config_yaml)
            email_config_dict = load_yaml(email_config_yaml)

        self.connection_type = email_config_dict.get('connection_type', 'smtp')
        self.host = email_config_dict.get('host', '')
        self.port = email_config_dict.get('port', 0)
        self.local_hostname = email_config_dict.get('local_hostname')
        self.timeout = email_config_dict.get('timeout')
        self.source_address = email_config_dict.get('source_address')
        self.username = email_config_dict.get('username')
        self.password = email_config_dict.get('password')
        self.server = None

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
        pass

    def connect(self):
        # type: () -> None
        """
        Connect to server

        Returns:
            None

        """
        if self.connection_type.lower() == 'ssl':
            self.server = smtplib.SMTP_SSL(host=self.host, port=self.port, local_hostname=self.local_hostname,
                                           timeout=self.timeout, source_address=self.source_address)
        elif self.connection_type.lower() == 'lmtp':
            self.server = smtplib.LMTP(host=self.host, port=self.port, local_hostname=self.local_hostname,
                                       source_address=self.source_address)
        else:
            self.server = smtplib.SMTP(host=self.host, port=self.port, local_hostname=self.local_hostname,
                                       timeout=self.timeout, source_address=self.source_address)
        self.server.login(self.username, self.password)

    def close(self):
        # type: () -> None
        """
        Close connection to email server

        Returns:
            None

        """
        self.server.quit()

    def send(self, recipients, subject, body, sender=None, **kwargs):
        # type: (str, str, str, Optional[str], ...) -> None
        """
        Send email

        Args:
            recipient (str): Email recipient
            subject (str): Email subject
            body (str): Email body
            sender (Optional[str]): Email sender. Defaults to SMTP username.
            **kwargs: See below
            mail_options (list): Mail options (see smtplib documentation)
            rcpt_options (list): Recipient options (see smtplib documentation)

        Returns:
            None

        """
        if sender is None:
            sender = self.username
        v = validate_email(sender, check_deliverability=False)  # validate and get info
        sender = v['email']  # replace with normalized form
        normalised_recipients = list()
        for recipient in recipients:
            v = validate_email(recipient, check_deliverability=True)  # validate and get info
            normalised_recipients.append(v['email'])  # replace with normalized form

        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(normalised_recipients)

        # Perform operations via server
        self.connect()
        self.server.sendmail(sender, normalised_recipients, msg.as_string(), **kwargs)
        self.close()
