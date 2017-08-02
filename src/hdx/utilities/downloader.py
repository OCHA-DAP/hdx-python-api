# -*- coding: utf-8 -*-
"""Downloading utilities for urls"""
import hashlib
import logging
from os.path import splitext, join, exists
from posixpath import basename
from tempfile import gettempdir
from typing import Optional

import requests
from basicauth import decode
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
from six.moves.urllib.parse import urlparse

from hdx.utilities import raisefrom
from hdx.utilities.loader import load_file_to_str, load_json, load_yaml

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    pass


class Download(object):
    """Download class with various download operations.

    Args:
        **kwargs: See below
        auth (Tuple[str, str]): Authorisation information in tuple form (user, pass) OR
        basic_auth (str): Authorisation information in basic auth string form (Basic xxxxxxxxxxxxxxxx) OR
        basic_auth_file (str): Path to file containing authorisation information in basic auth string form (Basic xxxxxxxxxxxxxxxx)
        extra_params_dict (dict): Extra parameters to put on end of url as a dictionary OR
        extra_params_json (str): Path to JSON file containing extra parameters to put on end of url OR
        extra_params_yaml (str): Path to YAML file containing extra parameters to put on end of url
    """
    def __init__(self, **kwargs):
        # type: (...) -> None
        s = requests.Session()
        auth_found = False
        auth = kwargs.get('auth')
        if auth:
            auth_found = True
            logger.info('Loading authorisation from auth argument')
        basic_auth = kwargs.get('basic_auth')
        if basic_auth:
            if auth_found:
                raise DownloadError('More than one authorisation given!')
            auth_found = True
            logger.info('Loading authorisation from basic_auth argument')
            auth = decode(basic_auth)
        basic_auth_file = kwargs.get('basic_auth_file')
        if basic_auth_file:
            if auth_found:
                raise DownloadError('More than one authorisation given!')
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
                raise DownloadError('More than one set of extra parameters given!')
            extra_params_found = True
            logger.info('Loading extra parameters from: %s' % extra_params_json)
            extra_params_dict = load_json(extra_params_json)

        extra_params_yaml = kwargs.get('extra_params_yaml', '')
        if extra_params_found:
            if extra_params_yaml:
                raise DownloadError('More than one set of extra parameters given!')
        else:
            if extra_params_yaml:
                logger.info('Loading extra parameters from: %s' % extra_params_yaml)
                extra_params_dict = load_yaml(extra_params_yaml)
            else:
                extra_params_dict = dict()
        s.params = extra_params_dict

        retries = Retry(total=5, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504], raise_on_redirect=True,
                        raise_on_status=True)
        s.mount('http://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
        s.mount('https://', HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100))
        self.session = s
        self.response = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.response:
            self.response.close()
        self.session.close()

    @staticmethod
    def get_path_for_url(url, folder=None):
        # type: (str, Optional[str]) -> str
        """Get filename from url and join to provided folder or temporary folder if no folder supplied, ensuring uniqueness

        Args:
            url (str): URL to download
            folder (Optional[str]): Folder to download it to. Defaults to None (temporary folder).

        Returns:
            str: Path of downloaded file

        """
        urlpath = urlparse(url).path
        filenameext = basename(urlpath)
        filename, extension = splitext(filenameext)
        if not folder:
            folder = gettempdir()
        path = join(folder, '%s%s' % (filename, extension))
        count = 0
        while exists(path):
            count += 1
            path = join(folder, '%s%d%s' % (filename, count, extension))
        return path

    def setup_stream(self, url, timeout=None):
        # type: (str, Optional[float]) -> None
        """Setup streaming download from provided url

        Args:
            url (str): URL to download
            timeout (Optional[float]): Timeout for connecting to URL. Defaults to None (no timeout).


        """
        self.response = None
        try:
            self.response = self.session.get(url, stream=True, timeout=timeout)
            self.response.raise_for_status()
        except Exception as e:
            raisefrom(DownloadError, 'Setup of Streaming Download of %s failed!', e)

    def hash_stream(self, url):
        # type: (str) -> str
        """Stream file from url and hash it using MD5. Must call setup_streaming_download method first.

        Args:
            url (str): URL to download

        Returns:
            str: MD5 hash of file

        """
        md5hash = hashlib.md5()
        try:
            for chunk in self.response.iter_content(chunk_size=10240):
                if chunk:  # filter out keep-alive new chunks
                    md5hash.update(chunk)
            return md5hash.hexdigest()
        except Exception as e:
            raisefrom(DownloadError, 'Download of %s failed in retrieval of stream!' % url, e)

    def stream_file(self, url, folder=None):
        # type: (str, Optional[str]) -> str
        """Stream file from url and store in provided folder or temporary folder if no folder supplied.
        Must call setup_streaming_download method first.

        Args:
            url (str): URL to download
            folder (Optional[str]): Folder to download it to. Defaults to None (temporary folder).

        Returns:
            str: Path of downloaded file

        """
        path = self.get_path_for_url(url, folder)
        f = None
        try:
            f = open(path, 'wb')
            for chunk in self.response.iter_content(chunk_size=10240):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    f.flush()
            return f.name
        except Exception as e:
            raisefrom(DownloadError, 'Download of %s failed in retrieval of stream!' % url, e)
        finally:
            if f:
                f.close()

    def download_file(self, url, folder=None, timeout=None):
        # type: (str, Optional[str], Optional[float]) -> str
        """Download file from url and store in provided folder or temporary folder if no folder supplied

        Args:
            url (str): URL to download
            folder (Optional[str]): Folder to download it to. Defaults to None.
            timeout (Optional[float]): Timeout for connecting to URL. Defaults to None (no timeout).

        Returns:
            str: Path of downloaded file

        """
        self.setup_stream(url, timeout)
        return self.stream_file(url, folder)

    def download(self, url, timeout=None):
        # type: (str, Optional[float]) -> requests.Response
        """Download url

        Args:
            url (str): URL to download
            timeout (Optional[float]): Timeout for connecting to URL. Defaults to None (no timeout).

        Returns:
            requests.Response: Response

        """
        try:
            self.response = self.session.get(url, timeout=timeout)
            self.response.raise_for_status()
        except Exception as e:
            raisefrom(DownloadError, 'Download of %s failed!' % url, e)
        return self.response
