# -*- coding: utf-8 -*-
"""Downloading utilities for urls"""
import hashlib
import logging
from os.path import splitext, join, exists
from posixpath import basename
from tempfile import gettempdir
from typing import Optional, Dict

import requests
from six.moves.urllib.parse import urlparse
from tabulator import Stream

from hdx.utilities import raisefrom
from hdx.utilities.session import get_session

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
        extra_params_dict (Dict[str]): Extra parameters to put on end of url as a dictionary OR
        extra_params_json (str): Path to JSON file containing extra parameters to put on end of url OR
        extra_params_yaml (str): Path to YAML file containing extra parameters to put on end of url
        status_forcelist (List[int]): HTTP statuses for which to force retry
    """
    def __init__(self, **kwargs):
        # type: (...) -> None
        self.session = get_session(**kwargs)
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

    @staticmethod
    def download_csv_key_value(url, delimiter=',', headers=None):
        # type: (str, str, Optional[int]) -> Dict
        """Download 2 column csv from url and return a dictionary of keys (first column) and values (second column)

        Args:
            url (str): URL to download
            delimiter (str): Delimiter for each row in csv. Defaults to ','.
            headers (Optional[int]): Number of row containing headers. Defaults to None.

        Returns:
            Dict: Dictionary keys (first column) and values (second column)

        """
        stream = Stream(url, delimiter=delimiter, headers=headers)
        stream.open()
        output_dict = dict()
        for row in stream.iter():
            if len(row) < 2:
                continue
            output_dict[row[0]] = row[1]
        stream.close()
        return output_dict

    @staticmethod
    def download_csv_rows_as_dicts(url, delimiter=',', headers=1):
        # type: (str, str, int) -> Dict[Dict]
        """Download multicolumn csv from url and return dictionary where keys are first column and values are
        dictionaries with keys from column headers and values from columns beneath

        Args:
            url (str): URL to download
            delimiter (str): Delimiter for each row in csv. Defaults to ','.
            headers (int): Number of row containing headers. Defaults to 1.

        Returns:
            Dict[Dict]: Dictionary where keys are first column and values are dictionaries with keys from column
            headers and values from columns beneath

        """
        stream = Stream(url, delimiter=delimiter, headers=headers)
        stream.open()
        output_dict = dict()
        headers = stream.headers
        first_header = headers[0]
        for row in stream.iter(keyed=True):
            first_val = row[first_header]
            output_dict[first_val] = dict()
            for header in row:
                if header == first_header:
                    continue
                else:
                    output_dict[first_val][header] = row[header]
        stream.close()
        return output_dict

    @staticmethod
    def download_csv_cols_as_dicts(url, delimiter=',', headers=1):
        # type: (str, str, int) -> Dict[Dict]
        """Download multicolumn csv from url and return dictionary where keys are header names and values are
        dictionaries with keys from first column and values from other columns

        Args:
            url (str): URL to download
            delimiter (str): Delimiter for each row in csv. Defaults to ','.
            headers (int): Number of row containing headers. Defaults to 1.

        Returns:
            Dict[Dict]: Dictionary where keys are header names and values are dictionaries with keys from first column
            and values from other columns

        """
        stream = Stream(url, delimiter=delimiter, headers=headers)
        stream.open()
        output_dict = dict()
        headers = stream.headers
        first_header = headers[0]
        for header in stream.headers:
            if header == first_header:
                continue
            output_dict[header] = dict()
        for row in stream.iter(keyed=True):
            for header in row:
                if header == first_header:
                    continue
                output_dict[header][row[first_header]] = row[header]
        stream.close()
        return output_dict
