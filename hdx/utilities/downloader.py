#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Downloading utilities for urls"""

from os.path import splitext, join, exists
from posixpath import basename
from tempfile import gettempdir
from typing import Optional
from urllib.parse import urlparse

import requests


class DownloadError(Exception):
    pass


def get_path_for_url(url: str, folder: Optional[str] = None) -> str:
    """Get filename from url and join to provided folder or temporary folder if no folder supplied, ensuring uniqueness

    Args:
        url (str): URL to download
        folder (Optional[str]): Folder to download it to. Defaults to None.

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


def download_file(url: str, folder: Optional[str] = None) -> str:
    """Download file from url and store in provided folder or temporary folder if no folder supplied

    Args:
        url (str): URL to download
        folder (Optional[str]): Folder to download it to. Defaults to None.

    Returns:
        str: Path of downloaded file

    """
    try:
        r = requests.get(url, stream=True)
    except Exception as e:
        raise DownloadError('Download of %s failed in setup of stream!' % url) from e
    if r.status_code != 200:
        raise DownloadError('Download of %s failed in setup of stream!' % url)
    path = get_path_for_url(url, folder)
    f = None
    try:
        f = open(path, 'wb')
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
        return f.name
    except Exception as e:
        raise DownloadError('Download of %s failed in retrieval of stream!' % url) from e
    finally:
        if f:
            f.close()


def get_headers(url: str, timeout: Optional[float] = None) -> dict:
    """Download url headers

    Args:
        url (str): URL to download
        timeout (Optional[float]): Timeout for connecting to URL. Defaults to None (no timeout).

    Returns:
        dict: Headers of url

    """
    try:
        r = requests.head(url, timeout=timeout)
    except Exception as e:
        raise DownloadError('Download of %s failed! (HEAD)' % url) from e
    if r.status_code != 200:
        raise DownloadError('Download of %s failed with status %d! (HEAD)' % (url, r.status_code))
    return r.headers


def download(url: str, timeout: Optional[float] = None) -> dict:
    """Download url

    Args:
        url (str): URL to download
        timeout (Optional[float]): Timeout for connecting to URL. Defaults to None (no timeout).

    Returns:
        str: Response

    """
    try:
        r = requests.get(url, timeout=timeout)
    except Exception as e:
        raise DownloadError('Download of %s failed! (GET)' % url) from e
    if r.status_code != 200:
        raise DownloadError('Download of %s failed with status %d! (GET)' % (url, r.status_code))
    return r.headers
