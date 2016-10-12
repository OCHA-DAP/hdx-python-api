#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Downloading utilities for urls"""

from tempfile import NamedTemporaryFile
from typing import Optional

import requests


class DownloadError(Exception):
    pass


def download_file(url: str, path: Optional[str] = None) -> str:
    """Download file from url and store in path

    Args:
        url (str): URL to download
        path (Optional[str]): path to download it to. Defaults to None.

    Returns:
        str: Path of downloaded file

    """
    try:
        r = requests.get(url, stream=True)
    except Exception as e:
        raise DownloadError('Download of %s failed in setup of stream!' % url) from e
    try:
        if path:
            f = open(path, 'wb')
        else:
            f = NamedTemporaryFile('wb', delete=False)
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
        return f.name
    except Exception as e:
        raise DownloadError('Download of %s failed in retrieval of stream!' % url) from e
    finally:
        f.close()
