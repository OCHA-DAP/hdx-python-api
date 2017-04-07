# -*- coding: UTF-8 -*-
"""Downloader Tests"""
import tempfile
from os import unlink
from os.path import join, abspath

import pytest

from hdx.utilities.downloader import Download, DownloadError
from hdx.utilities.path import script_dir


class TestDownloader:
    @pytest.fixture
    def fixtureurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'

    @pytest.fixture
    def fixturenotexistsurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv'

    def test_get_path_for_url(self, fixtureurl):
        scriptdir = script_dir(TestDownloader)
        path = Download.get_path_for_url(fixtureurl, scriptdir)
        assert abspath(path) == abspath(join(scriptdir, 'test_data.csv'))
        downloader_folder = join(scriptdir, '..', '..', 'fixtures', 'downloader')
        path = Download.get_path_for_url(fixtureurl, downloader_folder)
        assert abspath(path) == abspath(join(downloader_folder, 'test_data3.csv'))

    def test_setup_stream(self, fixtureurl, fixturenotexistsurl):
        with pytest.raises(DownloadError), Download() as download:
            download.setup_stream('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError), Download() as download:
            download.setup_stream(fixturenotexistsurl)
        with Download() as download:
            download.setup_stream(fixtureurl)
            headers = download.response.headers
            assert headers['Content-Length'] == '728'

    def test_hash_stream(self, fixtureurl):
        with Download() as download:
            download.setup_stream(fixtureurl)
            md5hash = download.hash_stream(fixtureurl)
            assert md5hash == 'da9db35a396cca10c618f6795bdb9ff2'

    def test_download_file(self, fixtureurl, fixturenotexistsurl):
        tmpdir = tempfile.gettempdir()
        with pytest.raises(DownloadError), Download() as download:
            download.download_file('NOTEXIST://NOTEXIST.csv', tmpdir)
        with pytest.raises(DownloadError), Download() as download:
            download.download_file(fixturenotexistsurl)
        with Download() as download:
            f = download.download_file(fixtureurl, tmpdir)
            fpath = abspath(f)
            unlink(f)
            assert fpath == abspath(join(tmpdir, 'test_data.csv'))

    def test_download(self, fixtureurl, fixturenotexistsurl):
        with pytest.raises(DownloadError), Download() as download:
            download.download('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError), Download() as download:
            download.download(fixturenotexistsurl)
        with Download() as download:
            result = download.download(fixtureurl)
            assert result.headers['Content-Length'] == '728'
