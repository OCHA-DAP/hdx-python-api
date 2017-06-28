# -*- coding: UTF-8 -*-
"""Downloader Tests"""
import tempfile
from os import unlink
from os.path import join, abspath

import pytest

from hdx.utilities.downloader import Download, DownloadError


class TestDownloader:
    @pytest.fixture(scope='class')
    def downloaderfolder(self, fixturesfolder):
        return join(fixturesfolder, 'downloader')

    @pytest.fixture(scope='class')
    def fixtureurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv'

    @pytest.fixture(scope='class')
    def fixturenotexistsurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv'

    def test_get_path_for_url(self, fixtureurl, configfolder, downloaderfolder):
        path = Download.get_path_for_url(fixtureurl, configfolder)
        assert abspath(path) == abspath(join(configfolder, 'test_data.csv'))
        path = Download.get_path_for_url(fixtureurl, downloaderfolder)
        assert abspath(path) == abspath(join(downloaderfolder, 'test_data3.csv'))

    def test_init(self, downloaderfolder):
        basicauthfile = join(downloaderfolder, 'basicauth.txt')
        with Download(basicauthfile=basicauthfile) as download:
            assert download.session.auth == ('testuser', 'testpass')
        with pytest.raises(DownloadError):
            Download(auth=('u', 'p'), basicauth='Basic xxxxxxxxxxxxxxxx')
        with pytest.raises(DownloadError):
            Download(auth=('u', 'p'), basicauthfile=join('lala', 'lala.txt'))
        with pytest.raises(DownloadError):
            Download(basicauth='Basic xxxxxxxxxxxxxxxx', basicauthfile=join('lala', 'lala.txt'))
        with pytest.raises(IOError):
            Download(basicauthfile='NOTEXIST')

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
