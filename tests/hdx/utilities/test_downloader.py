# -*- coding: UTF-8 -*-
"""Downloader Tests"""
import tempfile
from os import unlink
from os.path import join, abspath

import pytest

from hdx.utilities.downloader import Download, DownloadError
from hdx.utilities.session import SessionError


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

    @pytest.fixture(scope='class')
    def fixtureprocessurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/downloader/test_csv_processing.csv?a=1'

    def test_get_path_for_url(self, fixtureurl, configfolder, downloaderfolder):
        path = Download.get_path_for_url(fixtureurl, configfolder)
        assert abspath(path) == abspath(join(configfolder, 'test_data.csv'))
        path = Download.get_path_for_url(fixtureurl, downloaderfolder)
        assert abspath(path) == abspath(join(downloaderfolder, 'test_data3.csv'))

    def test_init(self, downloaderfolder):
        basicauthfile = join(downloaderfolder, 'basicauth.txt')
        with Download(basic_auth_file=basicauthfile) as downloader:
            assert downloader.session.auth == ('testuser', 'testpass')
        with pytest.raises(SessionError):
            Download(auth=('u', 'p'), basic_auth='Basic xxxxxxxxxxxxxxxx')
        with pytest.raises(SessionError):
            Download(auth=('u', 'p'), basic_auth_file=join('lala', 'lala.txt'))
        with pytest.raises(SessionError):
            Download(basic_auth='Basic dXNlcjpwYXNz', basic_auth_file=join('lala', 'lala.txt'))
        with pytest.raises(IOError):
            Download(basic_auth_file='NOTEXIST')
        extraparamsjson = join(downloaderfolder, 'extra_params.json')
        extraparamsyaml = join(downloaderfolder, 'extra_params.yml')
        with Download(basic_auth_file=basicauthfile, extra_params_dict={'key1': 'val1'}) as downloader:
            assert downloader.session.auth == ('testuser', 'testpass')
            assert downloader.session.params == {'key1': 'val1'}
        with Download(extra_params_json=extraparamsjson) as downloader:
            assert downloader.session.params == {'param_1': 'value 1', 'param_2': 'value_2', 'param_3': 12}
        with Download(extra_params_yaml=extraparamsyaml) as downloader:
            assert downloader.session.params == {'param1': 'value1', 'param2': 'value 2', 'param3': 10}
        with pytest.raises(SessionError):
            Download(extra_params_dict={'key1': 'val1'}, extra_params_json=extraparamsjson)
        with pytest.raises(SessionError):
            Download(extra_params_dict={'key1': 'val1'}, extra_params_yaml=extraparamsyaml)
        with pytest.raises(IOError):
            Download(extra_params_json='NOTEXIST')

    def test_setup_stream(self, fixtureurl, fixturenotexistsurl):
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.setup_stream('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.setup_stream(fixturenotexistsurl)
        with Download() as downloader:
            downloader.setup_stream(fixtureurl)
            headers = downloader.response.headers
            assert headers['Content-Length'] == '728'

    def test_hash_stream(self, fixtureurl):
        with Download() as downloader:
            downloader.setup_stream(fixtureurl)
            md5hash = downloader.hash_stream(fixtureurl)
            assert md5hash == 'da9db35a396cca10c618f6795bdb9ff2'

    def test_download_file(self, fixtureurl, fixturenotexistsurl):
        tmpdir = tempfile.gettempdir()
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download_file('NOTEXIST://NOTEXIST.csv', tmpdir)
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download_file(fixturenotexistsurl)
        with Download() as downloader:
            f = downloader.download_file(fixtureurl, tmpdir)
            fpath = abspath(f)
            unlink(f)
            assert fpath == abspath(join(tmpdir, 'test_data.csv'))

    def test_download(self, fixtureurl, fixturenotexistsurl):
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download(fixturenotexistsurl)
        with Download() as downloader:
            result = downloader.download(fixtureurl)
            assert result.headers['Content-Length'] == '728'
            result = downloader.download_csv_key_value(fixtureurl)
            assert result == {'615': '2231RTA', 'GWNO': 'EVENT_ID_CNTY'}

    def test_download_csv_key_value(self, fixtureprocessurl):
        result = Download.download_csv_key_value(fixtureprocessurl, headers=2)
        assert result == {'coal': '3', 'gas': '2'}

    def test_download_csv_rows_as_dicts(self, fixtureprocessurl):
        result = Download.download_csv_rows_as_dicts(fixtureprocessurl, headers=2)
        assert result == {'coal': {'header2': '3', 'header3': '7.4', 'header4': "'needed'"},
                          'gas': {'header2': '2', 'header3': '6.5', 'header4': "'n/a'"}}

    def test_download_csv_cols_as_dicts(self, fixtureprocessurl):
        result = Download.download_csv_cols_as_dicts(fixtureprocessurl, headers=2)
        assert result == {'header2': {'coal': '3', 'gas': '2'},
                          'header3': {'coal': '7.4', 'gas': '6.5'},
                          'header4': {'coal': "'needed'", 'gas': "'n/a'"}}
