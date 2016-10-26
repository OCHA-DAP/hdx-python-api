import tempfile
from os import unlink
from os.path import join, abspath

import pytest

from hdx.utilities.downloader import download_file, DownloadError, get_headers, download, get_path_for_url
from hdx.utilities.path import script_dir


class TestDownloader():
    def test_get_path_for_url(self):
        scriptdir = script_dir(TestDownloader)
        path = get_path_for_url(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv', scriptdir)
        assert abspath(path) == abspath(join(scriptdir, 'test_data.csv'))
        downloader_folder = join(scriptdir, '..', '..', 'fixtures', 'downloader')
        path = get_path_for_url(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv',
            downloader_folder)
        assert abspath(path) == abspath(join(downloader_folder, 'test_data3.csv'))

    def test_download_file(self):
        tmpdir = tempfile.gettempdir()
        with pytest.raises(DownloadError):
            download_file('NOTEXIST://NOTEXIST.csv', tmpdir)
        with pytest.raises(DownloadError):
            download_file(
                'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv')
        f = download_file(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv', tmpdir)
        fpath = abspath(f)
        unlink(f)
        assert fpath == abspath(join(tmpdir, 'test_data.csv'))

    def test_get_headers(self):
        with pytest.raises(DownloadError):
            get_headers('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError):
            get_headers('https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv')
        headers = get_headers(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv')
        assert headers['Content-Length'] == '479'

    def test_download(self):
        with pytest.raises(DownloadError):
            download('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError):
            download('https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv')
        result = download(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv')
        assert result['Content-Length'] == '479'
