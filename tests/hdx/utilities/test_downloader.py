import os
import tempfile

import pytest

from hdx.utilities.downloader import download_file, DownloadError, get_headers


class TestDownloader():
    def test_download_file(self):
        tmpdir = tempfile.gettempdir()
        tmpfile = os.path.join(tmpdir, 'HDXTempFile.tmp')
        with pytest.raises(DownloadError):
            download_file('NOTEXIST://NOTEXIST.csv', tmpfile)
        with pytest.raises(DownloadError):
            download_file(
                'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv', tmpfile)
        f = download_file(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv', tmpfile)
        assert f == tmpfile
        os.unlink(f)

    def test_get_headers(self):
        with pytest.raises(DownloadError):
            get_headers('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError):
            get_headers('https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/NOTEXIST.csv')
        headers = get_headers(
            'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/test_data.csv')
        assert headers['Content-Length'] == '479'
