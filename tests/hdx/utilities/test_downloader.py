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

    @pytest.fixture(scope='class')
    def emptyurl(self):
        return 'https://raw.githubusercontent.com/OCHA-DAP/hdx-python-api/master/tests/fixtures/downloader/test_data1.csv/empty.txt'

    def test_get_path_for_url(self, fixtureurl, configfolder, downloaderfolder):
        path = Download.get_path_for_url(fixtureurl, configfolder)
        assert abspath(path) == abspath(join(configfolder, 'test_data.csv'))
        path = Download.get_path_for_url(fixtureurl, downloaderfolder)
        assert abspath(path) == abspath(join(downloaderfolder, 'test_data3.csv'))

    def test_init(self, downloaderfolder):
        basicauthfile = join(downloaderfolder, 'basicauth.txt')
        with Download(basic_auth_file=basicauthfile) as downloader:
            assert downloader.session.auth == ('testuser', 'testpass')
        with pytest.raises(DownloadError):
            Download(auth=('u', 'p'), basic_auth='Basic xxxxxxxxxxxxxxxx')
        with pytest.raises(DownloadError):
            Download(auth=('u', 'p'), basic_auth_file=join('lala', 'lala.txt'))
        with pytest.raises(DownloadError):
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
        with pytest.raises(DownloadError):
            Download(extra_params_dict={'key1': 'val1'}, extra_params_json=extraparamsjson)
        with pytest.raises(DownloadError):
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

    def test_download(self, fixtureurl, fixturenotexistsurl, emptyurl):
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download('NOTEXIST://NOTEXIST.csv')
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download(fixturenotexistsurl)
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download_csv_with_header(emptyurl)
        with pytest.raises(DownloadError), Download() as downloader:
            downloader.download_csv_key_value(emptyurl)
        with Download() as downloader:
            result = downloader.download(fixtureurl)
            assert result.headers['Content-Length'] == '728'
            result = downloader.download_csv(fixtureurl)
            assert result == ['GWNO,EVENT_ID_CNTY,EVENT_ID_NO_CNTY,EVENT_DATE,YEAR,TIME_PRECISION,EVENT_TYPE,ACTOR1,ALLY_ACTOR_1,INTER1,ACTOR2,ALLY_ACTOR_2,INTER2,INTERACTION,COUNTRY,ADMIN1,ADMIN2,ADMIN3,LOCATION,LATITUDE,LONGITUDE,GEO_PRECISION,SOURCE,NOTES,FATALITIES',
                              '615,1416RTA,,18/04/2001,2001,1,Violence against civilians,Police Forces of Algeria (1999-),,1,Civilians (Algeria),Berber Ethnic Group (Algeria),7,17,Algeria,Tizi Ouzou,Beni-Douala,,Beni Douala,36.61954,4.08282,1,Associated Press Online,A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.,1',
                              '615,2229RTA,,19/04/2001,2001,1,Riots/Protests,Rioters (Algeria),Berber Ethnic Group (Algeria),5,Police Forces of Algeria (1999-),,1,15,Algeria,Tizi Ouzou,Tizi Ouzou,,Tizi Ouzou,36.71183,4.04591,3,Kabylie report,"Riots were reported in numerous villages in Kabylie, resulting in dozens wounded in clashes between protesters and police and significant material damage.",0',
                              '615,2230RTA,,20/04/2001,2001,1,Riots/Protests,Protesters (Algeria),Students (Algeria),6,,,0,60,Algeria,Bejaia,Amizour,,Amizour,36.64022,4.90131,1,Crisis Group,Students protested in the Amizour area. At least 3 were later arrested for allegedly insulting gendarmes.,0',
                              '615,2231RTA,,21/04/2001,2001,1,Riots/Protests,Rioters (Algeria),Berber Ethnic Group (Algeria),5,Police Forces of Algeria (1999-),,1,15,Algeria,Bejaia,Amizour,,Amizour,36.64022,4.90131,1,Kabylie report,"Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.",0',
                              '']
            result = list()
            for row in downloader.download_csv_with_header(fixtureurl):
                result.append(row)
            assert result == [{'COUNTRY': 'Algeria', 'EVENT_ID_CNTY': '1416RTA', 'ADMIN3': '', 'ALLY_ACTOR_1': '', 'ADMIN1': 'Tizi Ouzou', 'ACTOR2': 'Civilians (Algeria)', 'ALLY_ACTOR_2': 'Berber Ethnic Group (Algeria)', 'FATALITIES': '1', 'GWNO': '615', 'EVENT_ID_NO_CNTY': '', 'SOURCE': 'Associated Press Online', 'INTER2': '7', 'GEO_PRECISION': '1', 'LATITUDE': '36.61954', 'YEAR': '2001', 'INTER1': '1', 'ADMIN2': 'Beni-Douala', 'TIME_PRECISION': '1', 'INTERACTION': '17', 'LOCATION': 'Beni Douala', 'ACTOR1': 'Police Forces of Algeria (1999-)', 'EVENT_TYPE': 'Violence against civilians', 'LONGITUDE': '4.08282', 'EVENT_DATE': '18/04/2001', 'NOTES': 'A Berber student was shot while in police custody at a police station in Beni Douala. He later died on Apr.21.'},
                              {'COUNTRY': 'Algeria', 'EVENT_ID_CNTY': '2229RTA', 'ADMIN3': '', 'ALLY_ACTOR_1': 'Berber Ethnic Group (Algeria)', 'ADMIN1': 'Tizi Ouzou', 'ACTOR2': 'Police Forces of Algeria (1999-)', 'ALLY_ACTOR_2': '', 'FATALITIES': '0', 'GWNO': '615', 'EVENT_ID_NO_CNTY': '', 'SOURCE': 'Kabylie report', 'INTER2': '1', 'GEO_PRECISION': '3', 'LATITUDE': '36.71183', 'YEAR': '2001', 'INTER1': '5', 'ADMIN2': 'Tizi Ouzou', 'TIME_PRECISION': '1', 'INTERACTION': '15', 'LOCATION': 'Tizi Ouzou', 'ACTOR1': 'Rioters (Algeria)', 'EVENT_TYPE': 'Riots/Protests', 'LONGITUDE': '4.04591', 'EVENT_DATE': '19/04/2001', 'NOTES': 'Riots were reported in numerous villages in Kabylie, resulting in dozens wounded in clashes between protesters and police and significant material damage.'},
                              {'COUNTRY': 'Algeria', 'EVENT_ID_CNTY': '2230RTA', 'ADMIN3': '', 'ALLY_ACTOR_1': 'Students (Algeria)', 'ADMIN1': 'Bejaia', 'ACTOR2': '', 'ALLY_ACTOR_2': '', 'FATALITIES': '0', 'GWNO': '615', 'EVENT_ID_NO_CNTY': '', 'SOURCE': 'Crisis Group', 'INTER2': '0', 'GEO_PRECISION': '1', 'LATITUDE': '36.64022', 'YEAR': '2001', 'INTER1': '6', 'ADMIN2': 'Amizour', 'TIME_PRECISION': '1', 'INTERACTION': '60', 'LOCATION': 'Amizour', 'ACTOR1': 'Protesters (Algeria)', 'EVENT_TYPE': 'Riots/Protests', 'LONGITUDE': '4.90131', 'EVENT_DATE': '20/04/2001', 'NOTES': 'Students protested in the Amizour area. At least 3 were later arrested for allegedly insulting gendarmes.'},
                              {'COUNTRY': 'Algeria', 'EVENT_ID_CNTY': '2231RTA', 'ADMIN3': '', 'ALLY_ACTOR_1': 'Berber Ethnic Group (Algeria)', 'ADMIN1': 'Bejaia', 'ACTOR2': 'Police Forces of Algeria (1999-)', 'ALLY_ACTOR_2': '', 'FATALITIES': '0', 'GWNO': '615', 'EVENT_ID_NO_CNTY': '', 'SOURCE': 'Kabylie report', 'INTER2': '1', 'GEO_PRECISION': '1', 'LATITUDE': '36.64022', 'YEAR': '2001', 'INTER1': '5', 'ADMIN2': 'Amizour', 'TIME_PRECISION': '1', 'INTERACTION': '15', 'LOCATION': 'Amizour', 'ACTOR1': 'Rioters (Algeria)', 'EVENT_TYPE': 'Riots/Protests', 'LONGITUDE': '4.90131', 'EVENT_DATE': '21/04/2001', 'NOTES': 'Rioters threw molotov cocktails, rocks and burning tires at gendarmerie stations in Beni Douala, El-Kseur and Amizour.'}]
            result = downloader.download_csv_key_value(fixtureurl)
            assert result == {'615': '2231RTA', 'GWNO': 'EVENT_ID_CNTY'}
