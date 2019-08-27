# -*'coding: UTF-8 -*-
"""HDX GitHub helper Tests"""
import re
from os import mkdir
from os.path import join
from shutil import copyfile, rmtree

from github import Github
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

import hdx.hdx_configuration
from hdx.github_helper import GithubHelper


class TestGithubHelper:
    def test_github_dict_from_args(self):
        result = GithubHelper.github_dict_from_args('mytoken,myorg/myrepo,mybase/mybase2,dev')
        assert result == {'access_token': 'mytoken', 'base_folder': 'mybase/mybase2', 'branch': 'dev', 'repository': 'myorg/myrepo'}
        result = GithubHelper.github_dict_from_args('mytoken2,myorg/myrepo2,,')
        assert result == {'access_token': 'mytoken2', 'repository': 'myorg/myrepo2'}

    def test_create_or_update_in_github(self):
        configuration = hdx.hdx_configuration.Configuration(user_agent='test')
        configuration.set_use_github()
        file_to_upload = join('tests', 'fixtures', 'test_data.csv')
        outputfolder = 'temp'
        rmtree(outputfolder, ignore_errors=True)
        mkdir(outputfolder)
        filename = 'test_file.csv'
        outputfile = join(outputfolder, filename)
        copyfile(file_to_upload, outputfile)
        url = GithubHelper.create_or_update_in_github(configuration, outputfile)
        with Download(user_agent='test') as downloader:
            response = downloader.setup(url)
            assert response.headers['Content-Length'] == '728'
            file_to_upload = join('tests', 'fixtures', 'Accepted_Tags.csv')
            copyfile(file_to_upload, outputfile)
            url = GithubHelper.create_or_update_in_github(configuration, outputfile)
            rmtree(outputfolder)
            response = downloader.setup(url)
            assert response.headers['Content-Length'] == '2823'
        github = Github(configuration['github']['access_token'])
        repo = github.get_repo(configuration['github']['repository'])
        base_folder = configuration['github']['base_folder']
        branch = configuration['github']['branch']
        path = '%s/%s' % (base_folder, outputfile)
        sha = re.search('raw/(.*?)/', url).group(1)
        contents = repo.get_contents(path, ref=sha)
        repo.delete_file(contents.path, 'deleting file', contents.sha, branch=branch)
