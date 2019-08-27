# -*- coding: utf-8 -*-
"""Helper for handling resource uploads to GitHub.
"""
import logging
from typing import Optional

from github import Github, GithubException
from github.GithubObject import NotSet

import hdx.hdx_configuration

logger = logging.getLogger(__name__)


class GithubHelper(object):
    @staticmethod
    def github_dict_from_args(args):
        github_args = args.split(',')
        github = {'access_token': github_args[0], 'repository': github_args[1]}
        if github_args[2]:
            github['base_folder'] = github_args[2]
        if github_args[3]:
            github['branch'] = github_args[3]
        return github

    @staticmethod
    def create_or_update_in_github(configuration, file_to_upload):
        # type: (hdx.hdx_configuration.Configuration, str) -> Optional[str]
        """Helper method to create or update a file in GitHub and return the url

        Args:
            configuration (Configuration): HDX configuration
            file_to_upload (str): File to upload to GitHub

        Returns:
            Optional[str]: url of file in GitHub or None if there is no GitHub configuration
        """
        if not configuration.use_github:
            return None
        if 'github' not in configuration:
            raise GithubException('use_github flag set but no GitHub configuration provided!')
        with open(file_to_upload, 'rb') as input_file:
            data = input_file.read()
        if not data:
            raise GithubException('%s cannot be read!' % file_to_upload)
        github = Github(configuration['github']['access_token'])
        repository = configuration['github']['repository']
        repo = github.get_repo(repository)
        base_folder = configuration['github'].get('base_folder', None)
        branch = configuration['github'].get('branch', NotSet)
        if base_folder:
            path = '%s/%s' % (base_folder, file_to_upload)
        else:
            path = file_to_upload
        try:
            contents = repo.get_contents(path, ref=branch)
            result = repo.update_file(contents.path, 'updating file', data, contents.sha, branch=branch)
        except GithubException:
            result = repo.create_file(path, 'creating file', data, branch=branch)
        sha = result['commit'].sha
        return '%s/%s/raw/%s/%s' % (configuration['github_url'], repository, sha, path)
