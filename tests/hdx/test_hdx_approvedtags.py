# -*'coding: UTF-8 -*-
"""HDX Approved Tags Tests"""
import copy
import json

import pytest

from hdx.hdx_approvedtags import ApprovedTags
from hdx.hdx_configuration import Configuration
from tests.hdx.data.test_vocabulary import vocabulary_mockshow, vocabulary_delete


class TestHDXApprovedTags:
    @pytest.fixture(scope='function')
    def post_delete(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return vocabulary_delete(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def read(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = json.loads(data.decode('utf-8'))
                return vocabulary_mockshow(url, datadict)

        Configuration.read().remoteckan().session = MockSession()

    @pytest.fixture(scope='function')
    def post_createupdate(self):
        class MockSession(object):
            @staticmethod
            def post(url, data, headers, files, allow_redirects, auth=None):
                datadict = {'id': 'approved'}
                return vocabulary_mockshow(url, datadict, testshow=False)

        Configuration.read().remoteckan().session = MockSession()

    def test_delete_approved_vocabulary(self, configuration, post_delete):
        ApprovedTags.delete_approved_vocabulary()
        assert ApprovedTags._approved_vocabulary.data is None
        ApprovedTags._approved_vocabulary = None

    def test_get_approved_vocabulary(self, configuration, read):
        ApprovedTags._approved_vocabulary = None
        vocabulary = ApprovedTags.get_approved_vocabulary()
        assert vocabulary['name'] == 'approved'
        assert vocabulary['tags'][0]['name'] == '3-word addresses'

    def test_create_approved_vocabulary(self, configuration, post_createupdate):
        ApprovedTags._approved_vocabulary = None
        vocabulary = ApprovedTags.create_approved_vocabulary()
        assert vocabulary['name'] == 'approved'
        assert vocabulary['tags'][0]['name'] == '3-word addresses'

    def test_update_approved_vocabulary(self, configuration, post_createupdate):
        ApprovedTags._approved_vocabulary = None
        vocabulary = ApprovedTags.update_approved_vocabulary()
        assert vocabulary['name'] == 'approved'
        assert vocabulary['tags'][0]['name'] == '3-word addresses'

    def test_tag_mappings(self, configuration, read):
        tags_dict = ApprovedTags.read_tags_mappings()
        assert tags_dict['refugee'] == {'Action to Take': 'merge', 'New Tag(s)': 'refugees', 'Number of Public Datasets': '1822'}
        assert ApprovedTags.get_mapped_tag('refugee') == ['refugees']
        tags_dict['refugee']['Action to Take'] = 'ERROR'
        assert ApprovedTags.get_mapped_tag('refugee') == list()
        refugeesdict = copy.deepcopy(tags_dict['refugees'])
        del tags_dict['refugees']
        assert ApprovedTags.get_mapped_tag('refugees') == ['refugees']  # tag is in CKAN approved list but not tag cleanup spreadsheet
        tags_dict['refugees'] = refugeesdict
        ApprovedTags.get_approved_vocabulary().remove_tag('refugees')
        assert ApprovedTags.get_mapped_tag('refugees') == list()  # tag is not in CKAN approved list but is in tag cleanup spreadsheet
        ApprovedTags._approved_vocabulary = None
        ApprovedTags.get_approved_vocabulary()
