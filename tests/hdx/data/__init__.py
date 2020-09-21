import copy
import json


class MockResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

    def json(self):
        return json.loads(self.text)


resultgroups = [{'description': '', 'name': 'dza', 'image_display_url': '', 'display_name': 'Algeria', 'id': 'dza',
                 'title': 'Algeria'},
                {'description': '', 'name': 'zwe', 'image_display_url': '', 'display_name': 'Zimbabwe', 'id': 'zwe',
                 'title': 'Zimbabwe'}]

resulttags = [{'state': 'active', 'display_name': 'conflict', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b',
               'id': '1dae41e5-eacd-4fa5-91df-8d80cf579e53', 'name': 'conflict'},
              {'state': 'active', 'display_name': 'political violence', 'vocabulary_id': '4381925f-0ae9-44a3-b30d-cae35598757b',
               'id': 'aaafc63b-2234-48e3-8ccc-198d7cf0f3f3', 'name': 'political violence'}]

dataset_data = {
    'name': 'MyDataset1',
    'title': 'MyDataset1',
    'dataset_date': '06/04/2016',  # has to be MM/DD/YYYY
    'groups': resultgroups,
    'owner_org': 'My Org',
    'author': 'AN Other',
    'author_email': 'another@another.com',
    'maintainer': 'AN Other',
    'maintainer_email': 'another@another.com',
    'license_id': 'cc-by-sa',
    'subnational': '1',
    'notes': 'some notes',
    'caveats': 'some caveats',
    'data_update_frequency': '7',
    'methodology': 'other',
    'methodology_other': 'methodology description',
    'dataset_source': 'Mr Org',
    'package_creator': 'AN Other',
    'private': False,
    'url': None,
    'state': 'active',
    'tags': resulttags
}

resources_data = [{'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'description': 'Resource1',
                   'name': 'Resource1',
                   'url': 'http://resource1.xlsx',
                   'format': 'xlsx'},
                  {'id': '3d777226-96aa-4239-860a-703389d16d1f', 'description': 'Resource2',
                   'name': 'Resource2',
                   'url': 'http://resource2.csv',
                   'format': 'csv'},
                  {'id': '3d777226-96aa-4239-860a-703389d16d1g', 'description': 'Resource2',
                   'name': 'Resource2',
                   'url': 'http://resource2.xls',
                   'format': 'xls'}
                  ]

organization_data = {
    'name': 'MyOrganization1',
    'title': 'Humanitarian Organization',
    'description': 'We do humanitarian work',
}

user_data = {
    'name': 'MyUser1',
    'email': 'xxx@yyy.com',
    'password': 'xxx',
    'fullname': 'xxx xxx',
    'about': 'Data Scientist',
    'capacity': 'admin'
}

dataset_resultdict = {
    'resources': [{'revision_id': '43765383-1fce-471f-8166-d6c8660cc8a9', 'cache_url': None,
                   'datastore_active': False, 'format': 'xlsx', 'webstore_url': None,
                   'last_modified': None, 'tracking_summary': {'recent': 0, 'total': 0},
                   'id': 'de6549d8-268b-4dfe-adaf-a4ae5c8510d5', 'webstore_last_updated': None,
                   'mimetype': None, 'state': 'active', 'created': '2016-06-07T08:57:27.367939',
                   'description': 'Resource1', 'position': 0,
                   'hash': '', 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                   'name': 'Resource1',
                   'url': 'http://resource1.xlsx',
                   'resource_type': 'api', 'url_type': 'api', 'size': None, 'mimetype_inner': None,
                   'cache_last_updated': None},
                  {'revision_id': '387e5d1a-50ca-4039-b5a7-f7b6b88d0f2b', 'cache_url': None,
                   'datastore_active': False, 'format': 'zipped csv', 'webstore_url': None,
                   'last_modified': None, 'tracking_summary': {'recent': 0, 'total': 0},
                   'id': '3d777226-96aa-4239-860a-703389d16d1f', 'webstore_last_updated': None,
                   'mimetype': None, 'state': 'active', 'created': '2016-06-07T08:57:27.367959',
                   'description': 'Resource2', 'position': 1,
                   'hash': '', 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                   'name': 'Resource2',
                   'url': 'http://resource2_csv.zip',
                   'resource_type': 'api', 'url_type': 'api', 'size': None, 'mimetype_inner': None,
                   'cache_last_updated': None},
                  {'revision_id': '387e5d1a-50ca-4039-b5a7-f7b6b88d0f2c', 'cache_url': None,
                   'datastore_active': False, 'format': 'geojson', 'webstore_url': None,
                   'last_modified': None, 'tracking_summary': {'recent': 0, 'total': 0},
                   'id': '3d777226-96aa-4239-860a-703389d16d1g', 'webstore_last_updated': None,
                   'mimetype': None, 'state': 'active', 'created': '2016-06-07T08:57:27.367959',
                   'description': 'Resource2', 'position': 2,
                   'hash': '', 'package_id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
                   'name': 'Resource2',
                   'url': 'http://resource2_csv.zip',
                   'resource_type': 'api', 'url_type': 'api', 'size': None, 'mimetype_inner': None,
                   'cache_last_updated': None},
                  ],
    'isopen': True, 'caveats': 'Various',
    'revision_id': '032833ca-c403-40cc-8b86-69d5a6eecb1b', 'url': None, 'author': 'acled',
    'metadata_created': '2016-03-23T14:28:48.664205',
    'license_url': 'http://www.opendefinition.org/licenses/cc-by-sa',
    'relationships_as_object': [], 'creator_user_id': '154de241-38d6-47d3-a77f-0a9848a61df3',
    'methodology_other': "This page contains information.",
    'subnational': '1', 'maintainer_email': 'me@me.com',
    'license_title': 'Creative Commons Attribution Share-Alike',
    'title': 'MyDataset', 'private': False,
    'maintainer': '8b84230c-e04a-43ec-99e5-41307a203a2f', 'methodology': 'Other', 'num_tags': 4, 'license_id': 'cc-by-sa',
    'tracking_summary': {'recent': 32, 'total': 178}, 'relationships_as_subject': [],
    'owner_org': 'b67e6c74-c185-4f43-b561-0e114a736f19', 'id': '6f36a41c-f126-4b18-aaaf-6c2ddfbc5d4d',
    'dataset_source': 'ACLED', 'type': 'dataset',
    'notes': 'Notes',
    'organization': {'revision_id': '684f3eee-b708-4f91-bd22-7860d4eca423', 'description': 'MyOrganisation',
                     'name': 'acled', 'type': 'organization', 'image_url': '',
                     'approval_status': 'approved', 'state': 'active',
                     'title': 'MyOrganisation',
                     'created': '2015-01-09T14:44:54.006612',
                     'id': 'b67e6c74-c185-4f43-b561-0e114a736f19',
                     'is_organization': True},
    'state': 'active', 'author_email': 'me@me.com', 'package_creator': 'someone',
    'num_resources': 2, 'total_res_downloads': 4, 'name': 'MyDataset1',
    'metadata_modified': '2016-06-09T12:49:33.854367',
    'groups': resultgroups,
    'data_update_frequency': '7',
    'tags': resulttags,
    'version': None,
    'solr_additions': '{"countries": ["Algeria", "Zimbabwe"]}',
    'dataset_date': '06/04/2016'}


def dataset_mockshow(url, datadict):
    if 'show' not in url:
        return MockResponse(404,
                            '{"success": false, "error": {"message": "TEST ERROR: Not show", "__type": "TEST ERROR: Not Show Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
    if 'resource_show' in url:
        result = json.dumps(resources_data[0])
        return MockResponse(200,
                            '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=resource_show"}' % result)
    else:
        if datadict['id'] == 'TEST1':
            result = json.dumps(dataset_resultdict)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)
        if datadict['id'] == 'DatasetExist':
            result = json.dumps(dataset_resultdict)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)
        if datadict['id'] == 'TEST2':
            return MockResponse(404,
                                '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
        if datadict['id'] == 'TEST3':
            return MockResponse(200,
                                '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')
        if datadict['id'] == 'TEST4':
            resultdictcopy = copy.deepcopy(dataset_resultdict)
            resultdictcopy['id'] = 'TEST4'
            result = json.dumps(resultdictcopy)
            return MockResponse(200,
                                '{"success": true, "result": %s, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}' % result)

    return MockResponse(404,
                        '{"success": false, "error": {"message": "Not found", "__type": "Not Found Error"}, "help": "http://test-data.humdata.org/api/3/action/help_show?name=package_show"}')