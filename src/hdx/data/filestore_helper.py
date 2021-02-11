# -*- coding: utf-8 -*-
"""Helper to the Dataset class for handling resources with filestores.
"""
from typing import List, Dict, Any

from hdx.utilities.dictandlist import merge_two_dictionaries

import hdx.data.resource


class FilestoreHelper(object):
    temporary_url = 'updated_by_file_upload_step'

    @classmethod
    def check_filestore_resource(cls, resource, ignore_fields, filestore_resources, resource_index, **kwargs):
        # type: (hdx.data.resource.Resource, List[str], Dict[int, str], int, Any) -> None
        """Helper method to add new resource from dataset including filestore.

        Args:
            resource (hdx.data.resource.Resource): Resource to check
            ignore_fields (List[str]): List of fields to ignore when checking resource
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            resource_index (int): Index of resource

        Returns:
            None
        """
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            resource.check_required_fields(ignore_fields=ignore_fields)
        file_to_upload = resource.get_file_to_upload()
        if file_to_upload:
            filestore_resources[resource_index] = file_to_upload
            resource['url'] = cls.temporary_url

    @staticmethod
    def add_filestore_resources(resources_list, filestore_resources, **kwargs):
        # type: (List[Dict], Dict[int, str], Any) -> None
        """Helper method to create files in filestore by updating resources.

        Args:
            resources_list (List[Dict]): List of resources from dataset dict's resources key
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)

        Returns:
            None
        """
        for resource_index, file_to_upload in filestore_resources.items():
            resource = resources_list[resource_index]
            resource.set_file_to_upload(file_to_upload)
            resource.update_in_hdx(**kwargs)

    @classmethod
    def dataset_merge_filestore_resource(cls, resource, updated_resource, filestore_resources, resource_index, ignore_fields, **kwargs):
        # type: (hdx.data.resource.Resource, hdx.data.resource.Resource, Dict[int, str], int, List[str], Any) -> None
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.

        Args:
            resource (hdx.data.resource.Resource): Resource read from HDX
            updated_resource (hdx.data.resource.Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            resource_index (int): Index of resource
            ignore_fields (List[str]): List of fields to ignore when checking resource

        Returns:
            None
        """
        file_to_upload = updated_resource.get_file_to_upload()
        if file_to_upload:
            resource.set_file_to_upload(file_to_upload)
            filestore_resources[resource_index] = file_to_upload
        merge_two_dictionaries(resource, updated_resource)
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            if resource.get_file_to_upload() and 'url' in resource.data:
                del resource.data['url']
            resource.check_required_fields(ignore_fields=ignore_fields)
        if resource.get_file_to_upload():
            resource['url'] = cls.temporary_url

