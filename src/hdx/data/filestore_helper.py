# -*- coding: utf-8 -*-
"""Helper to the Dataset class for handling resources with filestores.
"""
from typing import List, Dict, Optional, Any

from hdx.utilities.dictandlist import merge_two_dictionaries

import hdx.data.resource


class FilestoreHelper(object):
    temporary_url = 'updated_by_file_upload_step'

    @classmethod
    def check_filestore_resource(cls, resource, ignore_fields, filestore_resources, **kwargs):
        # type: (hdx.data.resource.Resource, List[str], List[hdx.data.resource.Resource], Any) -> None
        """Helper method to add new resource from dataset including filestore.

        Args:
            resource (hdx.data.resource.Resource): Resource to check
            ignore_fields (List[str]): List of fields to ignore when checking resource
            filestore_resources (List[hdx.data.resource.Resource]): List of resources that use filestore (to be appended to)

        Returns:
            None
        """
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            resource.check_required_fields(ignore_fields=ignore_fields)
        if resource.get_file_to_upload():
            filestore_resources.append(resource)
            resource['url'] = cls.temporary_url

    @staticmethod
    def add_filestore_resources(resources_list, filestore_resources, batch_mode, **kwargs):
        # type: (List[Dict], List[hdx.data.resource.Resource], Optional[str], Any) -> None
        """Helper method to create files in filestore by updating resources.

        Args:
            resources_list (List[Dict]): List of resources from dataset dict's resources key
            filestore_resources (List[hdx.data.resource.Resource]): List of resources that use filestore (to be appended to)
            batch_mode (Optional[str]): batch_mode to be passed to resource_update call

        Returns:
            None
        """
        for resource in filestore_resources:
            for created_resource in resources_list:
                if resource['name'] == created_resource['name'] and resource['format'].upper() == created_resource['format'].upper():
                    merge_two_dictionaries(resource.data, created_resource)
                    del resource['url']
                    if batch_mode:
                        resource['batch_mode'] = batch_mode
                    resource.update_in_hdx(**kwargs)
                    merge_two_dictionaries(created_resource, resource.data)
                    break

    @classmethod
    def dataset_merge_filestore_resource(cls, resource, updated_resource, filestore_resources, ignore_fields, **kwargs):
        # type: (hdx.data.resource.Resource, hdx.data.resource.Resource, List[hdx.data.resource.Resource], List[str], Any) -> None
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.

        Args:
            resource (hdx.data.resource.Resource): Resource read from HDX
            updated_resource (hdx.data.resource.Resource): Updated resource from dataset
            filestore_resources (List[hdx.data.resource.Resource]): List of resources that use filestore (to be appended to)
            ignore_fields (List[str]): List of fields to ignore when checking resource

        Returns:
            None
        """
        if updated_resource.get_file_to_upload():
            resource.set_file_to_upload(updated_resource.get_file_to_upload())
            filestore_resources.append(resource)
        merge_two_dictionaries(resource, updated_resource)
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            resource.check_required_fields(ignore_fields=ignore_fields)
        if resource.get_file_to_upload():
            resource['url'] = cls.temporary_url

