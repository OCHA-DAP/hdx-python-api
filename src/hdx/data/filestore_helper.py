# -*- coding: utf-8 -*-
"""Helper to the Dataset class for handling resources with filestores.
"""
import logging
from typing import List, Dict

from hdx.utilities.dictandlist import merge_two_dictionaries

import hdx.data.resource
from hdx.github_helper import GithubHelper
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class FilestoreHelper(object):
    temporary_url = 'updated_by_file_upload_step'

    @classmethod
    def check_filestore_resource(cls, configuration, resource, ignore_fields, filestore_resources):
        # type: (Configuration, hdx.data.resource.Resource, List[str], List[hdx.data.resource.Resource]) -> None
        """Helper method to add new resource from dataset including filestore.

        Args:
            configuration (Configuration): HDX configuration
            resource (Resource): Resource to check
            ignore_fields (List[str]): List of fields to ignore when checking resource
            filestore_resources (List[Resource]): List of resources that use filestore (to be appended to)

        Returns:
            None
        """
        resource.check_required_fields(ignore_fields=ignore_fields)
        file_to_upload = resource.get_file_to_upload()
        if file_to_upload:
            url = GithubHelper.create_or_update_in_github(configuration, file_to_upload)
            if url is None:
                filestore_resources.append(resource)
                resource['url'] = cls.temporary_url
            else:
                resource['url'] = url

    @staticmethod
    def add_filestore_resources(resources_list, filestore_resources):
        # type: (List[Dict], List[hdx.data.resource.Resource]) -> None
        """Helper method to create files in filestore by updating resources.

        Args:
            resources_list (List[Dict]): List of resources from dataset dict's resources key
            filestore_resources (List[Resource]): List of resources that use filestore (to be appended to)

        Returns:
            None
        """
        for resource in filestore_resources:
            for created_resource in resources_list:
                if resource['name'] == created_resource['name']:
                    merge_two_dictionaries(resource.data, created_resource)
                    del resource['url']
                    resource.update_in_hdx()
                    merge_two_dictionaries(created_resource, resource.data)
                    break

    @classmethod
    def merge_filestore_resource(cls, configuration, resource, updated_resource, filestore_resources, ignore_fields):
        # type: (Configuration, hdx.data.resource.Resource, hdx.data.resource.Resource, List[hdx.data.resource.Resource], List[str]) -> None
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.

        Args:
            configuration (Configuration): HDX configuration
            resource (Resource): Resource read from HDX
            updated_resource (hResource): Updated resource from dataset
            filestore_resources (List[Resource]): List of resources that use filestore (to be appended to)
            ignore_fields (List[str]): List of fields to ignore when checking resource

        Returns:
            None
        """
        url = None
        file_to_upload = updated_resource.get_file_to_upload()
        if file_to_upload:
            url = GithubHelper.create_or_update_in_github(configuration, file_to_upload)
            if url is None:
                resource.set_file_to_upload(file_to_upload)
                filestore_resources.append(resource)
        merge_two_dictionaries(resource, updated_resource)
        resource.check_required_fields(ignore_fields=ignore_fields)
        if file_to_upload:
            if url is None:
                resource['url'] = cls.temporary_url
            else:
                resource['url'] = url

    @classmethod
    def update_resources(cls, configuration, resources, updated_resources, filestore_resources, update_resources,
                         update_resources_by_name, remove_additional_resources):
        # type: (Configuration, List[hdx.data.resource.Resource], List[Dict], List[hdx.data.resource.Resource], bool, bool, bool) -> None
        """Helper method to check if dataset or its resources exist and update them

        Args:
            configuration (Configuration): HDX configuration
            resources (List[Resource]): List of resources read from HDX
            updated_resources (List[Dict]): List of updated resources
            filestore_resources (List[Resource]): List of resources that use filestore (to be appended to)
            update_resources (bool): Whether to update resources
            update_resources_by_name (bool): Compare resource names rather than position in list
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating)

        Returns:
            None
        """
        if not update_resources or not updated_resources:
            return
        ignore_fields = ['package_id']
        if update_resources_by_name:
            resource_names = set()
            for resource in resources:
                resource_name = resource['name']
                resource_names.add(resource_name)
                for updated_resource in updated_resources:
                    if resource_name == updated_resource['name']:
                        logger.warning('Resource exists. Updating %s' % resource_name)
                        cls.merge_filestore_resource(configuration, resource, updated_resource, filestore_resources, ignore_fields)
                        break
            updated_resource_names = set()
            for updated_resource in updated_resources:
                updated_resource_name = updated_resource['name']
                updated_resource_names.add(updated_resource_name)
                if not updated_resource_name in resource_names:
                    cls.check_filestore_resource(configuration, updated_resource, ignore_fields, filestore_resources)
                    resources.append(updated_resource)

            if remove_additional_resources:
                resources_to_delete = list()
                for i, resource in enumerate(resources):
                    resource_name = resource['name']
                    if resource_name not in updated_resource_names:
                        logger.warning('Removing additional resource %s!' % resource_name)
                        resources_to_delete.append(i)
                for i in sorted(resources_to_delete, reverse=True):
                    del resources[i]

        else:  # update resources by position
            for i, updated_resource in enumerate(updated_resources):
                if len(resources) > i:
                    updated_resource_name = updated_resource['name']
                    resource = resources[i]
                    resource_name = resource['name']
                    logger.warning('Resource exists. Updating %s' % resource_name)
                    if resource_name != updated_resource_name:
                        logger.warning('Changing resource name to: %s' % updated_resource_name)
                    cls.merge_filestore_resource(configuration, resource, updated_resource, filestore_resources, ignore_fields)
                else:
                    cls.check_filestore_resource(configuration, updated_resource, ignore_fields, filestore_resources)
                    resources.append(updated_resource)

            if remove_additional_resources:
                resources_to_delete = list()
                for i, resource in enumerate(resources):
                    if len(updated_resources) <= i:
                        logger.warning('Removing additional resource %s!' % resource['name'])
                        resources_to_delete.append(i)
                for i in sorted(resources_to_delete, reverse=True):
                    del resources[i]
