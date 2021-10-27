"""Helper to the Dataset class for handling resources with filestores.
"""
from typing import Any, Dict, List

import hdx.data.resource
from hdx.utilities.dictandlist import merge_two_dictionaries


class FilestoreHelper:
    temporary_url = "updated_by_file_upload_step"

    @staticmethod
    def _get_ignore_fields(kwargs: Dict) -> List[str]:
        """Helper method to get ignore_fields from kwargs if it exists and add package_id

        Args:
            kwargs (Dict): Keyword arguments dictionary

        Returns:
            List[str]: Fields to ignore
        """
        ignore_fields = kwargs.get("ignore_fields", list())
        if "package_id" not in ignore_fields:
            ignore_fields.append("package_id")
        return ignore_fields

    @classmethod
    def check_filestore_resource(
        cls,
        resource: hdx.data.resource.Resource,
        filestore_resources: Dict[int, str],
        resource_index: int,
        **kwargs: Any,
    ) -> None:
        """Helper method to add new resource from dataset including filestore.

        Args:
            resource (Resource): Resource to check
            filestore_resources (Dict[int, str]): List of (index of resource, file to upload)
            resource_index (int): Index of resource

        Returns:
            None
        """
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            resource.check_required_fields(ignore_fields=cls._get_ignore_fields(kwargs))
        file_to_upload = resource.get_file_to_upload()
        if file_to_upload:
            filestore_resources[resource_index] = file_to_upload
            resource["url"] = cls.temporary_url

    @classmethod
    def dataset_merge_filestore_resource(
        cls,
        resource: hdx.data.resource.Resource,
        updated_resource: hdx.data.resource.Resource,
        filestore_resources: Dict[int, str],
        resource_index: int,
        **kwargs: Any,
    ) -> None:
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.

        Args:
            resource (Resource): Resource read from HDX
            updated_resource (Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            resource_index (int): Index of resource

        Returns:
            None
        """
        file_to_upload = updated_resource.get_file_to_upload()
        if file_to_upload:
            resource.set_file_to_upload(file_to_upload)
            filestore_resources[resource_index] = file_to_upload
        merge_two_dictionaries(resource, updated_resource)
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            if resource.get_file_to_upload() and "url" in resource.data:
                del resource.data["url"]
            resource.check_required_fields(ignore_fields=cls._get_ignore_fields(kwargs))
        if resource.get_file_to_upload():
            resource["url"] = cls.temporary_url
