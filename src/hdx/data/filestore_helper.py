"""Helper to the Dataset class for handling resources with filestores.
"""
from typing import TYPE_CHECKING, Any, Dict

from hdx.utilities.dictandlist import merge_two_dictionaries

if TYPE_CHECKING:
    from hdx.data.resource import Resource


class FilestoreHelper:
    temporary_url = "updated_by_file_upload_step"

    @staticmethod
    def resource_check_required_fields(
        resource: "Resource", check_upload: bool = False, **kwargs: Any
    ) -> None:
        """Helper method to get ignore_fields from kwargs if it exists and add package_id

        Args:
            resource (Resource): Resource to check
            check_upload (bool): Whether to check for file upload. Defaults to False.
            **kwargs: Keyword arguments

        Returns:
            None
        """
        if "ignore_check" in kwargs:  # allow ignoring of field checks
            return
        if (
            check_upload
            and resource.get_file_to_upload()
            and "url" in resource.data
        ):
            del resource.data["url"]
        ignore_fields = kwargs.get("ignore_fields", list())
        resource_ignore_fields = list()
        for ignore_field in ignore_fields:
            if ignore_field.startswith("resource:"):
                resource_ignore_field = ignore_field[9:].strip()
                resource_ignore_fields.append(resource_ignore_field)
        if "package_id" not in resource_ignore_fields:
            resource_ignore_fields.append("package_id")
        resource.check_required_fields(ignore_fields=resource_ignore_fields)

    @classmethod
    def check_filestore_resource(
        cls,
        resource: "Resource",
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
        cls.resource_check_required_fields(resource, **kwargs)
        file_to_upload = resource.get_file_to_upload()
        if file_to_upload:
            filestore_resources[resource_index] = file_to_upload
            resource["url"] = cls.temporary_url

    @classmethod
    def dataset_merge_filestore_resource(
        cls,
        resource: "Resource",
        updated_resource: "Resource",
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
        cls.resource_check_required_fields(
            resource, check_upload=True, **kwargs
        )
        if resource.get_file_to_upload():
            resource["url"] = cls.temporary_url
