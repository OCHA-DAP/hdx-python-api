"""Helper to the Dataset class for handling resources with filestores."""

from typing import TYPE_CHECKING, Any, Dict

from hdx.utilities.dateparse import now_utc_notz

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
        if check_upload and resource.get_file_to_upload() and "url" in resource.data:
            del resource.data["url"]
        ignore_fields = kwargs.get("ignore_fields", list())
        resource_ignore_fields = []
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
        resource_data_to_update: "Resource",
        filestore_resources: Dict[int, str],
        resource_index: int,
        **kwargs: Any,
    ) -> None:
        """Helper method to add new resource from dataset including filestore.

        Args:
            resource_data_to_update (Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resource, file to upload)
            resource_index (int): Index of resource

        Returns:
            None
        """
        cls.resource_check_required_fields(resource_data_to_update, **kwargs)
        file_to_upload = resource_data_to_update.get_file_to_upload()
        if file_to_upload:
            filestore_resources[resource_index] = file_to_upload
            resource_data_to_update["url"] = cls.temporary_url

    @classmethod
    def dataset_update_filestore_resource(
        cls,
        resource_data_to_update: "Resource",
        filestore_resources: Dict[int, str],
        resource_index: int,
    ) -> None:
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.

        Args:
            resource_data_to_update (Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            resource_index (int): Index of resource

        Returns:
            None
        """
        file_to_upload = resource_data_to_update.get_file_to_upload()
        if file_to_upload:
            filestore_resources[resource_index] = file_to_upload
            resource_data_to_update["url"] = cls.temporary_url

        data_updated = resource_data_to_update.is_marked_data_updated()
        if data_updated:
            # Should not output timezone info here
            resource_data_to_update["last_modified"] = now_utc_notz().isoformat(
                timespec="microseconds"
            )
            resource_data_to_update.data_updated = False
