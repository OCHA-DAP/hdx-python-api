"""Helper to the Dataset class for handling resources with filestores."""

import logging
from typing import TYPE_CHECKING, Any, Dict

from hdx.api.utilities.size_hash import get_size_and_hash
from hdx.utilities.dateparse import now_utc_notz

if TYPE_CHECKING:
    from hdx.data.resource import Resource

logger = logging.getLogger(__name__)


class FilestoreHelper:
    temporary_url = "updated_by_file_upload_step"

    @staticmethod
    def resource_check_required_fields(resource: "Resource", **kwargs: Any) -> None:
        """Helper method to get ignore_fields from kwargs if it exists and add package_id

        Args:
            resource (Resource): Resource to check
            **kwargs: Keyword arguments

        Returns:
            None
        """
        if "ignore_check" in kwargs:  # allow ignoring of field checks
            return
        ignore_fields = kwargs.get("ignore_fields", [])
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
    ) -> int:
        """Helper method to add new resource from dataset including filestore.
        Returns status code where:
        0 = no file to upload and last_modified set to now
        (resource creation or data_updated flag is True),
        1 = no file to upload and data_updated flag is False,
        2 = file uploaded to filestore (resource creation or either hash or size of file
        has changed),
        3 = file not uploaded to filestore (hash and size of file are the same),
        4 = file not uploaded (hash, size unchanged), given last_modified ignored

        Args:
            resource_data_to_update (Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resource, file to upload)
            resource_index (int): Index of resource

        Returns:
            int: Status code
        """
        resource_data_to_update.set_types()
        resource_data_to_update.correct_format(resource_data_to_update.data)
        cls.resource_check_required_fields(resource_data_to_update, **kwargs)
        file_to_upload = resource_data_to_update.get_file_to_upload()
        if file_to_upload:
            file_format = resource_data_to_update.get("format", "").lower()
            size, hash = get_size_and_hash(file_to_upload, file_format)
            filestore_resources[resource_index] = file_to_upload
            resource_data_to_update["url"] = cls.temporary_url
            resource_data_to_update["size"] = size
            resource_data_to_update["hash"] = hash
            return 2
        return 0

    @classmethod
    def dataset_update_filestore_resource(
        cls,
        original_resource_data: "Resource",
        resource_data_to_update: "Resource",
        filestore_resources: Dict[int, str],
        resource_index: int,
        **kwargs: Any,
    ) -> int:
        """Helper method to merge updated resource from dataset into HDX resource read from HDX including filestore.
        Returns status code where:
        0 = no file to upload and last_modified set to now
        (resource creation or data_updated flag is True),
        1 = no file to upload and data_updated flag is False,
        2 = file uploaded to filestore (resource creation or either hash or size of file
        has changed),
        3 = file not uploaded to filestore (hash and size of file are the same),
        4 = file not uploaded (hash, size unchanged), given last_modified ignored

        Args:
            original_resource_data (Resource): Original resource from dataset
            resource_data_to_update (Resource): Updated resource from dataset
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            resource_index (int): Index of resource
            **kwargs: Any,

        Returns:
            int: Status code
        """
        file_to_upload = resource_data_to_update.get_file_to_upload()
        if file_to_upload:
            force_update = kwargs.pop("force_update", False)
            file_format = resource_data_to_update.get("format", "").lower()
            size, hash = get_size_and_hash(file_to_upload, file_format)
            if (
                not force_update
                and size == original_resource_data.get("size")
                and hash == original_resource_data.get("hash")
            ):
                logger.warning(
                    f"Not updating filestore for resource {original_resource_data['name']} as size and hash unchanged!"
                )
                if resource_data_to_update._url_backup:
                    resource_data_to_update["url"] = resource_data_to_update._url_backup
                    resource_data_to_update._url_backup = None
                # ensure last_modified is not updated if file hasn't changed
                if "last_modified" in resource_data_to_update:
                    del resource_data_to_update["last_modified"]
                    status = 4
                else:
                    status = 3
            else:
                # update file if size or hash has changed
                filestore_resources[resource_index] = file_to_upload
                resource_data_to_update["resource_type"] = "file.upload"
                resource_data_to_update["url_type"] = "upload"
                if "tracking_summary" in resource_data_to_update:
                    del resource_data_to_update["tracking_summary"]
                resource_data_to_update["url"] = cls.temporary_url
                resource_data_to_update["size"] = size
                resource_data_to_update["hash"] = hash
                resource_data_to_update._url_backup = None
                status = 2
        else:
            if (
                "url" in resource_data_to_update
                and resource_data_to_update.get("url_type") != "upload"
            ):
                resource_data_to_update["resource_type"] = "api"
                resource_data_to_update["url_type"] = "api"
            if resource_data_to_update.is_marked_data_updated():
                # Should not output timezone info here
                resource_data_to_update["last_modified"] = now_utc_notz().isoformat(
                    timespec="microseconds"
                )
                resource_data_to_update._data_updated = False
                status = 0
            else:
                status = 1
        resource_data_to_update.correct_format(resource_data_to_update.data)
        return status
