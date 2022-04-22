"""Resource class containing all logic for creating, checking, and updating resources."""
import datetime
import logging
from os.path import join
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from hdx.utilities.downloader import Download
from hdx.utilities.uuid import is_valid_uuid

import hdx.data.dataset
import hdx.data.filestore_helper as filestore_helper
from hdx.api.configuration import Configuration
from hdx.data.date_helper import DateHelper
from hdx.data.hdxobject import HDXError, HDXObject
from hdx.data.resource_view import ResourceView

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """Resource class containing all logic for creating, checking, and updating resources.

    Args:
        initial_data (Optional[Dict]): Initial resource metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    _formats_dict = None

    def __init__(
        self,
        initial_data: Optional[Dict] = None,
        configuration: Optional[Configuration] = None,
    ) -> None:
        if not initial_data:
            initial_data = dict()
        super().__init__(initial_data, configuration=configuration)
        self.file_to_upload = None

    @staticmethod
    def actions() -> Dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            "show": "resource_show",
            "update": "resource_update",
            "create": "resource_create",
            "patch": "resource_patch",
            "delete": "resource_delete",
            "search": "resource_search",
            "datastore_delete": "datastore_delete",
            "datastore_search": "datastore_search",
        }

    def update_from_yaml(
        self, path: str = join("config", "hdx_resource_static.yml")
    ) -> None:
        """Update resource metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_resource_static.yml.

        Returns:
            None
        """
        super().update_from_yaml(path)

    def update_from_json(
        self, path: str = join("config", "hdx_resource_static.json")
    ) -> None:
        """Update resource metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_resource_static.json.

        Returns:
            None
        """
        super().update_from_json(path)

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Optional[Configuration] = None
    ) -> Optional["Resource"]:
        """Reads the resource given by identifier from HDX and returns Resource object

        Args:
            identifier (str): Identifier of resource
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Resource]: Resource object if successful read, None if not
        """

        if is_valid_uuid(identifier) is False:
            raise HDXError(f"{identifier} is not a valid resource id!")
        return cls._read_from_hdx_class("resource", identifier, configuration)

    def get_date_of_resource(
        self,
        date_format: Optional[str] = None,
        today: datetime.date = datetime.date.today(),
    ) -> Dict:
        """Get resource date as datetimes and strings in specified format. If no format is supplied, the ISO 8601
        format is used. Returns a dictionary containing keys startdate (start date as datetime), enddate (end
        date as datetime), startdate_str (start date as string), enddate_str (end date as string) and ongoing
        (whether the end date is a rolls forward every day).

        Args:
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.
            today (datetime.date): Date to use for today. Defaults to date.today.

        Returns:
            Dict: Dictionary of date information
        """
        return DateHelper.get_date_info(
            self.data.get("daterange_for_data"), date_format, today
        )

    def set_date_of_resource(
        self,
        startdate: Union[datetime.datetime, str],
        enddate: Union[datetime.datetime, str],
    ) -> None:
        """Set resource date from either datetime.datetime objects or strings.

        Args:
            startdate (Union[datetime.datetime, str]): Dataset start date
            enddate (Union[datetime.datetime, str]): Dataset end date

        Returns:
            None
        """
        self.data["daterange_for_data"] = DateHelper.get_hdx_date(
            startdate, enddate
        )

    @classmethod
    def read_formats_mappings(
        cls,
        configuration: Optional[Configuration] = None,
        url: Optional[str] = None,
    ) -> Dict:
        """
        Read HDX formats list

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            url (Optional[str]): Url of tags cleanup spreadsheet. Defaults to None (internal configuration parameter).

        Returns:
            Dict: Returns formats dictionary
        """
        if not cls._formats_dict:
            if configuration is None:
                configuration = Configuration.read()
            with Download(
                full_agent=configuration.get_user_agent()
            ) as downloader:
                if url is None:
                    url = configuration["formats_mapping_url"]
                downloader.download(url)
                cls._formats_dict = dict()
                for format_data in downloader.get_json():
                    format = format_data[0].lower()
                    if format == "_comment":
                        continue
                    cls._formats_dict[format] = format
                    for file_type in format_data[3]:
                        cls._formats_dict[file_type.lower()] = format
        return cls._formats_dict

    @classmethod
    def set_formatsdict(cls, formats_dict: Dict) -> None:
        """
        Set formats dictionary

        Args:
            formats_dict (Dict): Formats dictionary

        Returns:
            None
        """
        cls._formats_dict = formats_dict

    @classmethod
    def get_mapped_format(
        cls, file_type: str, configuration: Optional[Configuration] = None
    ) -> Optional[str]:
        """Given a format, return a format to which it maps

        Args:
            file_type (str): File type to map
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[str]: Mapped format or None if no mapping found
        """
        if configuration is None:
            configuration = Configuration.read()
        if not file_type:
            return None
        file_type = file_type.lower()
        mappings = cls.read_formats_mappings(configuration=configuration)
        format = mappings.get(file_type)
        if format is None:
            if file_type[0] == ".":
                file_type = file_type[1:]
            else:
                file_type = f".{file_type}"
            format = mappings.get(file_type)
        return format

    def get_file_type(self) -> Optional[str]:
        """Get the resource's file type

        Returns:
            Optional[str]: Resource's file type or None if it has not been set
        """
        format = self.data.get("format")
        if format:
            format = format.lower()
        return format

    def set_file_type(self, file_type: str) -> str:
        """Set the resource's file type

        Args:
            file_type (str): File type to set on resource
            log_none (bool): Whether to log an informational message about the file type being None. Defaults to True.

        Returns:
            str: Format that was set
        """
        format = self.get_mapped_format(
            file_type, configuration=self.configuration
        )
        if not format:
            raise HDXError(
                f"Supplied file type {file_type} is invalid and could not be mapped to a known type!"
            )
        self.data["format"] = format
        return format

    def clean_file_type(self) -> str:
        """Clean the resource's file type, setting it to None if it is invalid and cannot be mapped

        Returns:
            str: Format that was set
        """
        return self.set_file_type(self.data.get("format"))

    def get_file_to_upload(self) -> Optional[str]:
        """Get the file uploaded

        Returns:
            Optional[str]: The file that will be or has been uploaded or None if there isn't one
        """
        return self.file_to_upload

    def set_file_to_upload(
        self, file_to_upload: str, guess_format_from_suffix: bool = False
    ) -> str:
        """Delete any existing url and set the file uploaded to the local path provided

        Args:
            file_to_upload (str): Local path to file to upload
            guess_format_from_suffix (bool): Whether to try to set format based on file suffix. Defaults to False.

        Returns:
            Optional[str]: The format that was guessed or None if no format was set
        """
        if "url" in self.data:
            del self.data["url"]
        self.file_to_upload = file_to_upload
        format = None
        if guess_format_from_suffix:
            format = self.set_file_type(Path(file_to_upload).suffix)
        return format

    def check_url_filetoupload(self) -> None:
        """Check if url or file to upload provided for resource and add resource_type and url_type if not supplied.
        Correct the file type.

        Returns:
            None
        """
        if self.file_to_upload is None:
            if "url" in self.data:
                if "resource_type" not in self.data:
                    self.data["resource_type"] = "api"
                if "url_type" not in self.data:
                    self.data["url_type"] = "api"
            else:
                raise HDXError(
                    "Either a url or a file to upload must be supplied!"
                )
        else:
            if "url" in self.data:
                if (
                    self.data["url"]
                    != filestore_helper.FilestoreHelper.temporary_url
                ):
                    raise HDXError(
                        "Either a url or a file to upload must be supplied not both!"
                    )
            if "resource_type" not in self.data:
                self.data["resource_type"] = "file.upload"
            if "url_type" not in self.data:
                self.data["url_type"] = "upload"
            if "tracking_summary" in self.data:
                del self.data["tracking_summary"]
        self.clean_file_type()

    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Check that metadata for resource is complete. The parameter ignore_fields should be set if required to
        any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self.check_url_filetoupload()
        self._check_required_fields("resource", ignore_fields)

    def _get_files(self) -> Dict:
        """Return the files parameter for CKANAPI

        Returns:
            Dict: files parameter for CKANAPI
        """
        if self.file_to_upload is None:
            return dict()
        return {"upload": self.file_to_upload}

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Check if resource exists in HDX and if so, update it

        Args:
            **kwargs: See below
            operation (string): Operation to perform eg. patch. Defaults to update.

        Returns:
            None
        """
        self._check_load_existing_object("resource", "id")
        if self.file_to_upload and "url" in self.data:
            del self.data["url"]
        self._merge_hdx_update(
            "resource", "id", self._get_files(), True, **kwargs
        )

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            self.check_required_fields()
        id = self.data.get("id")
        files = self._get_files()
        if id and self._load_from_hdx("resource", id):
            logger.warning(f"{'resource'} exists. Updating {id}")
            if self.file_to_upload and "url" in self.data:
                del self.data["url"]
            self._merge_hdx_update("resource", "id", files, True, **kwargs)
        else:
            self._save_to_hdx("create", "name", files, True)

    def delete_from_hdx(self) -> None:
        """Deletes a resource from HDX

        Returns:
            None
        """
        self._delete_from_hdx("resource", "id")

    def get_dataset(self) -> "Dataset":  # noqa: F821
        """Return dataset containing this resource

        Returns:
            Dataset: Dataset containing this resource
        """
        package_id = self.data.get("package_id")
        if package_id is None:
            raise HDXError("Resource has no package id!")
        return hdx.data.dataset.Dataset.read_from_hdx(package_id)

    @staticmethod
    def search_in_hdx(
        query: str,
        configuration: Optional[Configuration] = None,
        **kwargs: Any,
    ) -> List["Resource"]:
        """Searches for resources in HDX. NOTE: Does not search dataset metadata!

        Args:
            query (str): Query
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            order_by (str): A field on the Resource model that orders the results
            offset (int): Apply an offset to the query
            limit (int): Apply a limit to the query
        Returns:
            List[Resource]: List of resources resulting from query
        """

        resources = []
        resource = Resource(configuration=configuration)
        success, result = resource._read_from_hdx(
            "resource", query, "query", Resource.actions()["search"]
        )
        if result:
            count = result.get("count", None)
            if count:
                for resourcedict in result["results"]:
                    resource = Resource(
                        resourcedict, configuration=configuration
                    )
                    resources.append(resource)
        else:
            logger.debug(result)
        return resources

    def download(self, folder: Optional[str] = None) -> Tuple[str, str]:
        """Download resource store to provided folder or temporary folder if no folder supplied

        Args:
            folder (Optional[str]): Folder to download resource to. Defaults to None.

        Returns:
            Tuple[str, str]: (URL downloaded, Path to downloaded file)

        """
        # Download the resource
        url = self.data.get("url", None)
        if not url:
            raise HDXError("No URL to download!")
        logger.debug(f"Downloading {url}")
        filename = self.data["name"]
        format = f".{self.data['format']}"
        if format not in filename:
            filename = f"{filename}{format}"
        apikey = self.configuration.get_api_key()
        if apikey:
            headers = {"Authorization": self.configuration.get_api_key()}
        else:
            headers = None
        with Download(
            full_agent=self.configuration.get_user_agent(), headers=headers
        ) as downloader:
            path = downloader.download_file(
                url, folder=folder, filename=filename
            )
            return url, path

    @staticmethod
    def get_all_resource_ids_in_datastore(
        configuration: Optional[Configuration] = None,
    ) -> List[str]:
        """Get list of resources that have a datastore returning their ids.

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[str]: List of resource ids that are in the datastore
        """
        resource = Resource(configuration=configuration)
        success, result = resource._read_from_hdx(
            "datastore",
            "_table_metadata",
            "resource_id",
            Resource.actions()["datastore_search"],
            limit=10000,
        )
        resource_ids = list()
        if not success:
            logger.debug(result)
        else:
            for record in result["records"]:
                resource_ids.append(record["name"])
        return resource_ids

    def has_datastore(self) -> bool:
        """Check if the resource has a datastore.

        Returns:
            bool: Whether the resource has a datastore or not
        """
        success, result = self._read_from_hdx(
            "datastore",
            self.data["id"],
            "resource_id",
            self.actions()["datastore_search"],
        )
        if not success:
            logger.debug(result)
        else:
            if result:
                return True
        return False

    def delete_datastore(self) -> None:
        """Delete a resource from the HDX datastore

        Returns:
            None
        """
        success, result = self._read_from_hdx(
            "datastore",
            self.data["id"],
            "resource_id",
            self.actions()["datastore_delete"],
            force=True,
        )
        if not success:
            logger.debug(result)

    def get_resource_views(self) -> List[ResourceView]:
        """Get any resource views in the resource

        Returns:
            List[ResourceView]: List of resource views
        """
        return ResourceView.get_all_for_resource(self.data["id"])

    def _get_resource_view(
        self, resource_view: Union[ResourceView, Dict]
    ) -> ResourceView:
        """Get resource view id

        Args:
            resource_view (Union[ResourceView,Dict]): ResourceView metadata from a ResourceView object or dictionary

        Returns:
            ResourceView: ResourceView object
        """
        if isinstance(resource_view, dict):
            resource_view = ResourceView(
                resource_view, configuration=self.configuration
            )
        if isinstance(resource_view, ResourceView):
            return resource_view
        raise HDXError(
            f"Type {type(resource_view).__name__} is not a valid resource view!"
        )

    def add_update_resource_view(
        self, resource_view: Union[ResourceView, Dict]
    ) -> None:
        """Add new resource view in resource with new metadata

        Args:
            resource_view (Union[ResourceView,Dict]): Resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None

        """
        resource_view = self._get_resource_view(resource_view)
        resource_view.create_in_hdx()

    def add_update_resource_views(
        self, resource_views: List[Union[ResourceView, Dict]]
    ) -> None:
        """Add new or update existing resource views in resource with new metadata.

        Args:
            resource_views (List[Union[ResourceView,Dict]]): A list of resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError("ResourceViews should be a list!")
        for resource_view in resource_views:
            self.add_update_resource_view(resource_view)

    def reorder_resource_views(
        self, resource_views: List[Union[ResourceView, Dict, str]]
    ) -> None:
        """Order resource views in resource.

        Args:
            resource_views (List[Union[ResourceView,Dict,str]]): A list of either resource view ids or resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError("ResourceViews should be a list!")
        ids = list()
        for resource_view in resource_views:
            if isinstance(resource_view, str):
                resource_view_id = resource_view
            else:
                resource_view_id = resource_view["id"]
            if is_valid_uuid(resource_view_id) is False:
                raise HDXError(
                    f"{resource_view} is not a valid resource view id!"
                )
            ids.append(resource_view_id)
        _, result = self._read_from_hdx(
            "resource view",
            self.data["id"],
            "id",
            ResourceView.actions()["reorder"],
            order=ids,
        )

    def delete_resource_view(
        self, resource_view: Union[ResourceView, Dict, str]
    ) -> None:
        """Delete a resource view from the resource and HDX

        Args:
            resource_view (Union[ResourceView,Dict,str]): Either a resource view id or resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None
        """
        if isinstance(resource_view, str):
            if is_valid_uuid(resource_view) is False:
                raise HDXError(
                    f"{resource_view} is not a valid resource view id!"
                )
            resource_view = ResourceView(
                {"id": resource_view}, configuration=self.configuration
            )
        else:
            resource_view = self._get_resource_view(resource_view)
            if "id" not in resource_view:
                found = False
                title = resource_view.get("title")
                for rv in self.get_resource_views():
                    if resource_view["title"] == rv["title"]:
                        resource_view = rv
                        found = True
                        break
                if not found:
                    raise HDXError(
                        f"No resource views have title {title} in this resource!"
                    )
        resource_view.delete_from_hdx()

    def enable_dataset_preview(self) -> None:
        """Enable dataset preview of resource

        Returns:
            None
        """
        self.data["dataset_preview_enabled"] = "True"

    def disable_dataset_preview(self) -> None:
        """Disable dataset preview of resource

        Returns:
            None
        """
        self.data["dataset_preview_enabled"] = "False"
