"""Resource class containing all logic for creating, checking, and updating resources."""

import json
import logging
from collections.abc import Iterator, Sequence
from datetime import datetime
from pathlib import Path, PureWindowsPath
from typing import Any, Optional
from urllib.parse import urlsplit

from hdx.utilities.dateparse import now_utc_notz, parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.file_hashing import get_size_and_hash
from hdx.utilities.retriever import Retrieve
from hdx.utilities.session import get_session
from hdx.utilities.uuid import is_valid_uuid

import hdx.api.utilities.url_utils
import hdx.data.dataset
import hdx.data.resource_matcher
from hdx.api.configuration import Configuration
from hdx.api.utilities.date_helper import DateHelper
from hdx.data.hdxobject import HDXError, HDXObject
from hdx.data.resource_view import ResourceView

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """Resource class containing all logic for creating, checking, and updating
    resources.

    Args:
        initial_data: Initial resource metadata dictionary. Defaults to None.
        configuration: HDX configuration. Defaults to global configuration.
    """

    _formats_dict = None
    _valid_datastore_dump_formats = frozenset({"csv", "tsv", "json", "xml"})
    _valid_data_dictionary_types = frozenset(
        {"text", "numeric", "date", "timestamp without time zone"}
    )

    def __init__(
        self,
        initial_data: dict | None = None,
        configuration: Configuration | None = None,
    ) -> None:
        if not initial_data:
            initial_data = {}
        super().__init__(initial_data, configuration=configuration)
        self._file_to_upload = None
        self._data_updated = False
        self._url_backup = None

    @staticmethod
    def actions() -> dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dictionary of actions that can be performed on object
        """
        return {
            "show": "resource_show",
            "update": "resource_update",
            "create": "resource_create",
            "patch": "resource_patch",
            "delete": "resource_delete",
            "search": "resource_search",
            "broken": "hdx_mark_broken_link_in_resource",
            "datastore_delete": "datastore_delete",
            "datastore_create": "datastore_create",
            "datastore_insert": "datastore_insert",
            "datastore_upsert": "datastore_upsert",
            "datastore_search": "datastore_search",
            "datastore_search_sql": "datastore_search_sql",
            "datapusher_submit": "datapusher_submit",
            "datapusher_status": "datapusher_status",
            "push_to_datastore": "hdx_push_resource_to_datastore",
            "p_coded": "hdx_p_coded_resource_update",
            "in_hapi": "hdx_mark_resource_in_hapi",
            "in_quarantine": "hdx_mark_resource_in_quarantine",
            "qa_patch": "hdx_qa_resource_patch",
            "fs_check_revise": "hdx_fs_check_resource_revise",
            "fs_check_reset": "hdx_fs_check_resource_reset",
        }

    def update_from_yaml(
        self, path: Path | str = Path("config", "hdx_resource_static.yaml")
    ) -> None:
        """Update resource metadata with static metadata from YAML file

        Args:
            path: Path to YAML dataset metadata. Defaults to config/hdx_resource_static.yaml.

        Returns:
            None
        """
        super().update_from_yaml(path)

    def update_from_json(
        self, path: Path | str = Path("config", "hdx_resource_static.json")
    ) -> None:
        """Update resource metadata with static metadata from JSON file

        Args:
            path: Path to JSON dataset metadata. Defaults to config/hdx_resource_static.json.

        Returns:
            None
        """
        super().update_from_json(path)

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Configuration | None = None
    ) -> Optional["Resource"]:
        """Reads the resource given by identifier from HDX and returns Resource object

        Args:
            identifier: Identifier of resource
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Resource object if successful read, None if not
        """

        if is_valid_uuid(identifier) is False:
            raise HDXError(f"{identifier} is not a valid resource id!")
        return cls._read_from_hdx_class("resource", identifier, configuration)

    def get_date_of_resource(
        self,
        date_format: str | None = None,
        today: datetime | None = None,
    ) -> dict:
        """Get resource date as datetimes and strings in specified format. If no format
        is supplied, the ISO 8601 format is used. Returns a dictionary containing keys
        startdate (start date as datetime), enddate (end date as datetime),
        startdate_str (start date as string), enddate_str (end date as string) and
        ongoing (whether the end date is a rolls forward every day).

        Args:
            date_format: Date format. None is taken to be ISO 8601. Defaults to None.
            today: Date to use for today. Defaults to now_utc().

        Returns:
            Dictionary of date information
        """
        return DateHelper.get_time_period_info(
            self.data.get("daterange_for_data"), date_format, today
        )

    def set_date_of_resource(
        self,
        startdate: datetime | str,
        enddate: datetime | str,
        ignore_timeinfo: bool = True,
    ) -> None:
        """Set resource date from either datetime objects or strings. Any time and time
        zone information will be ignored by default (meaning that the time of the start
        date is set to 00:00:00, the time of any end date is set to 23:59:59 and the
        time zone is set to UTC). To have the time and time zone accounted for, set
        ignore_timeinfo to False. In this case, the time will be converted to UTC.

        Args:
            startdate: Resource start date
            enddate: Resource end date
            ignore_timeinfo: Ignore time and time zone of date. Defaults to True.

        Returns:
            None
        """
        self.data["daterange_for_data"] = DateHelper.get_hdx_time_period(
            startdate, enddate, ignore_timeinfo=ignore_timeinfo
        )

    @classmethod
    def read_formats_mappings(
        cls,
        configuration: Configuration | None = None,
        url: str | None = None,
    ) -> dict:
        """
        Read HDX formats list

        Args:
            configuration: HDX configuration. Defaults to global configuration.
            url: Url of tags cleanup spreadsheet. Defaults to None (internal configuration parameter).

        Returns:
            Returns formats dictionary
        """
        if not cls._formats_dict:
            if configuration is None:
                configuration = Configuration.read()
            with Download(
                full_agent=configuration.get_user_agent(),
                use_env=False,
            ) as downloader:
                if url is None:
                    url = configuration["formats_mapping_url"]
                downloader.download(url)
                cls._formats_dict = {}
                for format_data in downloader.get_json():
                    hdx_format = format_data[0].lower()
                    if hdx_format == "_comment":
                        continue
                    cls._formats_dict[hdx_format] = hdx_format
                    for file_format in format_data[3]:
                        cls._formats_dict[file_format.lower()] = hdx_format
        return cls._formats_dict

    @classmethod
    def set_formatsdict(cls, formats_dict: dict) -> None:
        """
        Set formats dictionary

        Args:
            formats_dict: Formats dictionary

        Returns:
            None
        """
        cls._formats_dict = formats_dict

    @classmethod
    def get_mapped_format(
        cls, format: str, configuration: Configuration | None = None
    ) -> str | None:
        """Given a file format, return an HDX format to which it maps

        Args:
            format: File type to map
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            Mapped format or None if no mapping found
        """
        if not format:
            return None
        if configuration is None:
            configuration = Configuration.read()
        file_format = format.lower()
        mappings = cls.read_formats_mappings(configuration=configuration)
        hdx_format = mappings.get(file_format)
        if hdx_format is None:
            if file_format[0] == ".":
                file_format = file_format[1:]
            else:
                file_format = f".{file_format}"
            hdx_format = mappings.get(file_format)
        return hdx_format

    def get_format(self) -> str | None:
        """Get the resource's format

        Returns:
            Resource's format or None if it has not been set
        """
        file_format = self.data.get("format")
        if file_format:
            file_format = file_format.lower()
        return file_format

    def set_format(self, format: str) -> str:
        """Set the resource's file type

        Args:
            format: Format to set on resource

        Returns:
            Format that was set
        """
        file_format = self.get_mapped_format(format, configuration=self.configuration)
        if not file_format:
            raise HDXError(
                f"Supplied file type {file_format} is invalid and could not be mapped to a known type!"
            )
        self.data["format"] = file_format
        return file_format

    def get_hdx_data_dictionary(self) -> list[dict] | None:
        """Get the resource's data dictionary (column definitions)

        Returns:
            List of column definition dictionaries or None if not set
        """
        data_dictionary = self.data.get("hdx_data_dictionary")
        if data_dictionary is None:
            return None
        if isinstance(data_dictionary, str):
            return json.loads(data_dictionary)
        return data_dictionary

    def set_hdx_data_dictionary(self, data_dictionary: Sequence[dict]) -> None:
        """Set the resource's data dictionary (column definitions). Each column
        definition must be a dict with non-empty string values for field
        (column name in the CSV file), label (human-readable column label) and
        description (human-readable description of the column). If present,
        data_type must be one of the PostgreSQL types produced by DataPusher+
        (text, numeric, date, timestamp without time zone).

        Args:
            data_dictionary: Sequence of column definition dictionaries

        Returns:
            None
        """
        if not data_dictionary:
            raise HDXError("hdx_data_dictionary must be a non-empty list!")
        for i, column in enumerate(data_dictionary):
            if not isinstance(column, dict):
                raise HDXError(f"hdx_data_dictionary[{i}] must be a dict!")
            for required_key in ("field", "label", "description"):
                value = column.get(required_key)
                if not isinstance(value, str) or not value.strip():
                    raise HDXError(
                        f"hdx_data_dictionary[{i}] is missing a non-empty '{required_key}'!"
                    )
            if "data_type" in column:
                data_type = column["data_type"]
                if data_type not in self._valid_data_dictionary_types:
                    valid_types = ", ".join(sorted(self._valid_data_dictionary_types))
                    raise HDXError(
                        f"hdx_data_dictionary[{i}] has invalid 'data_type' "
                        f"'{data_type}'! Must be one of: {valid_types}"
                    )
        self.data["hdx_data_dictionary"] = json.dumps(
            data_dictionary, separators=(",", ":")
        )

    def update_hdx_data_dictionary(
        self,
        data_dictionary: Sequence[dict],
        replace: bool = False,
        update_datastore: bool = True,
    ) -> None:
        """Update the resource's data dictionary (column definitions) in HDX
        without changing last_modified, so no dataset update notifications are
        sent. Column definitions are as for set_hdx_data_dictionary. Unless
        replace is True, they are merged by field into the existing data
        dictionary in the resource metadata (so call read_from_hdx first).
        The resource's hdx_data_dictionary field is patched, then if
        update_datastore is True and the resource has a datastore, the
        datastore field labels and descriptions are updated using
        datastore_create without reloading the data.

        Args:
            data_dictionary: Sequence of column definition dictionaries
            replace: Replace rather than merge into existing data dictionary. Defaults to False.
            update_datastore: Update datastore field labels and descriptions. Defaults to True.

        Returns:
            None
        """
        if replace:
            merged = [dict(column) for column in data_dictionary]
        else:
            new_columns = {column["field"]: column for column in data_dictionary}
            merged = []
            for column in self.get_hdx_data_dictionary() or []:
                new_column = new_columns.pop(column.get("field"), None)
                merged.append({**column, **new_column} if new_column else column)
            merged.extend(new_columns.values())
        self.set_hdx_data_dictionary(merged)
        self._write_to_hdx(
            "patch",
            {
                "id": self.data["id"],
                "hdx_data_dictionary": self.data["hdx_data_dictionary"],
            },
            "id",
        )
        if not update_datastore or not self.has_datastore():
            return
        columns = {column["field"]: column for column in merged}
        fields = []
        for field in self.get_datastore_fields():
            info = dict(field.get("info") or {})
            column = columns.pop(field["id"], None)
            if column:
                info["label"] = column["label"]
                info["notes"] = column["description"]
            fields.append({"id": field["id"], "type": field["type"], "info": info})
        if columns:
            logger.warning(
                f"Data dictionary fields {', '.join(columns)} not in datastore for resource {self.data['id']}!"
            )
        self._write_to_hdx(
            "datastore_create",
            {"resource_id": self.data["id"], "force": True, "fields": fields},
            "resource_id",
        )

    def clean_format(self) -> str:
        """Clean the resource's format, setting it to None if it is invalid and
        cannot be mapped

        Returns:
            Format that was set
        """
        return self.set_format(self.data.get("format"))

    def get_file_to_upload(self) -> str | None:
        """Get the file uploaded

        Returns:
            The file that will be or has been uploaded or None if there isn't one
        """
        return self._file_to_upload

    def set_file_to_upload(
        self, file_to_upload: Path | str, guess_format_from_suffix: bool = False
    ) -> str:
        """Delete any existing url and set the file uploaded to the local path provided

        Args:
            file_to_upload: Local path to file to upload
            guess_format_from_suffix: Set format from file suffix. Defaults to False.

        Returns:
            The format that was guessed or None if no format was set
        """
        if "url" in self.data:
            self._url_backup = self.data["url"]
            del self.data["url"]
        self._file_to_upload = file_to_upload
        file_format = None
        if guess_format_from_suffix:
            file_format = self.set_format(Path(file_to_upload).suffix)
        return file_format

    def check_both_url_filetoupload(self) -> None:
        """Check for error where both url or file to upload are provided for resource.

        Returns:
            None
        """
        if self._file_to_upload is not None and "url" in self.data:
            raise HDXError(
                "Either a url or a file to upload must be supplied not both!"
            )

    def check_neither_url_filetoupload(self) -> None:
        if self._file_to_upload is None and "url" not in self.data:
            raise HDXError("Either a url or a file to upload must be supplied!")

    def correct_format(self, data: dict | None = None) -> None:
        """Correct the format of the file

        Args:
            data: Resource data.

        Returns:
            None
        """
        file_format = data.get("format")
        if file_format is None:
            return
        file_format = self.get_mapped_format(
            file_format, configuration=self.configuration
        )
        if not file_format:
            raise HDXError(
                f"Supplied file format {file_format} is invalid and could not be mapped to a known type!"
            )
        data["format"] = file_format

    def set_types(self) -> None:
        """Add resource_type and url_type if not supplied based on url or file to
        upload.

        Returns:
            None
        """
        if self._file_to_upload is None:
            if "url" in self.data:
                self.data["resource_type"] = "api"
                self.data["url_type"] = "api"
        else:
            self.data["resource_type"] = "file.upload"
            self.data["url_type"] = "upload"
            if "tracking_summary" in self.data:
                del self.data["tracking_summary"]

    def check_required_fields(self, ignore_fields: Sequence[str] = ()) -> None:
        """Check that metadata for resource is complete. The parameter ignore_fields
        should be set if required to any fields that should be ignored for the
        particular operation.

        Args:
            ignore_fields: Fields to ignore. Default is ().

        Returns:
            None
        """
        self._check_required_fields("resource", ignore_fields)
        self.check_neither_url_filetoupload()

    def _resource_merge_hdx_update(
        self,
        **kwargs: Any,
    ) -> int:
        """Helper method to update last_modified for external resources if
        data_updated is True, then check if HDX object exists and update it.
        Returns status code where:
        0 = no file to upload and last_modified set to now
        (data_updated flag is True),
        1 = no file to upload and data_updated flag is False,
        2 = file uploaded to filestore (either hash or size of file has changed),
        3 = file not uploaded to filestore (hash and size of file are the same),
        4 = file not uploaded (hash, size unchanged), last_modified field ignored

        Args:
            **kwargs: See below
            operation (str): Operation to perform eg. patch. Defaults to update.
            data_updated (bool): If True, set last_modified to now. Defaults to False.
            force_update (bool): Force file to be updated even if it hasn't changed. Defaults to False.

        Returns:
            Status code
        """
        data_updated = kwargs.pop("data_updated", self._data_updated)
        files = {}
        if self._file_to_upload:
            force_update = kwargs.pop("force_update", False)
            file_format = self._old_data.get("format", "").lower()
            size, hash = get_size_and_hash(self._file_to_upload, file_format)
            if not force_update and hash == self.data.get("hash"):
                logger.warning(
                    f"Not updating filestore for resource {self.data['name']} as hash unchanged!"
                )
                if self._url_backup:
                    self._old_data["url"] = self._url_backup
                # ensure last_modified is not updated if file hasn't changed
                if "last_modified" in self.data:
                    del self.data["last_modified"]
                    status = 4
                else:
                    status = 3
            else:
                # update file if size or hash has changed
                files["upload"] = self._file_to_upload
                self._old_data["resource_type"] = "file.upload"
                self._old_data["url_type"] = "upload"
                if "tracking_summary" in self._old_data:
                    del self._old_data["tracking_summary"]
                self._old_data["size"] = size
                self._old_data["hash"] = hash
                status = 2
            self._url_backup = None
        else:
            if "url" in self._old_data and self._old_data.get("url_type") != "upload":
                self._old_data["resource_type"] = "api"
                self._old_data["url_type"] = "api"
            if data_updated:
                # Should not output timezone info here
                self._old_data["last_modified"] = now_utc_notz().isoformat(
                    timespec="microseconds"
                )
                self._data_updated = False
                status = 0
            else:
                status = 1

        self.correct_format(self._old_data)
        # old_data will be merged into data in the next step
        self._merge_hdx_update("resource", "id", files, True, **kwargs)
        return status

    def _get_resource_id(self, **kwargs: Any) -> str | None:
        """Helper function to get resource id if available from given resource or by
        comparing ot a given dataset's resources.

        Args:
            **kwargs: See below
            dataset (Dataset): Existing dataset if available to obtain resource id

        Returns:
            Resource id or None
        """
        loadedid = self.data.get("id")
        if loadedid is None:
            dataset = kwargs.get("dataset")
            if dataset:
                dataset_id = dataset.get("id")
                if not dataset_id:
                    dataset_name = dataset.get("name")
                    if dataset_name:
                        dataset = hdx.data.dataset.Dataset.read_from_hdx(dataset_name)
                        if dataset:
                            dataset_id = dataset["id"]
                    else:
                        raise HDXError("No dataset id or name in dataset!")
                if dataset_id:
                    package_id = self.data.get("package_id")
                    if package_id and package_id != dataset_id:
                        logger.warning(
                            f"Using dataset id {dataset_id} from dataset parameter which doesn't match {package_id} in resource!"
                        )
            else:
                dataset_id = self.data.get("package_id")
                if dataset_id:
                    dataset = hdx.data.dataset.Dataset.read_from_hdx(dataset_id)
                    if not dataset:
                        dataset_id = None
            if dataset_id:
                self.data["package_id"] = dataset["id"]
                dataset_resources = dataset.get_resources()
                matching_index = (
                    hdx.data.resource_matcher.ResourceMatcher.match_resource_list(
                        dataset_resources, self
                    )
                )
                if matching_index:
                    matching_resource = dataset_resources[matching_index]
                    loadedid = matching_resource.get("id")
                    if loadedid:
                        self.data["id"] = loadedid
        return loadedid

    def update_in_hdx(self, **kwargs: Any) -> int:
        """Check if resource exists in HDX and if so, update it. To indicate
        that the data in an external resource (given by a URL) has been
        updated, set data_updated to True, which will result in the resource
        last_modified field being set to now. If the method set_file_to_upload
        is used to supply a file, the resource last_modified field is set to
        now automatically regardless of the value of data_updated.

        Returns status code where:
        0 = no file to upload and last_modified set to now
        (data_updated flag is True),
        1 = no file to upload and data_updated flag is False,
        2 = file uploaded to filestore (either hash or size of file has changed),
        3 = file not uploaded to filestore (hash and size of file are the same),
        4 = file not uploaded (hash, size unchanged), given last_modified ignored

        Args:
            **kwargs: See below
            operation (string): Operation to perform eg. patch. Defaults to update.
            data_updated (bool): If True, set last_modified to now. Defaults to False.
            date_data_updated (datetime): Date to use for last_modified. Default to None.
            force_update (bool): Force file to be updated even if it hasn't changed. Defaults to False.
            dataset (Dataset): Existing dataset if available to obtain resource id

        Returns:
            Status code
        """
        self.check_both_url_filetoupload()
        _ = self._get_resource_id(**kwargs)
        self._check_load_existing_object("resource", "id")
        return self._resource_merge_hdx_update(**kwargs)

    def create_in_hdx(self, **kwargs: Any) -> int:
        """Check if resource exists in HDX and if so, update it, otherwise
        create it. To indicate that the data in an external resource (given by
        a URL) has been updated, set data_updated to True, which will result in
        the resource last_modified field being set to now. If the method
        set_file_to_upload is used to supply a file, the resource last_modified
        field is set to now automatically regardless of the value of
        data_updated.

        Returns status code where:
        0 = no file to upload and last_modified set to now
        (resource creation or data_updated flag is True),
        1 = no file to upload and data_updated flag is False,
        2 = file uploaded to filestore (resource creation or either hash or size of file
        has changed),
        3 = file not uploaded to filestore (hash and size of file are the same),
        4 = file not uploaded (hash, size unchanged), given last_modified ignored

        Args:
            **kwargs: See below
            data_updated (bool): If True, set last_modified to now. Defaults to False.
            date_data_updated (datetime): Date to use for last_modified. Default to None.
            force_update (bool): Force file to be updated even if it hasn't changed. Defaults to False.
            dataset (Dataset): Existing dataset if available to obtain resource id

        Returns:
            Status code
        """
        self.check_both_url_filetoupload()
        loadedid = self._get_resource_id(**kwargs)
        if loadedid:
            if self._load_from_hdx("resource", loadedid):
                logger.warning(f"{'resource'} exists. Updating {loadedid}")
                return self._resource_merge_hdx_update(**kwargs)
            logger.warning(f"Failed to load resource with id {loadedid}")

        self.set_types()
        self.correct_format(self.data)
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            self.check_required_fields()
        files = {}
        if self._file_to_upload:
            files["upload"] = self._file_to_upload
            self.data["size"], self.data["hash"] = get_size_and_hash(
                self._file_to_upload, self.get_format()
            )
            status = 2
        else:
            status = 0
        self._save_to_hdx("create", "name", files, True)
        return status

    def delete_from_hdx(self) -> None:
        """Deletes a resource from HDX

        Returns:
            None
        """
        self._delete_from_hdx("resource", "id")

    def get_dataset(self) -> "Dataset":  # noqa: F821
        """Return dataset containing this resource

        Returns:
            Dataset containing this resource
        """
        package_id = self.data.get("package_id")
        if package_id is None:
            raise HDXError("Resource has no package id!")
        return hdx.data.dataset.Dataset.read_from_hdx(package_id)

    @staticmethod
    def search_in_hdx(
        query: str,
        configuration: Configuration | None = None,
        **kwargs: Any,
    ) -> list["Resource"]:
        """Searches for resources in HDX. NOTE: Does not search dataset metadata!

        Args:
            query: Query
            configuration: HDX configuration. Defaults to global configuration.
            **kwargs: See below
            order_by (str): A field on the Resource model that orders the results
            offset (int): Apply an offset to the query
            limit (int): Apply a limit to the query
        Returns:
            List of resources resulting from query
        """

        resources = []
        resource = Resource(configuration=configuration)
        _success, result = resource._read_from_hdx(
            "resource", query, "query", Resource.actions()["search"]
        )
        if result:
            count = result.get("count", None)
            if count:
                for resourcedict in result["results"]:
                    resource = Resource(resourcedict, configuration=configuration)
                    resources.append(resource)
        else:
            logger.debug(result)
        return resources

    @staticmethod
    def _get_safe_filename(filename: str) -> str:
        """Strip any directory components (/ or \\) from a filename built from
        resource metadata so downloads cannot be written outside the folder

        Args:
            filename: Filename to make safe

        Returns:
            Filename without directory components
        """
        safe_filename = PureWindowsPath(filename).name
        if safe_filename in ("", ".", ".."):
            raise HDXError(f"Invalid filename {filename} for download!")
        return safe_filename

    def _download_file(
        self,
        url: str,
        folder: Path | str | None,
        filename: str,
        retriever: Retrieve | None,
    ) -> Path:
        """Helper method to download a file from HDX

        Args:
            url: URL to download
            folder: Folder to download file to. None means temporary folder.
            filename: Filename to use
            retriever: Retrieve object to use. None means do not use one.

        Returns:
            Path to downloaded file
        """
        # Only send the HDX API key to the HDX site, not to external resource URLs
        hdx_site_url = self.configuration.get_hdx_site_url()
        if urlsplit(url)[:2] == urlsplit(hdx_site_url)[:2]:
            session = hdx.api.utilities.url_utils.get_ckan_ready_session(
                self.configuration
            )
        else:
            session = get_session(
                full_agent=self.configuration.get_user_agent(), use_env=False
            )
        filename = self._get_safe_filename(filename)
        with Download(session=session) as downloader:
            if retriever:
                downloader = retriever.clone(downloader)
            return downloader.download_file(url, folder=folder, filename=filename)

    def download(
        self, folder: Path | str | None = None, retriever: Retrieve | None = None
    ) -> tuple[str, Path]:
        """Download resource store to provided folder or temporary folder if no folder
        supplied

        Args:
            folder: Folder to download resource to. Defaults to None.
            retriever: Retrieve object to use. Defaults to None.

        Returns:
            (URL downloaded, Path to downloaded file)

        """
        # Download the resource
        url = self.data.get("url", None)
        if not url:
            raise HDXError("No URL to download!")
        logger.debug(f"Downloading {url}")
        filename = self.data["name"]
        file_format = f".{self.get_format()}"
        if not filename.endswith(file_format):
            filename = f"{filename}{file_format}"
        return url, self._download_file(url, folder, filename, retriever)

    def get_datastore_dump_url(self, file_format: str = "csv") -> str:
        """Get the URL from which the resource datastore can be downloaded in
        full.

        Args:
            file_format: Dump format (csv, tsv, json or xml). Defaults to csv.

        Returns:
            Datastore dump URL
        """
        file_format = file_format.lower()
        if file_format not in self._valid_datastore_dump_formats:
            valid_formats = ", ".join(sorted(self._valid_datastore_dump_formats))
            raise HDXError(
                f"Invalid datastore dump format '{file_format}'! Must be one of: {valid_formats}"
            )
        url = (
            f"{self.configuration.get_hdx_site_url()}/datastore/dump/{self.data['id']}"
        )
        if file_format != "csv":
            url = f"{url}?format={file_format}"
        return url

    def download_datastore(
        self,
        folder: Path | str | None = None,
        filename: str | None = None,
        file_format: str = "csv",
        retriever: Retrieve | None = None,
    ) -> tuple[str, Path]:
        """Download the resource datastore in full to provided folder or
        temporary folder if no folder supplied

        Args:
            folder: Folder to download datastore to. Defaults to None.
            filename: Filename to use. Defaults to None (resource name with file_format extension).
            file_format: Dump format (csv, tsv, json or xml). Defaults to csv.
            retriever: Retrieve object to use. Defaults to None.

        Returns:
            (URL downloaded, Path to downloaded file)
        """
        file_format = file_format.lower()
        url = self.get_datastore_dump_url(file_format)
        logger.debug(f"Downloading {url}")
        if filename is None:
            filename = self.data["name"]
            extension = f".{file_format}"
            if not filename.endswith(extension):
                filename = f"{filename}{extension}"
        return url, self._download_file(url, folder, filename, retriever)

    @staticmethod
    def get_all_resource_ids_in_datastore(
        configuration: Configuration | None = None,
    ) -> list[str]:
        """Get list of resources that have a datastore returning their ids.

        Args:
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            List of resource ids that are in the datastore
        """
        resource = Resource(configuration=configuration)
        success, result = resource._read_from_hdx(
            "datastore",
            "_table_metadata",
            "resource_id",
            Resource.actions()["datastore_search"],
            limit=10000,
        )
        resource_ids = []
        if not success:
            logger.debug(result)
        else:
            for record in result["records"]:
                resource_ids.append(record["name"])
        return resource_ids

    def has_datastore(self) -> bool:
        """Check if the resource has a datastore.

        Returns:
            Whether the resource has a datastore or not
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

    def create_datastore(
        self,
        schema: Sequence[dict],
        primary_key: str | Sequence[str] | None = None,
    ) -> None:
        """Create a datastore for the resource with the given schema.

        Args:
            schema: Sequence of field definitions, each a dict with 'id' and 'type' keys.
            primary_key: Primary key field name(s). Defaults to None.

        Returns:
            None
        """
        data: dict = {
            "resource_id": self.data["id"],
            "force": True,
            "fields": schema,
        }
        if primary_key is not None:
            if not isinstance(primary_key, str):
                primary_key = ",".join(primary_key)
            data["primary_key"] = primary_key
        self._write_to_hdx("datastore_create", data, "resource_id")

    def update_datastore(
        self,
        records: Sequence[dict],
        method: str = "upsert",
    ) -> None:
        """Update (upsert) records into the resource datastore.

        Args:
            records: Sequence of record dicts to insert or update.
            method: Datastore update method ('upsert', 'insert', or 'update'). Defaults to 'upsert'.

        Returns:
            None
        """
        data = {
            "resource_id": self.data["id"],
            "force": True,
            "method": method,
            "records": records,
        }
        self._write_to_hdx("datastore_upsert", data, "resource_id")

    def search_datastore(
        self,
        filters: dict | None = None,
        q: str | dict | None = None,
        fields: Sequence[str] | None = None,
        sort: str | None = None,
        distinct: bool = False,
        limit: int = 100,
        offset: int = 0,
        include_total: bool = True,
        **kwargs: Any,
    ) -> dict:
        """Search the resource datastore using CKAN datastore_search.

        Args:
            filters: Dictionary of field name to value(s) to match. Defaults to None.
            q: Full text query string or dictionary of field name to query. Defaults to None.
            fields: Fields to return. Defaults to None (all fields).
            sort: Comma separated field names with optional asc/desc eg. "code desc". Defaults to None.
            distinct: Whether to return only distinct rows. Defaults to False.
            limit: Maximum number of rows to return. Defaults to 100.
            offset: Number of rows to skip. Defaults to 0.
            include_total: Whether to include total number of matching rows. Defaults to True.
            **kwargs: Other parameters to pass to datastore_search eg. plain, language.

        Returns:
            datastore_search result dictionary with keys including records, fields and total
        """
        data: dict = {
            "resource_id": self.data["id"],
            "limit": limit,
            "offset": offset,
            "include_total": include_total,
        }
        if filters is not None:
            data["filters"] = filters
        if q is not None:
            data["q"] = q
        if fields is not None:
            data["fields"] = ",".join(fields)
        if sort is not None:
            data["sort"] = sort
        if distinct:
            data["distinct"] = True
        data.update(kwargs)
        return self._write_to_hdx("datastore_search", data, "resource_id")

    def get_datastore_records(
        self,
        filters: dict | None = None,
        q: str | dict | None = None,
        fields: Sequence[str] | None = None,
        sort: str | None = None,
        distinct: bool = False,
        page_size: int = 10000,
        **kwargs: Any,
    ) -> Iterator[dict]:
        """Iterate over all matching records in the resource datastore, paging
        through datastore_search results.

        Args:
            filters: Dictionary of field name to value(s) to match. Defaults to None.
            q: Full text query string or dictionary of field name to query. Defaults to None.
            fields: Fields to return. Defaults to None (all fields).
            sort: Comma separated field names with optional asc/desc eg. "code desc". Defaults to None.
            distinct: Whether to return only distinct rows. Defaults to False.
            page_size: Number of rows to request per call. Defaults to 10000.
            **kwargs: Other parameters to pass to datastore_search eg. plain, language. records_format csv and tsv are not supported.

        Returns:
            Iterator of record dictionaries
        """
        if page_size < 1:
            raise HDXError("page_size must be at least 1!")
        records_format = kwargs.get("records_format")
        if records_format in ("csv", "tsv"):
            raise HDXError(
                f"records_format {records_format} is not supported! Use search_datastore instead."
            )
        offset = 0
        while True:
            result = self.search_datastore(
                filters=filters,
                q=q,
                fields=fields,
                sort=sort,
                distinct=distinct,
                limit=page_size,
                offset=offset,
                include_total=False,
                **kwargs,
            )
            records = result["records"]
            yield from records
            # HDX caps limit at its maximum rows per call and returns the limit applied
            limit = result["limit"]
            if len(records) < limit:
                return
            offset += limit

    def get_datastore_fields(self) -> list[dict]:
        """Get the field definitions of the resource datastore excluding the
        internal _id field.

        Returns:
            List of field dictionaries each with id and type keys
        """
        result = self.search_datastore(limit=0, include_total=False)
        return [field for field in result["fields"] if field["id"] != "_id"]

    @staticmethod
    def search_datastore_sql(
        sql: str,
        configuration: Configuration | None = None,
    ) -> list[dict]:
        """Run a read-only SQL query against the HDX datastore using CKAN
        datastore_search_sql. Tables are named by resource id, which must be
        double quoted eg. SELECT * FROM "RESOURCE_ID". Logs a warning if HDX
        truncates the results.

        Args:
            sql: SQL SELECT statement
            configuration: HDX configuration. Defaults to global configuration.

        Returns:
            List of record dictionaries
        """
        resource = Resource(configuration=configuration)
        result = resource._write_to_hdx("datastore_search_sql", {"sql": sql})
        records = result["records"]
        if result.get("records_truncated"):
            logger.warning(
                f"SQL query results truncated by HDX to {len(records)} rows!"
            )
        return records

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

    def submit_to_datapusher(self, ignore_hash: bool = False) -> bool:
        """Submit the resource to DataPusher+ to load its file into the
        datastore. This does not change last_modified. Unlike the automatic
        submission on file upload, HDX's datastore allowlist and format checks
        are not applied.

        Args:
            ignore_hash: Reload even if the file hash is unchanged. Defaults to False.

        Returns:
            True if submitted, False if not (eg. a job is already pending)
        """
        return self._write_to_hdx(
            "datapusher_submit",
            {"resource_id": self.data["id"], "ignore_hash": ignore_hash},
            "resource_id",
        )

    def push_to_datastore(self) -> bool:
        """Submit the resource to DataPusher+ using HDX's wrapper action.
        Unlike submit_to_datapusher, this requires sysadmin.

        Returns:
            True if submitted, False if not
        """
        result = self._write_to_hdx(
            "push_to_datastore", {"resource_id": self.data["id"]}, "resource_id"
        )
        if not result["success"]:
            logger.debug(result["message"])
        return result["success"]

    def get_datapusher_status(self) -> dict | None:
        """Get the status of the resource's latest DataPusher+ job

        Returns:
            Status dictionary with keys including status and error or None if no job
        """
        success, result = self._read_from_hdx(
            "datapusher",
            self.data["id"],
            "resource_id",
            self.actions()["datapusher_status"],
        )
        if success:
            return result
        logger.debug(result)
        return None

    def get_resource_views(self) -> list[ResourceView]:
        """Get any resource views in the resource

        Returns:
            List of resource views
        """
        return ResourceView.get_all_for_resource(self.data["id"])

    def _get_resource_view(self, resource_view: ResourceView | dict) -> ResourceView:
        """Get resource view id

        Args:
            resource_view: ResourceView metadata from a ResourceView object or dictionary

        Returns:
            ResourceView object
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

    def add_update_resource_view(self, resource_view: ResourceView | dict) -> None:
        """Add new resource view in resource with new metadata

        Args:
            resource_view: Resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None

        """
        resource_view = self._get_resource_view(resource_view)
        resource_view.create_in_hdx()

    def add_update_resource_views(
        self, resource_views: Sequence[ResourceView | dict]
    ) -> None:
        """Add new or update existing resource views in resource with new metadata.

        Args:
            resource_views: A list of resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError("ResourceViews should be a list!")
        for resource_view in resource_views:
            self.add_update_resource_view(resource_view)

    def reorder_resource_views(
        self, resource_views: Sequence[ResourceView | dict | str]
    ) -> None:
        """Order resource views in resource.

        Args:
            resource_views: A list of either resource view ids or resource views metadata from ResourceView objects or dictionaries

        Returns:
            None
        """
        if not isinstance(resource_views, list):
            raise HDXError("ResourceViews should be a list!")
        ids = []
        for resource_view in resource_views:
            if isinstance(resource_view, str):
                resource_view_id = resource_view
            else:
                resource_view_id = resource_view["id"]
            if is_valid_uuid(resource_view_id) is False:
                raise HDXError(f"{resource_view} is not a valid resource view id!")
            ids.append(resource_view_id)
        self._read_from_hdx(
            "resource view",
            self.data["id"],
            "id",
            ResourceView.actions()["reorder"],
            order=ids,
        )

    def delete_resource_view(self, resource_view: ResourceView | dict | str) -> None:
        """Delete a resource view from the resource and HDX

        Args:
            resource_view: Either a resource view id or resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None
        """
        if isinstance(resource_view, str):
            if is_valid_uuid(resource_view) is False:
                raise HDXError(f"{resource_view} is not a valid resource view id!")
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

    def is_broken(self) -> bool:
        """Return if resource is broken

        Returns:
            Whether resource is broken
        """
        return self.data.get("broken_link", False)

    def mark_broken(self) -> None:
        """Mark resource as broken

        Returns:
            None
        """
        success, result = self._read_from_hdx(
            "resource",
            self.data["id"],
            action=self.actions()["broken"],
            batch_mode="KEEP_OLD",
            skip_validation=True,
        )
        if success:
            self.data = result
        else:
            logger.debug(result)

    def get_fs_check_info(self) -> list[dict]:
        """Get file structure check information for the resource from its
        fs_check_info field. Each check event can have keys such as state,
        timestamp, sheet_changes and hxl_proxy_response.

        Returns:
            List of file structure check events
        """
        fs_check_info = self.data.get("fs_check_info")
        if not fs_check_info:
            return []
        if isinstance(fs_check_info, str):
            return json.loads(fs_check_info)
        return fs_check_info

    def _set_from_package(self, package: dict) -> None:
        """Set the resource's metadata from the matching resource in a package
        metadata dictionary returned by HDX

        Args:
            package: Package metadata dictionary

        Returns:
            None
        """
        for resource in package.get("resources", []):
            if resource["id"] == self.data["id"]:
                self.data = resource
                return

    def revise_fs_check(self, key: str, value: Any) -> None:
        """Set a file structure check field of the resource in HDX. If key is
        fs_check_info and value is a dictionary, it is appended to the
        existing check events (the last 20 are kept) rather than replacing
        them. Requires sysadmin or permission to manage QA.

        Args:
            key: Resource field eg. fs_check_info
            value: Value to set

        Returns:
            None
        """
        result = self._write_to_hdx(
            "fs_check_revise",
            {
                "package_id": self.data["package_id"],
                "id": self.data["id"],
                "key": key,
                "value": value,
            },
            "id",
        )
        self._set_from_package(result["package"])

    def reset_fs_check(self, key: str = "fs_check_info") -> None:
        """Clear a file structure check field of the resource in HDX. Requires
        sysadmin or permission to manage QA.

        Args:
            key: Resource field to clear. Defaults to fs_check_info.

        Returns:
            None
        """
        result = self._write_to_hdx(
            "fs_check_reset",
            {"package_id": self.data["package_id"], "id": self.data["id"], "key": key},
            "id",
        )
        self._set_from_package(result["package"])

    def is_p_coded(self) -> bool | None:
        """Return if the resource is marked as containing p-codes

        Returns:
            Whether resource is p-coded or None if not set
        """
        p_coded = self.data.get("p_coded")
        if p_coded is None:
            return None
        if isinstance(p_coded, str):
            return p_coded.lower() == "true"
        return p_coded

    def set_p_coded(self, p_coded: bool) -> None:
        """Mark the resource in HDX as containing p-codes or not. Requires
        sysadmin or permission to manage p-codes.

        Args:
            p_coded: Whether resource is p-coded

        Returns:
            None
        """
        self.data = self._write_to_hdx(
            "p_coded", {"id": self.data["id"], "p_coded": p_coded}, "id"
        )

    def is_in_hapi(self) -> bool:
        """Return if the resource is marked as being in HAPI

        Returns:
            Whether resource is in HAPI
        """
        return self.data.get("in_hapi") == "yes"

    def set_in_hapi(self, in_hapi: bool) -> None:
        """Mark the resource in HDX as being in HAPI or not. Requires sysadmin
        or permission to manage the in HAPI flag.

        Args:
            in_hapi: Whether resource is in HAPI

        Returns:
            None
        """
        self.data = self._write_to_hdx(
            "in_hapi",
            {"id": self.data["id"], "in_hapi": "yes" if in_hapi else "no-data"},
            "id",
        )

    def is_in_quarantine(self) -> bool:
        """Return if the resource is in quarantine

        Returns:
            Whether resource is in quarantine
        """
        in_quarantine = self.data.get("in_quarantine", False)
        if isinstance(in_quarantine, str):
            return in_quarantine.lower() == "true"
        return in_quarantine

    def set_in_quarantine(self, in_quarantine: bool) -> None:
        """Put the resource in HDX in quarantine or take it out. Quarantining
        also removes the resource's shape_info and geopreview. Requires
        sysadmin or permission to manage QA.

        Args:
            in_quarantine: Whether resource is in quarantine

        Returns:
            None
        """
        self.data = self._write_to_hdx(
            "in_quarantine",
            {
                "id": self.data["id"],
                "in_quarantine": "true" if in_quarantine else "false",
            },
            "id",
        )

    def qa_patch(self, **kwargs: Any) -> None:
        """Patch QA fields of the resource in HDX eg. pii, sdc_report_flag,
        broken_link. Requires sysadmin or permission to manage QA.

        Args:
            **kwargs: Resource fields to patch

        Returns:
            None
        """
        self.data = self._write_to_hdx(
            "qa_patch", {"id": self.data["id"], **kwargs}, "id"
        )

    def is_marked_data_updated(self) -> bool:
        """Return if the resource's data is marked to be updated

        Returns:
            Whether resource's data is marked to be updated
        """
        return self._data_updated

    def mark_data_updated(self) -> None:
        """Mark resource data as updated

        Returns:
            None
        """
        self._data_updated = True

    def get_date_data_updated(self) -> datetime:
        """Get date resource data was updated

        Returns:
            Date resource data was updated
        """
        return parse_date(self.data["last_modified"], include_microseconds=True)

    def set_date_data_updated(
        self, date: datetime | str, ignore_timeinfo: bool = False
    ) -> None:
        """Set date resource data was updated

        Args:
            date: Date resource data was updated
            ignore_timeinfo: Ignore time and time zone of date. Defaults to False.

        Returns:
            None
        """
        self.data["last_modified"] = DateHelper.get_hdx_date(
            date, ignore_timeinfo=ignore_timeinfo, include_microseconds=True
        )

    def get_hdx_url(self) -> str | None:
        """Get the url of the resource on HDX

        Returns:
            Url of the resource on HDX or None if the resource is missing the id field
        """
        id = self.data.get("id")
        if not id:
            return None
        dataset_id = self.data.get("package_id")
        if not dataset_id:
            return None
        return f"{self.configuration.get_hdx_site_url()}/dataset/{dataset_id}/resource/{id}"

    def get_api_url(self) -> str | None:
        """Get the API url of the resource on HDX

        Returns:
            API url of the resource on HDX or None if the resource is missing the id field
        """
        id = self.data.get("id")
        if not id:
            return None
        return f"{self.configuration.get_hdx_site_url()}/api/3/action/resource_show?id={id}"
