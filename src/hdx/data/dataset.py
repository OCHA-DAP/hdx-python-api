"""Dataset class containing all logic for creating, checking, and updating datasets and associated resources."""

import json
import logging
import sys
import warnings
from copy import deepcopy
from datetime import datetime
from os.path import isfile, join
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from hxl.input import HXLIOException, InputOptions, munge_url

import hdx.data.organization as org_module
import hdx.data.resource as res_module
import hdx.data.resource_view as resource_view
import hdx.data.showcase as sc_module
import hdx.data.user as user
import hdx.data.vocabulary as vocabulary
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.api.utilities.dataset_title_helper import DatasetTitleHelper
from hdx.api.utilities.date_helper import DateHelper
from hdx.api.utilities.filestore_helper import FilestoreHelper
from hdx.data.hdxobject import HDXError, HDXObject
from hdx.data.resource_matcher import ResourceMatcher
from hdx.location.country import Country
from hdx.utilities.base_downloader import BaseDownload
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    now_utc,
    now_utc_notz,
    parse_date,
    parse_date_range,
)
from hdx.utilities.dictandlist import merge_two_dictionaries, write_list_to_csv
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_json
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.saver import save_json
from hdx.utilities.typehint import ListTuple, ListTupleDict
from hdx.utilities.uuid import is_valid_uuid

if TYPE_CHECKING:
    from hdx.data.organization import Organization
    from hdx.data.resource import Resource
    from hdx.data.showcase import Showcase
    from hdx.data.user import User

logger = logging.getLogger(__name__)


class NotRequestableError(HDXError):
    pass


class Dataset(HDXObject):
    """Dataset class enabling operations on datasets and associated resources.

    Args:
        initial_data (Optional[Dict]): Initial dataset metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    max_attempts = 5
    max_int = sys.maxsize
    update_frequencies = {
        "-2": "As needed",
        "-1": "Never",
        "0": "Live",
        "1": "Every day",
        "2": "Every two days",
        "7": "Every week",
        "14": "Every two weeks",
        "30": "Every month",
        "60": "Every two months",
        "90": "Every three months",
        "120": "Every four months",
        "180": "Every six months",
        "300": "Every ten months",
        "365": "Every year",
        "730": "Every two years",
        "as needed": "-2",
        "adhoc": "-2",
        "never": "-1",
        "live": "0",
        "every day": "1",
        "every two days": "2",
        "every 2 days": "2",
        "every week": "7",
        "every two weeks": "14",
        "every month": "30",
        "every 2 months": "60",
        "every two months": "60",
        "every 3 months": "90",
        "every three months": "90",
        "every quarter": "90",
        "every four months": "120",
        "every 4 months": "120",
        "every six months": "180",
        "every 6 months": "180",
        "every ten months": "300",
        "every 10 months": "300",
        "every year": "365",
        "every two years": "730",
        "every 2 years": "730",
        "daily": "1",
        "weekly": "7",
        "fortnightly": "14",
        "every other week": "14",
        "monthly": "30",
        "quarterly": "90",
        "semiannually": "180",
        "semiyearly": "180",
        "annually": "365",
        "yearly": "365",
    }

    def __init__(
        self,
        initial_data: Optional[Dict] = None,
        configuration: Optional[Configuration] = None,
    ) -> None:
        if not initial_data:
            initial_data = {}
        self.init_resources()
        super().__init__(initial_data, configuration=configuration)
        self.preview_resourceview = None

    @staticmethod
    def actions() -> Dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            "show": "package_show",
            "update": "package_update",
            "create": "package_create",
            "patch": "package_patch",
            "revise": "package_revise",
            "delete": "hdx_dataset_purge",
            "search": "package_search",
            "reorder": "package_resource_reorder",
            "list": "package_list",
            "autocomplete": "package_autocomplete",
            "hxl": "package_hxl_update",
            "create_default_views": "package_create_default_resource_views",
        }

    def __setitem__(self, key: Any, value: Any) -> None:
        """Set dictionary items but do not allow setting of resources

        Args:
            key (Any): Key in dictionary
            value (Any): Value to put in dictionary

        Returns:
            None
        """
        if key == "resources":
            self.add_update_resources(value, ignore_datasetid=True)
            return
        super().__setitem__(key, value)

    def separate_resources(self) -> None:
        """Move contents of resources key in internal dictionary into self.resources

        Returns:
            None
        """
        self._separate_hdxobjects(
            self.resources, "resources", "name", res_module.Resource
        )

    def unseparate_resources(self) -> None:
        """Move self.resources into resources key in internal dictionary

        Returns:
            None
        """
        if self.resources:
            self.data["resources"] = self._convert_hdxobjects(self.resources)

    def get_dataset_dict(self) -> Dict:
        """Move self.resources into resources key in internal dictionary

        Returns:
            Dict: Dataset dictionary
        """
        package = deepcopy(self.data)
        if self.resources:
            package["resources"] = self._convert_hdxobjects(self.resources)
        return package

    def save_to_json(self, path: str, follow_urls: bool = False):
        """Save dataset to JSON. If follow_urls is True, resource urls that point to
        datasets, HXL proxy urls etc. are followed to retrieve final urls.

        Args:
            path (str): Path to save dataset
            follow_urls (bool): Whether to follow urls. Defaults to False.

        Returns:
            None
        """
        dataset_dict = self.get_dataset_dict()
        if follow_urls:
            for resource in dataset_dict.get("resources", tuple()):
                try:
                    resource["url"] = munge_url(resource["url"], InputOptions())
                except HXLIOException:
                    pass
        save_json(dataset_dict, path)

    @staticmethod
    def load_from_json(path: str) -> Optional["Dataset"]:
        """Load dataset from JSON

        Args:
            path (str): Path to load dataset

        Returns:
            Optional[Dataset]: Dataset created from JSON or None
        """
        jsonobj = load_json(path, loaderror_if_empty=False)
        if jsonobj is None:
            return None
        dataset = Dataset(jsonobj)
        dataset.separate_resources()
        return dataset

    def init_resources(self) -> None:
        """Initialise self.resources list

        Returns:
            None
        """
        self.resources: List[res_module.Resource] = []

    def _get_resource_from_obj(
        self, resource: Union["Resource", Dict, str]
    ) -> "Resource":
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Union[Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary

        Returns:
            Resource: Resource object
        """
        if isinstance(resource, str):
            if is_valid_uuid(resource) is False:
                raise HDXError(f"{resource} is not a valid resource id!")
            resource = res_module.Resource.read_from_hdx(
                resource, configuration=self.configuration
            )
        elif isinstance(resource, dict):
            resource = res_module.Resource(resource, configuration=self.configuration)
        if not isinstance(resource, res_module.Resource):
            raise HDXError(
                f"Type {type(resource).__name__} cannot be added as a resource!"
            )
        return resource

    def add_update_resource(
        self,
        resource: Union["Resource", Dict, str],
        ignore_datasetid: bool = False,
    ) -> None:
        """Add new or update existing resource in dataset with new metadata

        Args:
            resource (Union[Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary
            ignore_datasetid (bool): Whether to ignore dataset id in the resource

        Returns:
            None
        """
        resource = self._get_resource_from_obj(resource)
        if "package_id" in resource:
            if not ignore_datasetid:
                raise HDXError(
                    f"Resource {resource['name']} being added already has a dataset id!"
                )
        resource.check_url_filetoupload()
        resource_index = ResourceMatcher.match_resource_list(self.resources, resource)
        if resource_index is None:
            self.resources.append(resource)
        else:
            updated_resource = merge_two_dictionaries(
                self.resources[resource_index], resource
            )
            if resource.get_file_to_upload():
                updated_resource.set_file_to_upload(resource.get_file_to_upload())
            if resource.is_marked_data_updated():
                updated_resource.mark_data_updated()

    def add_update_resources(
        self,
        resources: ListTuple[Union["Resource", Dict, str]],
        ignore_datasetid: bool = False,
    ) -> None:
        """Add new to the dataset or update existing resources with new metadata

        Args:
            resources (ListTuple[Union[Resource,Dict,str]]): A list of either resource ids or resources metadata from either Resource objects or dictionaries
            ignore_datasetid (bool): Whether to ignore dataset id in the resource. Defaults to False.

        Returns:
            None
        """
        resource_objects = []
        for resource in resources:
            resource = self._get_resource_from_obj(resource)
            if "package_id" in resource:
                if not ignore_datasetid:
                    raise HDXError(
                        f"Resource {resource['name']} being added already has a dataset id!"
                    )
            resource_objects.append(resource)
        (
            resource_matches,
            updated_resource_matches,
            _,
            updated_resource_no_matches,
        ) = ResourceMatcher.match_resource_lists(self.resources, resource_objects)
        for i, resource_index in enumerate(resource_matches):
            resource = resource_objects[updated_resource_matches[i]]
            resource.check_url_filetoupload()
            updated_resource = merge_two_dictionaries(
                self.resources[resource_index], resource
            )
            if resource.get_file_to_upload():
                updated_resource.set_file_to_upload(resource.get_file_to_upload())
            if resource.is_marked_data_updated():
                updated_resource.mark_data_updated()
        for resource_index in updated_resource_no_matches:
            resource = resource_objects[resource_index]
            resource.check_url_filetoupload()
            self.resources.append(resource)

    def delete_resource(
        self,
        resource: Union["Resource", Dict, str],
        delete: bool = True,
    ) -> bool:
        """Delete a resource from the dataset and also from HDX by default

        Args:
            resource (Union[Resource,Dict,str]): Either resource id or resource metadata from a Resource object or a dictionary
            delete (bool): Whetehr to delete the resource from HDX (not just the dataset). Defaults to True.

        Returns:
            bool: True if resource removed or False if not
        """
        if isinstance(resource, str):
            if is_valid_uuid(resource) is False:
                raise HDXError(f"{resource} is not a valid resource id!")
        return self._remove_hdxobject(self.resources, resource, delete=delete)

    def get_resources(self) -> List["Resource"]:
        """Get dataset's resources

        Returns:
            List[Resource]: List of Resource objects
        """
        return self.resources

    def get_resource(self, index: int = 0) -> "Resource":
        """Get one resource from dataset by index

        Args:
            index (int): Index of resource in dataset. Defaults to 0.

        Returns:
            Resource: Resource object
        """
        return self.resources[index]

    def number_of_resources(self) -> int:
        """Get number of dataset's resources

        Returns:
            int: Number of Resource objects
        """
        return len(self.resources)

    def reorder_resources(
        self, resource_ids: ListTuple[str], hxl_update: bool = True
    ) -> None:
        """Reorder resources in dataset according to provided list. Resources are
        updated in the dataset object to match new order. However, the dataset is not
        refreshed by rereading from HDX. If only some resource ids are supplied then
        these are assumed to be first and the other resources will stay in their
        original order.

        Args:
            resource_ids (ListTuple[str]): List of resource ids
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.

        Returns:
            None
        """
        dataset_id = self.data.get("id")
        if not dataset_id:
            raise HDXError(
                "Dataset has no id! It must be read, created or updated first."
            )
        data = {"id": dataset_id, "order": resource_ids}
        results = self._write_to_hdx("reorder", data)
        ordered_ids = results["order"]
        reordered_resources = []
        for resource_id in ordered_ids:
            resource = next(x for x in self.resources if x["id"] == resource_id)
            reordered_resources.append(resource)
        self.resources = reordered_resources
        if hxl_update:
            self.hxl_update()

    def move_resource(
        self,
        resource_name: str,
        insert_before: str,
    ) -> "Resource":
        """Move resource in dataset to be before the resource whose name starts
        with the value of insert_before.

        Args:
            resource_name (str): Name of resource to move
            insert_before (str): Resource to insert before

        Returns:
            Resource: The resource that was moved
        """
        from_index = None
        to_index = None
        for i, resource in enumerate(self.resources):
            res_name = resource["name"]
            if res_name == resource_name:
                from_index = i
            elif res_name.startswith(insert_before):
                to_index = i
        if to_index is None:
            # insert at the start if resource cannot be found
            to_index = 0
        resource = self.resources.pop(from_index)
        if from_index < to_index:
            # to index was calculated while element was in front
            to_index -= 1
        self.resources.insert(to_index, resource)
        return resource

    def update_from_yaml(
        self, path: str = join("config", "hdx_dataset_static.yaml")
    ) -> None:
        """Update dataset metadata with static metadata from YAML file

        Args:
            path (str): Path to YAML dataset metadata. Defaults to config/hdx_dataset_static.yaml.

        Returns:
            None
        """
        super().update_from_yaml(path)
        self.separate_resources()

    def update_from_json(
        self, path: str = join("config", "hdx_dataset_static.json")
    ) -> None:
        """Update dataset metadata with static metadata from JSON file

        Args:
            path (str): Path to JSON dataset metadata. Defaults to config/hdx_dataset_static.json.

        Returns:
            None
        """
        super().update_from_json(path)
        self.separate_resources()

    @staticmethod
    def read_from_hdx(
        identifier: str, configuration: Optional[Configuration] = None
    ) -> Optional["Dataset"]:
        """Reads the dataset given by identifier from HDX and returns Dataset object

        Args:
            identifier (str): Identifier of dataset
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Dataset]: Dataset object if successful read, None if not
        """

        dataset = Dataset(configuration=configuration)
        result = dataset._dataset_load_from_hdx(identifier)
        if result:
            return dataset
        return None

    def _dataset_create_resources(self) -> None:
        """Creates resource objects in dataset"""

        if "resources" in self.data:
            self.old_data["resources"] = self._copy_hdxobjects(
                self.resources,
                res_module.Resource,
                ("file_to_upload", "data_updated"),
            )
            self.init_resources()
            self.separate_resources()

    def _dataset_load_from_hdx(self, id_or_name: str) -> bool:
        """Loads the dataset given by either id or name from HDX

        Args:
            id_or_name (str): Either id or name of dataset

        Returns:
            bool: True if loaded, False if not
        """

        if not self._load_from_hdx("dataset", id_or_name):
            return False
        self._dataset_create_resources()
        return True

    def check_required_fields(
        self,
        ignore_fields: ListTuple[str] = tuple(),
        allow_no_resources: bool = False,
        **kwargs: Any,
    ) -> None:
        """Check that metadata for dataset and its resources is complete. The parameter ignore_fields
        should be set if required to any fields that should be ignored for the particular operation.
        Prepend "resource:" for resource fields.

        Args:
            ignore_fields (ListTuple[str]): Fields to ignore. Default is tuple().
            allow_no_resources (bool): Whether to allow no resources. Defaults to False.

        Returns:
            None
        """
        dataset_ignore_fields = []
        for ignore_field in ignore_fields:
            if not ignore_field.startswith("resource:"):
                dataset_ignore_fields.append(ignore_field)
        if self.is_requestable():
            self._check_required_fields("dataset-requestable", dataset_ignore_fields)
        else:
            self._check_required_fields("dataset", dataset_ignore_fields)
            if len(self.resources) == 0 and not allow_no_resources:
                raise HDXError(
                    "There are no resources! Please add at least one resource!"
                )
            for resource in self.resources:
                FilestoreHelper.resource_check_required_fields(
                    resource, ignore_fields=ignore_fields
                )

    @staticmethod
    def revise(
        match: Dict[str, Any],
        filter: ListTuple[str] = tuple(),
        update: Dict[str, Any] = {},
        files_to_upload: Dict[str, str] = {},
        configuration: Optional[Configuration] = None,
        **kwargs: Any,
    ) -> "Dataset":
        """Revises an HDX dataset in HDX

        Args:
            match (Dict[str,Any]): Metadata on which to match dataset
            filter (ListTuple[str]): Filters to apply. Defaults to tuple().
            update (Dict[str,Any]): Metadata updates to apply. Defaults to {}.
            files_to_upload (Dict[str,str]): Files to upload to HDX. Defaults to {}.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: Additional arguments to pass to package_revise

        Returns:
            Dataset: Dataset object
        """
        separators = (",", ":")
        data = {"match": json.dumps(match, separators=separators)}
        if filter:
            data["filter"] = json.dumps(filter, separators=separators)
        if update:
            data["update"] = json.dumps(update, separators=separators)
        for key, value in kwargs.items():
            data[key] = json.dumps(value, separators=separators)
        dataset = Dataset(data, configuration=configuration)
        result = dataset._write_to_hdx(
            "revise",
            data,
            id_field_name="match",
            files_to_upload=files_to_upload,
        )
        dataset.data = result["package"]
        dataset.init_resources()
        dataset.separate_resources()
        return dataset

    def _prepare_hdx_call(self, data: Dict, kwargs: Any) -> None:
        """Common method used in create and update calls. Cleans tags and
        processes keyword arguments populating updated_by_script (details
        about what script is doing the update and when), batch
        and setting batch_mode in kwargs if needed (whether datasets on the
        CKAN /datasets page are grouped).

        Args:
            data (Dict): Dataset data to update if needed
            **kwargs: See below
            updated_by_script (str): Script info. Defaults to user agent.
            batch: Batch UUID for where multiple datasets are grouped into one batch
            batch_mode: Whether to group by batch. Defaults to not grouping.

        Returns:
            None
        """
        self.clean_tags()
        scriptinfo = kwargs.get("updated_by_script")
        if scriptinfo:
            del kwargs["updated_by_script"]
        else:
            scriptinfo = self.configuration.get_user_agent()
        # Should not output timezone info here
        data["updated_by_script"] = (
            f"{scriptinfo} ({now_utc_notz().isoformat(timespec='microseconds')})"
        )
        batch = kwargs.get("batch")
        if batch:
            if not is_valid_uuid(batch):
                raise HDXError(f"{batch} is not a valid UUID!")
            data["batch"] = batch
            del kwargs["batch"]
            if "batch_mode" not in kwargs:
                kwargs["batch_mode"] = "DONT_GROUP"

    def _revise_filter(
        self, dataset_data_to_update: Dict, keys_to_delete: ListTuple[str]
    ):
        """Returns the revise filter parameter adding to it any keys in the
        dataset metadata that are specified to be deleted. Also compare lists
        in original and updated metadata to see if any have had elements
        removed in which case these should be added to the filter

        Args:
            dataset_data_to_update (Dict): Dataset data to be updated
            keys_to_delete (ListTuple[str]): List of top level metadata keys to delete
        """
        revise_filter = []
        for key in keys_to_delete:
            revise_filter.append(f"-{key}")
        for key, value in dataset_data_to_update.items():
            if not isinstance(value, list):
                continue
            if key not in self.old_data:
                continue
            orig_list = self.old_data[key]
            elements_to_remove = []
            for i, orig_value in enumerate(orig_list):
                if isinstance(orig_value, dict) and any(
                    x in orig_value for x in ("id", "name")
                ):
                    # Where lists have an id or name, we use one of those to
                    # match elements and work out what needs to be deleted
                    # ie. any elements in the original list that don't exist
                    # in the updated list
                    el_id = orig_value.get("id")
                    el_name = orig_value.get("name")
                    present = False
                    for value_dict in value:
                        if el_id and "id" in value_dict and value_dict["id"] == el_id:
                            present = True
                            break
                        if (
                            el_name
                            and "name" in value_dict
                            and value_dict["name"] == el_name
                        ):
                            present = True
                            break
                    if not present:
                        elements_to_remove.append(i)
                elif orig_value not in value:
                    # Otherwise we do a simple exact match of list elements
                    # and delete any elements that are in the original list but
                    # not in the updated list
                    elements_to_remove.append(i)
            for element_index in reversed(elements_to_remove):
                del orig_list[element_index]
                if element_index == len(orig_list):
                    revise_filter.append(f"-{key}__{element_index}")
        return revise_filter

    def _revise_files_to_upload_resource_deletions(
        self,
        revise_filter: List[str],
        resources_to_update: ListTuple["Resource"],
        resources_to_delete: ListTuple[int],
        filestore_resources: Dict[int, str],
    ):
        """Returns the files to be uploaded and updates the revise filter with
        any resources to be deleted also updating filestore_resources to
        reflect any deletions.

        Args:
            revise_filter (List[str]): Keys and list elements to delete
            resources_to_update (ListTuple[Resource]): Resources to update
            resources_to_delete (ListTuple[int]): List of indexes of resources to delete
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
        """
        files_to_upload = {}
        if not self.is_requestable():
            for resource_index in resources_to_delete:
                del resources_to_update[resource_index]
                if resource_index == len(resources_to_update):
                    revise_filter.append(f"-resources__{resource_index}")
                new_fsresources = {}
                for index in filestore_resources:
                    if index > resource_index:
                        new_fsresources[index - 1] = filestore_resources[index]
                    else:
                        new_fsresources[index] = filestore_resources[index]
                filestore_resources = new_fsresources
            for (
                resource_index,
                file_to_upload,
            ) in filestore_resources.items():
                files_to_upload[f"update__resources__{resource_index}__upload"] = (
                    file_to_upload
                )
        return files_to_upload

    def _revise_dataset(
        self,
        keys_to_delete: ListTuple[str],
        resources_to_update: ListTuple["Resource"],
        resources_to_delete: ListTuple[int],
        filestore_resources: Dict[int, str],
        new_resource_order: Optional[ListTuple[str]],
        hxl_update: bool,
        create_default_views: bool = False,
        test: bool = False,
        **kwargs: Any,
    ) -> Dict:
        """Helper method to save the modified dataset and add any filestore resources

        Args:
            keys_to_delete (ListTuple[str]): List of top level metadata keys to delete
            resources_to_update (ListTuple[Resource]): Resources to update
            resources_to_delete (ListTuple[int]): List of indexes of resources to delete
            filestore_resources (Dict[int, str]): List of (index of resources, file to upload)
            new_resource_order (Optional[ListTuple[str]]): New resource order to use or None
            hxl_update (bool): Whether to call package_hxl_update.
            create_default_views (bool): Whether to create default views. Defaults to False.
            test (bool): Whether running in a test. Defaults to False.
            **kwargs: See below
            ignore_field (str): Any field to ignore when checking dataset metadata. Defaults to None.

        Returns:
            Dict: Dictionary of what gets passed to the revise call (for testing)
        """
        results = {}
        dataset_data_to_update = self.old_data
        self.old_data = self.data
        if (
            "batch_mode" in kwargs
        ):  # Whether or not CKAN should change groupings of datasets on /datasets page
            dataset_data_to_update["batch_mode"] = kwargs["batch_mode"]
        if (
            "skip_validation" in kwargs
        ):  # Whether or not CKAN should perform validation steps (checking fields present)
            dataset_data_to_update["skip_validation"] = kwargs["skip_validation"]
        dataset_data_to_update["state"] = "active"
        revise_filter = self._revise_filter(dataset_data_to_update, keys_to_delete)
        files_to_upload = self._revise_files_to_upload_resource_deletions(
            revise_filter,
            resources_to_update,
            resources_to_delete,
            filestore_resources,
        )
        dataset_data_to_update["resources"] = self._convert_hdxobjects(
            resources_to_update
        )
        results["filter"] = revise_filter
        results["update"] = dataset_data_to_update
        results["files_to_upload"] = files_to_upload
        if test:
            return results
        new_dataset = self.revise(
            {"id": self.data["id"]},
            filter=revise_filter,
            update=dataset_data_to_update,
            files_to_upload=files_to_upload,
        )
        self.data = new_dataset.data
        self.resources = new_dataset.resources

        # We do field check after call so that we have the changed data
        if "ignore_check" not in kwargs or not kwargs.get(
            "ignore_check"
        ):  # allow ignoring of field checks
            ignore_fields = kwargs.get("ignore_fields", list())
            ignore_field = self.configuration["dataset"].get("ignore_on_update")
            if ignore_field and ignore_field not in ignore_fields:
                ignore_fields.append(ignore_field)
            ignore_field = kwargs.get("ignore_field")
            if ignore_field and ignore_field not in ignore_fields:
                ignore_fields.append(ignore_field)
            self.check_required_fields(ignore_fields=ignore_fields)
        if new_resource_order:
            existing_order = [(x["name"], x["format"].lower()) for x in self.resources]
            if existing_order != new_resource_order:
                sorted_resources = sorted(
                    self.resources,
                    key=lambda x: new_resource_order.index(
                        (x["name"], x["format"].lower())
                    ),
                )
                self.reorder_resources(
                    [x["id"] for x in sorted_resources], hxl_update=False
                )
        if create_default_views:
            self.create_default_views()
        self._create_preview_resourceview()
        if hxl_update:
            self.hxl_update()
        return results

    def _dataset_update_resources(
        self,
        update_resources: bool,
        match_resources_by_metadata: bool,
        remove_additional_resources: bool,
        match_resource_order: bool,
        **kwargs: Any,
    ) -> Tuple[List, List, Dict, List]:
        """Helper method to compare new and existing dataset data returning
        resources to be updated, resources to be deleted, resources where files
        need to be uploaded to the filestore and if match_resource_order is
        True, then the new resource order.

        Args:
            update_resources (bool): Whether to update resources
            match_resources_by_metadata (bool): Compare resource metadata rather than position in list
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating)
            match_resource_order (bool): Match order of given resources by name

        Returns:
            Tuple[List, List, Dict, List]: (resources_to_update, resources_to_delete, filestore_resources, new_resource_order)
        """
        # When the user sets up a dataset, "data" contains the metadata. The
        # HDX dataset update process involves copying "data" to "old_data" and
        # then reading the existing dataset on HDX into "data". Hence,
        # "old_data" below contains the user-supplied data we want to use for
        # updating while "data" contains the data read from HDX
        resources_metadata_to_update = self.old_data.get("resources", None)
        resources_to_update = []
        resources_to_delete = []
        filestore_resources = {}
        if update_resources and resources_metadata_to_update:
            if match_resources_by_metadata:
                (
                    resource_matches,
                    updated_resource_matches,
                    resource_no_matches,
                    updated_resource_no_matches,
                ) = ResourceMatcher.match_resource_lists(
                    self.resources, resources_metadata_to_update
                )
                for i, resource in enumerate(self.resources):
                    try:
                        match_index = resource_matches.index(i)
                    except ValueError:
                        resources_to_update.append(res_module.Resource({}))
                        continue
                    resource_data_to_update = resources_metadata_to_update[
                        updated_resource_matches[match_index]
                    ]
                    logger.warning(f"Resource exists. Updating {resource['name']}")
                    FilestoreHelper.dataset_update_filestore_resource(
                        resource_data_to_update,
                        filestore_resources,
                        i,
                    )
                    resources_to_update.append(resource_data_to_update)

                resource_index = len(self.resources)
                for updated_resource_index in updated_resource_no_matches:
                    resource_data_to_update = resources_metadata_to_update[
                        updated_resource_index
                    ]
                    FilestoreHelper.check_filestore_resource(
                        resource_data_to_update,
                        filestore_resources,
                        resource_index,
                        **kwargs,
                    )
                    resource_index = resource_index + 1
                    resources_to_update.append(resource_data_to_update)
                if remove_additional_resources:
                    for resource_index in resource_no_matches:
                        resource = self.resources[resource_index]
                        logger.warning(
                            f"Removing additional resource {resource['name']}!"
                        )
                        resources_to_delete.append(resource_index)
            else:  # update resources by position
                for i, resource_data_to_update in enumerate(
                    resources_metadata_to_update
                ):
                    if len(self.resources) > i:
                        updated_resource_name = resource_data_to_update["name"]
                        resource_name = self.resources[i]["name"]
                        logger.warning(f"Resource exists. Updating {resource_name}")
                        if resource_name != updated_resource_name:
                            logger.warning(
                                f"Changing resource name to: {updated_resource_name}"
                            )
                        FilestoreHelper.dataset_update_filestore_resource(
                            resource_data_to_update,
                            filestore_resources,
                            i,
                        )
                    else:
                        FilestoreHelper.check_filestore_resource(
                            resource_data_to_update,
                            filestore_resources,
                            i,
                            **kwargs,
                        )
                    resources_to_update.append(resource_data_to_update)

                for i, resource in enumerate(self.resources):
                    if len(resources_metadata_to_update) <= i:
                        resources_to_update.append(resource)
                        if remove_additional_resources:
                            logger.warning(
                                f"Removing additional resource {resource['name']}!"
                            )
                            resources_to_delete.append(i)
        resources_to_delete = list(reversed(resources_to_delete))
        if match_resource_order:
            new_resource_order = [
                (x["name"], x["format"].lower()) for x in resources_metadata_to_update
            ]
        else:
            new_resource_order = None
        return (
            resources_to_update,
            resources_to_delete,
            filestore_resources,
            new_resource_order,
        )

    def _dataset_hdx_update(
        self,
        update_resources: bool,
        match_resources_by_metadata: bool,
        keys_to_delete: ListTuple[str],
        remove_additional_resources: bool,
        match_resource_order: bool,
        create_default_views: bool,
        hxl_update: bool,
        **kwargs: Any,
    ) -> Dict:
        """Helper method to compare new and existing dataset data, update
        resources including those with files in the filestore, delete extra
        resources if needed and update dataset data and save the dataset and
        resources to HDX.

        Args:
            update_resources (bool): Whether to update resources
            match_resources_by_metadata (bool): Compare resource metadata rather than position in list
            keys_to_delete (ListTuple[str]): List of top level metadata keys to delete
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating)
            match_resource_order (bool): Match order of given resources by name
            create_default_views (bool): Whether to call package_create_default_resource_views.
            hxl_update (bool): Whether to call package_hxl_update.

        Returns:
            Dict: Dictionary of what gets passed to the revise call (for testing)
        """
        (
            resources_to_update,
            resources_to_delete,
            filestore_resources,
            new_resource_order,
        ) = self._dataset_update_resources(
            update_resources,
            match_resources_by_metadata,
            remove_additional_resources,
            match_resource_order,
            **kwargs,
        )
        keep_crisis_tags = kwargs.get("keep_crisis_tags", True)
        if keep_crisis_tags:
            for tag in self.data["tags"]:
                tag_name = tag["name"]
                if tag_name[:7] != "crisis-":
                    continue
                found = False
                for old_tag in self.old_data["tags"]:
                    if old_tag["name"] == tag_name:
                        found = True
                        break
                if not found:
                    self.old_data["tags"].append(tag)
        self._prepare_hdx_call(self.old_data, kwargs)
        return self._revise_dataset(
            keys_to_delete,
            resources_to_update,
            resources_to_delete,
            filestore_resources,
            new_resource_order,
            hxl_update,
            create_default_views=create_default_views,
            **kwargs,
        )

    def update_in_hdx(
        self,
        update_resources: bool = True,
        match_resources_by_metadata: bool = True,
        keys_to_delete: ListTuple[str] = tuple(),
        remove_additional_resources: bool = False,
        match_resource_order: bool = False,
        create_default_views: bool = True,
        hxl_update: bool = True,
        **kwargs: Any,
    ) -> None:
        """Check if dataset exists in HDX and if so, update it. match_resources_by_metadata uses ids if they are
        available, otherwise names only if names are unique or format in addition if not.

        Args:
            update_resources (bool): Whether to update resources. Defaults to True.
            match_resources_by_metadata (bool): Compare resource metadata rather than position in list. Defaults to True.
            keys_to_delete (ListTuple[str]): List of top level metadata keys to delete. Defaults to tuple().
            remove_additional_resources (bool): Remove additional resources found in dataset. Defaults to False.
            match_resource_order (bool): Match order of given resources by name. Defaults to False.
            create_default_views (bool): Whether to call package_create_default_resource_views. Defaults to True.
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.
            **kwargs: See below
            keep_crisis_tags (bool): Whether to keep existing crisis tags. Defaults to True.
            updated_by_script (str): String to identify your script. Defaults to your user agent.
            batch (str): A string you can specify to show which datasets are part of a single batch update

        Returns:
            None
        """
        loaded = False
        if "id" in self.data:
            self._check_existing_object("dataset", "id")
            if self._dataset_load_from_hdx(self.data["id"]):
                loaded = True
            else:
                logger.warning(f"Failed to load dataset with id {self.data['id']}")
        if not loaded:
            self._check_existing_object("dataset", "name")
            if not self._dataset_load_from_hdx(self.data["name"]):
                raise HDXError("No existing dataset to update!")
        self._dataset_hdx_update(
            update_resources=update_resources,
            match_resources_by_metadata=match_resources_by_metadata,
            keys_to_delete=keys_to_delete,
            remove_additional_resources=remove_additional_resources,
            match_resource_order=match_resource_order,
            create_default_views=create_default_views,
            hxl_update=hxl_update,
            **kwargs,
        )
        logger.info(f"Updated {self.get_hdx_url()}")

    def create_in_hdx(
        self,
        allow_no_resources: bool = False,
        update_resources: bool = True,
        match_resources_by_metadata: bool = True,
        keys_to_delete: ListTuple[str] = tuple(),
        remove_additional_resources: bool = False,
        match_resource_order: bool = False,
        create_default_views: bool = True,
        hxl_update: bool = True,
        **kwargs: Any,
    ) -> None:
        """Check if dataset exists in HDX and if so, update it, otherwise create it. match_resources_by_metadata uses
        ids if they are available, otherwise names only if names are unique or format in addition if not.

        Args:
            allow_no_resources (bool): Whether to allow no resources. Defaults to False.
            update_resources (bool): Whether to update resources (if updating). Defaults to True.
            match_resources_by_metadata (bool): Compare resource metadata rather than position in list. Defaults to True.
            keys_to_delete (ListTuple[str]): List of top level metadata keys to delete. Defaults to tuple().
            remove_additional_resources (bool): Remove additional resources found in dataset (if updating). Defaults to False.
            match_resource_order (bool): Match order of given resources by name. Defaults to False.
            create_default_views (bool): Whether to call package_create_default_resource_views (if updating). Defaults to True.
            hxl_update (bool): Whether to call package_hxl_update. Defaults to True.
            **kwargs: See below
            keep_crisis_tags (bool): Whether to keep existing crisis tags. Defaults to True.
            updated_by_script (str): String to identify your script. Defaults to your user agent.
            batch (str): A string you can specify to show which datasets are part of a single batch update

        Returns:
            None
        """
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            self.check_required_fields(allow_no_resources=allow_no_resources, **kwargs)
            # No need to check again after revising dataset
            kwargs["ignore_check"] = True
        loadedid = None
        if "id" in self.data:
            if self._dataset_load_from_hdx(self.data["id"]):
                loadedid = self.data["id"]
            else:
                logger.warning(f"Failed to load dataset with id {self.data['id']}")
        if not loadedid:
            if self._dataset_load_from_hdx(self.data["name"]):
                loadedid = self.data["name"]
        if loadedid:
            self._dataset_hdx_update(
                update_resources=update_resources,
                match_resources_by_metadata=match_resources_by_metadata,
                keys_to_delete=keys_to_delete,
                remove_additional_resources=remove_additional_resources,
                match_resource_order=match_resource_order,
                create_default_views=create_default_views,
                hxl_update=hxl_update,
                **kwargs,
            )
            logger.info(f"Updated {self.get_hdx_url()}")
            return

        filestore_resources = {}
        if self.resources:
            for i, resource in enumerate(self.resources):
                FilestoreHelper.check_filestore_resource(
                    resource, filestore_resources, i, **kwargs
                )
        self.unseparate_resources()
        self._prepare_hdx_call(self.data, kwargs)
        kwargs["operation"] = "create"
        if not kwargs.get("ignore_check"):
            kwargs["ignore_check"] = True
        self._hdx_update("dataset", "name", force_active=True, **kwargs)
        if not filestore_resources and not keys_to_delete:
            self.init_resources()
            self.separate_resources()
        else:
            self._revise_dataset(
                keys_to_delete,
                self.resources,
                [],
                filestore_resources,
                None,
                hxl_update,
                **kwargs,
            )
        logger.info(f"Created {self.get_hdx_url()}")

    def delete_from_hdx(self) -> None:
        """Deletes a dataset from HDX.

        Returns:
            None
        """
        self._delete_from_hdx("dataset", "id")

    def hxl_update(self) -> None:
        """Checks dataset for HXL in resources and updates tags and other metadata to trigger HXL preview.

        Returns:
            None
        """
        self._read_from_hdx("dataset", self.data["id"], action=self.actions()["hxl"])

    @classmethod
    def search_in_hdx(
        cls,
        query: Optional[str] = "*:*",
        configuration: Optional[Configuration] = None,
        page_size: int = 1000,
        **kwargs: Any,
    ) -> List["Dataset"]:
        """Searches for datasets in HDX

        Args:
            query (Optional[str]): Query (in Solr format). Defaults to '*:*'.
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            page_size (int): Size of page to use internally to query HDX. Defaults to 1000.
            **kwargs: See below
            fq (string): Any filter queries to apply
            rows (int): Number of matching rows to return. Defaults to all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            sort (string): Sorting of results. Defaults to 'relevance asc, metadata_modified desc' if rows<=page_size or 'metadata_modified asc' if rows>page_size.
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (List[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List[Dataset]: list of datasets resulting from query
        """

        dataset = Dataset(configuration=configuration)
        limit = kwargs.get("limit")
        total_rows = kwargs.get("rows")
        if limit:
            del kwargs["limit"]
            if not total_rows:
                total_rows = limit
        else:
            if not total_rows:
                total_rows = cls.max_int
        sort = kwargs.get("sort")
        if not sort:
            if total_rows > page_size:
                kwargs["sort"] = "metadata_created asc"
            else:
                kwargs["sort"] = "relevance asc, metadata_modified desc"
        offset = kwargs.get("offset")
        start = kwargs.get("start")
        if offset:
            del kwargs["offset"]
            if not start:
                start = offset
        else:
            if not start:
                start = 0
        all_datasets = None
        attempts = 0
        while (
            attempts < cls.max_attempts and all_datasets is None
        ):  # if the count values vary for multiple calls, then must redo query
            all_datasets = []
            counts = set()
            for page in range(total_rows // page_size + 1):
                pagetimespagesize = page * page_size
                kwargs["start"] = start + pagetimespagesize
                rows_left = total_rows - pagetimespagesize
                rows = min(rows_left, page_size)
                kwargs["rows"] = rows
                _, result = dataset._read_from_hdx(
                    "dataset",
                    query,
                    "q",
                    Dataset.actions()["search"],
                    **kwargs,
                )
                datasets = []
                if result:
                    count = result.get("count", None)
                    if count:
                        counts.add(count)
                        no_results = len(result["results"])
                        for datasetdict in result["results"]:
                            dataset = Dataset(configuration=configuration)
                            dataset.old_data = {}
                            dataset.data = datasetdict
                            dataset._dataset_create_resources()
                            datasets.append(dataset)
                        all_datasets += datasets
                        if no_results < rows:
                            break
                    else:
                        break
                else:
                    logger.debug(result)
            if (
                kwargs["sort"] != "metadata_created asc"
                and all_datasets
                and len(counts) != 1
            ):  # Make sure counts are all same for multiple calls to HDX
                all_datasets = None
                attempts += 1
            else:
                ids = [
                    dataset["id"] for dataset in all_datasets
                ]  # check for duplicates (shouldn't happen)
                if len(ids) != len(set(ids)):
                    all_datasets = None
                    attempts += 1
        if attempts == cls.max_attempts and all_datasets is None:
            raise HDXError("Maximum attempts reached for searching for datasets!")
        return all_datasets

    @staticmethod
    def get_all_dataset_names(
        configuration: Optional[Configuration] = None, **kwargs: Any
    ) -> List[str]:
        """Get all dataset names in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            rows (int): Number of rows to return. Defaults to all datasets (sys.maxsize)
            start (int): Offset in the complete result for where the set of returned dataset names should begin

        Returns:
            List[str]: list of all dataset names in HDX
        """
        total_rows = kwargs.get("rows")
        if total_rows:
            del kwargs["rows"]
            kwargs["limit"] = total_rows
        start = kwargs.get("start")
        if start:
            del kwargs["start"]
            kwargs["offset"] = start
        dataset = Dataset(configuration=configuration)
        return dataset._write_to_hdx("list", kwargs)

    @classmethod
    def get_all_datasets(
        cls,
        configuration: Optional[Configuration] = None,
        page_size: int = 1000,
        **kwargs: Any,
    ) -> List["Dataset"]:
        """Get all datasets from HDX (just calls search_in_hdx)

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            page_size (int): Size of page to use internally to query HDX. Defaults to 1000.
            **kwargs: See below
            fq (string): Any filter queries to apply
            rows (int): Number of matching rows to return. Defaults to all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            sort (string): Sorting of results. Defaults to 'metadata_modified asc'.
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (List[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List[Dataset]: list of datasets resulting from query
        """
        if "sort" not in kwargs:
            kwargs["sort"] = "metadata_created asc"
        return cls.search_in_hdx(
            query="*:*",
            configuration=configuration,
            page_size=page_size,
            **kwargs,
        )

    @staticmethod
    def get_all_resources(
        datasets: ListTuple["Dataset"],
    ) -> List["Resource"]:
        """Get all resources from a list of datasets (such as returned by search)

        Args:
            datasets (ListTuple[Dataset]): list of datasets

        Returns:
            List[Resource]: list of resources within those datasets
        """
        resources = []
        for dataset in datasets:
            for resource in dataset.get_resources():
                resources.append(resource)
        return resources

    @classmethod
    def autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Optional[Configuration] = None,
    ) -> List:
        """Autocomplete a dataset name and return matches

        Args:
            name (str): Name to autocomplete
            limit (int): Maximum number of matches to return
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List: Autocomplete matches
        """
        return cls._autocomplete(name, limit, configuration)

    def get_time_period(
        self,
        date_format: Optional[str] = None,
        today: datetime = now_utc(),
    ) -> Dict:
        """Get dataset date as datetimes and strings in specified format. If no format is supplied, the ISO 8601
        format is used. Returns a dictionary containing keys startdate (start date as datetime), enddate (end
        date as datetime), startdate_str (start date as string), enddate_str (end date as string) and ongoing
        (whether the end date is a rolls forward every day).

        Args:
            date_format (Optional[str]): Date format. None is taken to be ISO 8601. Defaults to None.
            today (datetime): Date to use for today. Defaults to now_utc().

        Returns:
            Dict: Dictionary of date information
        """
        return DateHelper.get_time_period_info(
            self.data.get("dataset_date"), date_format, today
        )

    def get_reference_period(
        self,
        date_format: Optional[str] = None,
        today: datetime = now_utc(),
    ) -> Dict:
        warnings.warn(
            "get_reference_period() is deprecated, use get_time_period() instead",
            DeprecationWarning,
        )
        return self.get_time_period(date_format, today)

    def set_time_period(
        self,
        startdate: Union[datetime, str],
        enddate: Union[datetime, str, None] = None,
        ongoing: bool = False,
        ignore_timeinfo: bool = True,
    ) -> None:
        """Set time period from either datetime objects or strings. Any time and time
        zone information will be ignored by default (meaning that the time of the start
        date is set to 00:00:00, the time of any end date is set to 23:59:59 and the
        time zone is set to UTC). To have the time and time zone accounted for, set
        ignore_timeinfo to False. In this case, the time will be converted to UTC.

        Args:
            startdate (Union[datetime, str]): Dataset start date
            enddate (Union[datetime, str, None]): Dataset end date. Defaults to None.
            ongoing (bool): True if ongoing, False if not. Defaults to False.
            ignore_timeinfo (bool): Ignore time and time zone of date. Defaults to True.

        Returns:
            None
        """
        self.data["dataset_date"] = DateHelper.get_hdx_time_period(
            startdate, enddate, ongoing, ignore_timeinfo
        )

    def set_reference_period(
        self,
        startdate: Union[datetime, str],
        enddate: Union[datetime, str, None] = None,
        ongoing: bool = False,
        ignore_timeinfo: bool = True,
    ) -> None:
        warnings.warn(
            "set_reference_period() is deprecated, use set_time_period() instead",
            DeprecationWarning,
        )
        self.set_time_period(startdate, enddate, ongoing, ignore_timeinfo)

    def set_time_period_year_range(
        self,
        dataset_year: Union[str, int, Iterable],
        dataset_end_year: Optional[Union[str, int]] = None,
    ) -> List[int]:
        """Set time period as a range from year or start and end year.

        Args:
            dataset_year (Union[str, int, Iterable]): Dataset year given as string or int or range in an iterable
            dataset_end_year (Optional[Union[str, int]]): Dataset end year given as string or int

        Returns:
            List[int]: The start and end year if supplied or sorted list of years
        """
        (
            self.data["dataset_date"],
            retval,
        ) = DateHelper.get_hdx_time_period_from_years(dataset_year, dataset_end_year)
        return retval

    def set_reference_period_year_range(
        self,
        dataset_year: Union[str, int, Iterable],
        dataset_end_year: Optional[Union[str, int]] = None,
    ) -> List[int]:
        warnings.warn(
            "set_reference_period_year_range() is deprecated, use set_time_period_year_range() instead",
            DeprecationWarning,
        )
        return self.set_time_period_year_range(dataset_year, dataset_end_year)

    @classmethod
    def list_valid_update_frequencies(cls) -> List[str]:
        """List of valid update frequency values

        Returns:
            List[str]: Allowed update frequencies
        """
        return list(cls.update_frequencies.keys())

    @classmethod
    def transform_update_frequency(cls, frequency: Union[str, int]) -> Optional[str]:
        """Get numeric update frequency (as string since that is required field format) from textual representation or
        vice versa (eg. 'Every month' = '30', '30' or 30 = 'Every month')

        Args:
            frequency (Union[str, int]): Update frequency in one format

        Returns:
            Optional[str]: Update frequency in alternative format or None if not valid
        """
        if isinstance(frequency, int):
            frequency = str(frequency)
        return cls.update_frequencies.get(frequency.lower())

    def get_expected_update_frequency(self) -> Optional[str]:
        """Get expected update frequency (in textual rather than numeric form)

        Returns:
            Optional[str]: Update frequency in textual form or None if the update frequency doesn't exist or is blank.
        """
        days = self.data.get("data_update_frequency", None)
        if days:
            return Dataset.transform_update_frequency(days)
        else:
            return None

    def set_expected_update_frequency(self, update_frequency: Union[str, int]) -> None:
        """Set expected update frequency. You can pass frequencies like "Every week" or '7' or 7. Valid values for
        update frequency can be found from Dataset.list_valid_update_frequencies().

        Args:
            update_frequency (Union[str, int]): Update frequency

        Returns:
            None
        """
        if isinstance(update_frequency, int):
            update_frequency = str(update_frequency)
        try:
            int(update_frequency)
        except ValueError:
            update_frequency = Dataset.transform_update_frequency(update_frequency)
        if update_frequency not in Dataset.update_frequencies.keys():
            raise HDXError("Invalid update frequency supplied!")
        self.data["data_update_frequency"] = update_frequency

    def get_tags(self) -> List[str]:
        """Return the dataset's list of tags

        Returns:
            List[str]: list of tags or [] if there are none
        """
        return self._get_tags()

    def add_tag(
        self, tag: str, log_deleted: bool = True
    ) -> Tuple[List[str], List[str]]:
        """Add a tag

        Args:
            tag (str): Tag to add
            log_deleted (bool): Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple[List[str], List[str]]: Tuple containing list of added tags and list of deleted tags and tags not added
        """
        return vocabulary.Vocabulary.add_mapped_tag(self, tag, log_deleted=log_deleted)

    def add_tags(
        self, tags: ListTuple[str], log_deleted: bool = True
    ) -> Tuple[List[str], List[str]]:
        """Add a list of tags

        Args:
            tags (ListTuple[str]): List of tags to add
            log_deleted (bool): Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple[List[str], List[str]]: Tuple containing list of added tags and list of deleted tags and tags not added
        """
        return vocabulary.Vocabulary.add_mapped_tags(
            self, tags, log_deleted=log_deleted
        )

    def clean_tags(self, log_deleted: bool = True) -> Tuple[List[str], List[str]]:
        """Clean tags in an HDX object according to tags cleanup spreadsheet, deleting invalid tags that cannot be mapped

        Args:
            log_deleted (bool): Whether to log informational messages about deleted tags. Defaults to True.

        Returns:
            Tuple[List[str], List[str]]: Tuple containing list of mapped tags and list of deleted tags and tags not added
        """
        return vocabulary.Vocabulary.clean_tags(self, log_deleted=log_deleted)

    def remove_tag(self, tag: str) -> bool:
        """Remove a tag

        Args:
            tag (str): Tag to remove

        Returns:
            bool: True if tag removed or False if not
        """
        return self._remove_hdxobject(
            self.data.get("tags"), tag.lower(), matchon="name"
        )

    def is_subnational(self) -> bool:
        """Return if the dataset is subnational

        Returns:
            bool: True if the dataset is subnational, False if not
        """
        return self.data["subnational"] == "1"

    def set_subnational(self, subnational: bool) -> None:
        """Set if dataset is subnational or national

        Args:
            subnational (bool): True for subnational, False for national

        Returns:
            None
        """
        if subnational:
            self.data["subnational"] = "1"
        else:
            self.data["subnational"] = "0"

    def get_location_iso3s(
        self, locations: Optional[ListTuple[str]] = None
    ) -> List[str]:
        """Return the dataset's location

        Args:
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.

        Returns:
            List[str]: list of location iso3s
        """
        countries = self.data.get("groups")
        countryisos = []
        if not countries:
            return countryisos
        for country in countries:
            countryiso = Locations.get_HDX_code_from_location(
                country["name"],
                locations=locations,
                configuration=self.configuration,
            )
            if countryiso:
                countryisos.append(countryiso)
        return countryisos

    def get_location_names(
        self, locations: Optional[ListTuple[str]] = None
    ) -> List[str]:
        """Return the dataset's location

        Args:
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.

        Returns:
            List[str]: list of location names
        """
        countries = self.data.get("groups")
        countrynames = []
        if not countries:
            return countrynames
        for country in countries:
            countryname = Locations.get_location_from_HDX_code(
                country["name"],
                locations=locations,
                configuration=self.configuration,
            )
            if countryname:
                countrynames.append(countryname)
        return countrynames

    def add_country_location(
        self,
        country: str,
        exact: bool = True,
        locations: Optional[ListTuple[str]] = None,
        use_live: bool = True,
    ) -> bool:
        """Add a country. If an iso 3 code is not provided, value is parsed and if it is a valid country name,
        converted to an iso 3 code. If the country is already added, it is ignored.

        Args:
            country (str): Country to add
            exact (bool): True for exact matching or False to allow fuzzy matching. Defaults to True.
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if country added or False if country already present
        """
        iso3, match = Country.get_iso3_country_code_fuzzy(country, use_live=use_live)
        if iso3 is None:
            raise HDXError(f"Country: {country} - cannot find iso3 code!")
        return self.add_other_location(
            iso3,
            exact=exact,
            alterror="Country: %s with iso3: %s could not be found in HDX list!"
            % (country, iso3),
            locations=locations,
        )

    def add_country_locations(
        self,
        countries: ListTuple[str],
        locations: Optional[ListTuple[str]] = None,
        use_live: bool = True,
    ) -> bool:
        """Add a list of countries. If iso 3 codes are not provided, values are parsed and where they are valid country
        names, converted to iso 3 codes. If any country is already added, it is ignored.

        Args:
            countries (ListTuple[str]): List of countries to add
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if all countries added or False if any already present.
        """
        allcountriesadded = True
        for country in countries:
            if not self.add_country_location(
                country, locations=locations, use_live=use_live
            ):
                allcountriesadded = False
        return allcountriesadded

    def add_region_location(
        self,
        region: str,
        locations: Optional[ListTuple[str]] = None,
        use_live: bool = True,
    ) -> bool:
        """Add all countries in a region. If a 3 digit UNStats M49 region code is not provided, value is parsed as a
        region name. If any country is already added, it is ignored.

        Args:
            region (str): M49 region, intermediate region or subregion to add
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.
            use_live (bool): Try to get use latest country data from web rather than file in package. Defaults to True.

        Returns:
            bool: True if all countries in region added or False if any already present.
        """
        return self.add_country_locations(
            Country.get_countries_in_region(
                region, exception=HDXError, use_live=use_live
            ),
            locations=locations,
        )

    def add_other_location(
        self,
        location: str,
        exact: bool = True,
        alterror: Optional[str] = None,
        locations: Optional[ListTuple[str]] = None,
    ) -> bool:
        """Add a location which is not a country or region. Value is parsed and compared to existing locations in
        HDX. If the location is already added, it is ignored.

        Args:
            location (str): Location to add
            exact (bool): True for exact matching or False to allow fuzzy matching. Defaults to True.
            alterror (Optional[str]): Alternative error message to builtin if location not found. Defaults to None.
            locations (Optional[ListTuple[str]]): Valid locations list. Defaults to list downloaded from HDX.

        Returns:
            bool: True if location added or False if location already present
        """
        hdx_code, match = Locations.get_HDX_code_from_location_partial(
            location, locations=locations, configuration=self.configuration
        )
        if hdx_code is None or (exact is True and match is False):
            if alterror is None:
                raise HDXError(f"Location: {location} - cannot find in HDX!")
            else:
                raise HDXError(alterror)
        groups = self.data.get("groups", None)
        hdx_code = hdx_code.lower()
        if groups:
            if hdx_code in [x["name"] for x in groups]:
                return False
        else:
            groups = []
        groups.append({"name": hdx_code})
        self.data["groups"] = groups
        return True

    def remove_location(self, location: str) -> bool:
        """Remove a location. If the location is already added, it is ignored.

        Args:
            location (str): Location to remove

        Returns:
            bool: True if location removed or False if not
        """
        res = self._remove_hdxobject(self.data.get("groups"), location, matchon="name")
        if not res:
            res = self._remove_hdxobject(
                self.data.get("groups"), location.upper(), matchon="name"
            )
        if not res:
            res = self._remove_hdxobject(
                self.data.get("groups"), location.lower(), matchon="name"
            )
        return res

    def get_maintainer(self) -> "User":
        """Get the dataset's maintainer.

        Returns:
            User: Dataset's maintainer
        """
        return user.User.read_from_hdx(
            self.data["maintainer"], configuration=self.configuration
        )

    def set_maintainer(self, maintainer: Union["User", Dict, str]) -> None:
        """Set the dataset's maintainer.

        Args:
            maintainer (Union[User,Dict,str]): Either a user id or User metadata from a User object or dictionary.
        Returns:
            None
        """
        if isinstance(maintainer, user.User) or isinstance(maintainer, dict):
            if "id" not in maintainer:
                maintainer = user.User.read_from_hdx(
                    maintainer["name"], configuration=self.configuration
                )
            maintainer = maintainer["id"]
        elif not isinstance(maintainer, str):
            raise HDXError(
                f"Type {type(maintainer).__name__} cannot be added as a maintainer!"
            )
        if is_valid_uuid(maintainer) is False:
            raise HDXError(f"{maintainer} is not a valid user id for a maintainer!")
        self.data["maintainer"] = maintainer

    def get_organization(self) -> "Organization":
        """Get the dataset's organization.

        Returns:
            Organization: Dataset's organization
        """
        return org_module.Organization.read_from_hdx(
            self.data["owner_org"], configuration=self.configuration
        )

    def set_organization(
        self,
        organization: Union["Organization", Dict, str],
    ) -> None:
        """Set the dataset's organization.

        Args:
            organization (Union[Organization,Dict,str]): Either an Organization id or Organization metadata from an Organization object or dictionary.
        Returns:
            None
        """
        if isinstance(organization, org_module.Organization) or isinstance(
            organization, dict
        ):
            if "id" not in organization:
                organization = org_module.Organization.read_from_hdx(
                    organization["name"], configuration=self.configuration
                )
            organization = organization["id"]
        elif not isinstance(organization, str):
            raise HDXError(
                f"Type {type(organization).__name__} cannot be added as a organization!"
            )
        if is_valid_uuid(organization) is False and organization != "hdx":
            raise HDXError(f"{organization} is not a valid organization id!")
        self.data["owner_org"] = organization

    def get_showcases(self) -> List["Showcase"]:
        """Get any showcases the dataset is in

        Returns:
            List[Showcase]: List of showcases
        """
        assoc_result, showcases_dicts = self._read_from_hdx(
            "showcase",
            self.data["id"],
            fieldname="package_id",
            action=sc_module.Showcase.actions()["list_showcases"],
        )
        showcases = []
        if assoc_result:
            for showcase_dict in showcases_dicts:
                showcase = sc_module.Showcase(
                    showcase_dict, configuration=self.configuration
                )
                showcases.append(showcase)
        return showcases

    def _get_dataset_showcase_dict(
        self, showcase: Union["Showcase", Dict, str]
    ) -> Dict:
        """Get dataset showcase dict

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id or Showcase metadata from a Showcase object or dictionary

        Returns:
            dict: Dataset showcase dict
        """
        if isinstance(showcase, sc_module.Showcase) or isinstance(showcase, dict):
            if "id" not in showcase:
                showcase = sc_module.Showcase.read_from_hdx(showcase["name"])
            showcase = showcase["id"]
        elif not isinstance(showcase, str):
            raise HDXError(
                f"Type {type(showcase).__name__} cannot be added as a showcase!"
            )
        if is_valid_uuid(showcase) is False:
            raise HDXError(f"{showcase} is not a valid showcase id!")
        return {"package_id": self.data["id"], "showcase_id": showcase}

    def add_showcase(
        self,
        showcase: Union["Showcase", Dict, str],
        showcases_to_check: ListTuple["Showcase"] = None,
    ) -> bool:
        """Add dataset to showcase

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id or showcase metadata from a Showcase object or dictionary
            showcases_to_check (ListTuple[Showcase]): List of showcases against which to check existence of showcase. Defaults to showcases containing dataset.

        Returns:
            bool: True if the showcase was added, False if already present
        """
        dataset_showcase = self._get_dataset_showcase_dict(showcase)
        if showcases_to_check is None:
            showcases_to_check = self.get_showcases()
        for showcase in showcases_to_check:
            if dataset_showcase["showcase_id"] == showcase["id"]:
                return False
        showcase = sc_module.Showcase(
            {"id": dataset_showcase["showcase_id"]},
            configuration=self.configuration,
        )
        showcase._write_to_hdx("associate", dataset_showcase, "package_id")
        return True

    def add_showcases(
        self,
        showcases: ListTuple[Union["Showcase", Dict, str]],
        showcases_to_check: ListTuple["Showcase"] = None,
    ) -> bool:
        """Add dataset to multiple showcases

        Args:
            showcases (ListTuple[Union[Showcase,Dict,str]]): A list of either showcase ids or showcase metadata from Showcase objects or dictionaries
            showcases_to_check (ListTuple[Showcase]): list of showcases against which to check existence of showcase. Defaults to showcases containing dataset.

        Returns:
            bool: True if all showcases added or False if any already present
        """
        if showcases_to_check is None:
            showcases_to_check = self.get_showcases()
        allshowcasesadded = True
        for showcase in showcases:
            if not self.add_showcase(showcase, showcases_to_check=showcases_to_check):
                allshowcasesadded = False
        return allshowcasesadded

    def remove_showcase(self, showcase: Union["Showcase", Dict, str]) -> None:
        """Remove dataset from showcase

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id string or showcase metadata from a Showcase object or dictionary

        Returns:
            None
        """
        dataset_showcase = self._get_dataset_showcase_dict(showcase)
        showcase = sc_module.Showcase(
            {"id": dataset_showcase["showcase_id"]},
            configuration=self.configuration,
        )
        showcase._write_to_hdx("disassociate", dataset_showcase, "package_id")

    def is_requestable(self) -> bool:
        """Return whether the dataset is requestable or not

        Returns:
            bool: Whether the dataset is requestable or not
        """
        return self.data.get("is_requestdata_type", False)

    def set_requestable(self, requestable: bool = True) -> None:
        """Set the dataset to be of type requestable or not

        Args:
            requestable (bool): Set whether dataset is requestable. Defaults to True.

        Returns:
            None
        """
        self.data["is_requestdata_type"] = requestable
        if requestable:
            self.data["private"] = False

    def get_fieldnames(self) -> List[str]:
        """Return list of fieldnames in your data. Only applicable to requestable datasets.

        Returns:
            List[str]: List of field names
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "get_fieldnames is only applicable to requestable datasets!"
            )
        return self._get_stringlist_from_commastring("field_names")

    def add_fieldname(self, fieldname: str) -> bool:
        """Add a fieldname to list of fieldnames in your data. Only applicable to requestable datasets.

        Args:
            fieldname (str): Fieldname to add

        Returns:
            bool: True if fieldname added or False if tag already present
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "add_fieldname is only applicable to requestable datasets!"
            )
        return self._add_string_to_commastring("field_names", fieldname)

    def add_fieldnames(self, fieldnames: ListTuple[str]) -> bool:
        """Add a list of fieldnames to list of fieldnames in your data. Only applicable to requestable datasets.

        Args:
            fieldnames (ListTuple[str]): List of fieldnames to add

        Returns:
            bool: True if all fieldnames added or False if any already present
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "add_fieldnames is only applicable to requestable datasets!"
            )
        return self._add_strings_to_commastring("field_names", fieldnames)

    def remove_fieldname(self, fieldname: str) -> bool:
        """Remove a fieldname. Only applicable to requestable datasets.

        Args:
            fieldname (str): Fieldname to remove

        Returns:
            bool: True if fieldname removed or False if not
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "remove_fieldname is only applicable to requestable datasets!"
            )
        return self._remove_string_from_commastring("field_names", fieldname)

    def get_filetypes(self) -> List[str]:
        """Return list of filetypes in your data

        Returns:
            List[str]: List of filetypes
        """
        if not self.is_requestable():
            return [resource.get_format() for resource in self.resources]
        return self._get_stringlist_from_commastring("file_types")

    def add_filetype(self, filetype: str) -> bool:
        """Add a filetype to list of filetypes in your data. Only applicable to requestable datasets.

        Args:
            filetype (str): filetype to add

        Returns:
            bool: True if filetype added or False if tag already present
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "add_filetype is only applicable to requestable datasets!"
            )
        return self._add_string_to_commastring("file_types", filetype)

    def add_filetypes(self, filetypes: ListTuple[str]) -> bool:
        """Add a list of filetypes to list of filetypes in your data. Only applicable to requestable datasets.

        Args:
            filetypes (ListTuple[str]): list of filetypes to add

        Returns:
            bool: True if all filetypes added or False if any already present
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "add_filetypes is only applicable to requestable datasets!"
            )
        return self._add_strings_to_commastring("file_types", filetypes)

    def remove_filetype(self, filetype: str) -> bool:
        """Remove a filetype

        Args:
            filetype (str): Filetype to remove

        Returns:
            bool: True if filetype removed or False if not
        """
        if not self.is_requestable():
            raise NotRequestableError(
                "remove_filetype is only applicable to requestable datasets!"
            )
        return self._remove_string_from_commastring("file_types", filetype)

    def preview_off(self) -> None:
        """Set dataset preview off

        Returns:
            None
        """
        self.data["dataset_preview"] = "no_preview"
        for resource in self.resources:
            resource.disable_dataset_preview()

    def preview_resource(self) -> None:
        """Set dataset preview on for an unspecified resource

        Returns:
            None
        """
        self.data["dataset_preview"] = "resource_id"

    def set_quickchart_resource(
        self, resource: Union["Resource", Dict, str, int]
    ) -> "Resource":
        """Set the resource that will be used for displaying QuickCharts in dataset preview

        Args:
            resource (Union[Resource,Dict,str,int]): Either resource id or name, resource metadata from a Resource object or a dictionary or position

        Returns:
            Resource: Resource that is used for preview or None if no preview set
        """
        if isinstance(resource, int) and not isinstance(resource, bool):
            resource = self.resources[resource]
        if isinstance(resource, res_module.Resource) or isinstance(resource, dict):
            res = resource.get("id")
            if res is None:
                resource = resource["name"]
            else:
                resource = res
        elif not isinstance(resource, str):
            raise HDXError(
                f"Resource id cannot be found in type {type(resource).__name__}!"
            )
        if is_valid_uuid(resource) is True:
            search = "id"
        else:
            search = "name"
        preview_resource = None
        for dataset_resource in self.resources:
            if preview_resource is None and dataset_resource[search] == resource:
                dataset_resource.enable_dataset_preview()
                self.preview_resource()
                preview_resource = dataset_resource
            else:
                dataset_resource.disable_dataset_preview()
        return preview_resource

    def quickcharts_resource_last(self) -> bool:
        """Move the QuickCharts resource to be last. Assumes that it's name begins 'QuickCharts-'.

        Returns:
            bool: True if QuickCharts resource found, False if not
        """
        for i, resource in enumerate(self.resources):
            if resource["name"][:12] == "QuickCharts-":
                self.resources.append(self.resources.pop(i))
                return True
        return False

    def create_default_views(self, create_datastore_views: bool = False) -> None:
        """Create default resource views for all resources in dataset

        Args:
            create_datastore_views (bool): Whether to try to create resource views that point to the datastore

        Returns:
            None
        """
        data = {
            "package": self.get_dataset_dict(),
            "create_datastore_views": create_datastore_views,
        }
        self._write_to_hdx("create_default_views", data, "package")

    def _create_preview_resourceview(self) -> None:
        """Creates preview resourceview

        Returns:
            None
        """
        if self.preview_resourceview:
            for resource in self.resources:
                if resource["name"] == self.preview_resourceview["resource_name"]:
                    del self.preview_resourceview["resource_name"]
                    self.preview_resourceview["resource_id"] = resource["id"]
                    self.preview_resourceview.create_in_hdx()
                    self.preview_resourceview = None
                    break

    def _generate_resource_view(
        self,
        resource: Union["Resource", Dict, str, int] = 0,
        path: Optional[str] = None,
        bites_disabled: Optional[ListTuple[bool]] = None,
        indicators: Optional[ListTuple[Dict]] = None,
        findreplace: Optional[Dict] = None,
    ) -> Optional[resource_view.ResourceView]:
        """Create QuickCharts for the given resource in a dataset. If you do
        not supply a path, then the internal indicators resource view template
        will be used. You can disable specific bites by providing
        bites_disabled, a list of 3 bools where True indicates a specific bite
        is disabled and False indicates leave enabled. The parameter indicators
        is a list with 3 dictionaries of form:
        {"code": "MY_INDICATOR_CODE", "title": "MY_INDICATOR_TITLE",
        "unit": "MY_INDICATOR_UNIT"}. Optionally, the following defaults can be
        overridden in the parameter indicators: {"code_col": "#indicator+code",
        "value_col": "#indicator+value+num", "date_col": "#date+year",
        "date_format": "%Y", "aggregate_col": "null"}.

        Creation of the resource view will be delayed until after the next
        dataset create or update if a resource id is not yet available.

        Args:
            resource (Union[Resource,Dict,str,int]): Either resource id or name, resource metadata from a Resource object or a dictionary or position. Defaults to 0.
            path (Optional[str]): Path to YAML resource view metadata. Defaults to None (config/hdx_resource_view_static.yaml or internal template).
            bites_disabled (Optional[ListTuple[bool]]): Which QC bites should be disabled. Defaults to None (all bites enabled).
            indicators (Optional[ListTuple[Dict]]): Indicator codes, QC titles and units for resource view template. Defaults to None (don't use template).
            findreplace (Optional[Dict]): Replacements for anything else in resource view. Defaults to None.

        Returns:
            Optional[resource_view.ResourceView]: The resource view if QuickCharts created, None is not
        """
        if not bites_disabled:
            bites_disabled = [False, False, False]
        elif all(i for i in bites_disabled):
            return None
        res = self.set_quickchart_resource(resource)
        if res is None:
            return None
        if "id" in res:
            resourceview_data = {"resource_id": res["id"]}
        else:
            resourceview_data = {"resource_name": res["name"]}
        resourceview = resource_view.ResourceView(resourceview_data)
        if path is None:
            if indicators is None:
                path = join("config", "hdx_resource_view_static.yaml")
                if not isfile(path):
                    path = path.replace(".yaml", ".yml")
            else:
                path = script_dir_plus_file(
                    "indicator_resource_view_template.yaml",
                    NotRequestableError,
                )
        resourceview.update_from_yaml(path=path)

        def replace_string(instr, what, withwhat):
            return instr.replace(what, str(withwhat))

        defaults = {
            "code_col": "#indicator+code",
            "value_col": "#indicator+value+num",
            "date_col": "#date+year",
            "date_format": "%Y",
            "aggregate_col": "null",
        }

        def replace_col(hxl_preview_cfg, col_str, ind, col, with_quotes=False):
            if with_quotes:
                col_str = f'"{col_str}"'
            replace = ind.get(col)
            if replace:
                if with_quotes:
                    replace = f'"{replace}"'
            else:
                replace = defaults[col]
            return replace_string(hxl_preview_cfg, col_str, replace)

        hxl_preview_config = resourceview["hxl_preview_config"]
        if indicators is None:
            indicators_notexist = [False, False, False]
        else:
            len_indicators = len(indicators)
            if len_indicators == 0:
                return None
            indicators_notexist = [True, True, True]

            def replace_indicator(qc_config, index):
                indicator = indicators[index]
                ind_str = str(index + 1)
                qc_config = replace_string(
                    qc_config, f"CODE_VALUE_{ind_str}", str(indicator["code"])
                )
                replace = indicator.get("description", "")
                qc_config = replace_string(
                    qc_config, f"DESCRIPTION_VALUE_{ind_str}", replace
                )
                qc_config = replace_string(
                    qc_config, f"TITLE_VALUE_{ind_str}", indicator["title"]
                )
                replace = indicator.get("unit", "")
                qc_config = replace_string(qc_config, f"UNIT_VALUE_{ind_str}", replace)
                qc_config = replace_col(
                    qc_config, f"CODE_COL_{ind_str}", indicator, "code_col"
                )
                qc_config = replace_col(
                    qc_config, f"VALUE_COL_{ind_str}", indicator, "value_col"
                )
                qc_config = replace_col(
                    qc_config, f"DATE_COL_{ind_str}", indicator, "date_col"
                )
                qc_config = replace_col(
                    qc_config,
                    f"DATE_FORMAT_{ind_str}",
                    indicator,
                    "date_format",
                )
                qc_config = replace_col(
                    qc_config,
                    f"AGGREGATE_COL_{ind_str}",
                    indicator,
                    "aggregate_col",
                    True,
                )
                indicators_notexist[index] = False
                return qc_config

            if indicators[0]:
                hxl_preview_config = replace_indicator(hxl_preview_config, 0)
            if len_indicators > 1 and indicators[1]:
                hxl_preview_config = replace_indicator(hxl_preview_config, 1)
            if len_indicators > 2 and indicators[2]:
                hxl_preview_config = replace_indicator(hxl_preview_config, 2)
            if indicators_notexist == [True, True, True]:
                return None
        hxl_preview_config = json.loads(hxl_preview_config)
        bites = hxl_preview_config["bites"]
        for i, disable in reversed(list(enumerate(bites_disabled))):
            if disable or indicators_notexist[i]:
                del bites[i]
        hxl_preview_config = json.dumps(
            hxl_preview_config, indent=None, separators=(",", ":")
        )
        if findreplace:
            for find in findreplace:
                hxl_preview_config = replace_string(
                    hxl_preview_config, find, findreplace[find]
                )
        resourceview["hxl_preview_config"] = hxl_preview_config

        if "resource_id" in resourceview:
            resourceview.create_in_hdx()
            self.preview_resourceview = None
        else:
            self.preview_resourceview = resourceview
        return resourceview

    def generate_quickcharts(
        self,
        resource: Union["Resource", Dict, str, int] = 0,
        path: Optional[str] = None,
        bites_disabled: Optional[ListTuple[bool]] = None,
        indicators: Optional[ListTuple[Dict]] = None,
        findreplace: Optional[Dict] = None,
    ) -> resource_view.ResourceView:
        """Create QuickCharts for the given resource in a dataset. If you do
        not supply a path, then the internal indicators resource view template
        will be used. You can disable specific bites by providing
        bites_disabled, a list of 3 bools where True indicates a specific bite
        is disabled and False indicates leave enabled. The parameter indicators
        is a list with 3 dictionaries of form:
        {"code": "MY_INDICATOR_CODE", "title": "MY_INDICATOR_TITLE",
        "unit": "MY_INDICATOR_UNIT"}. Optionally, the following defaults can be
        overridden in the parameter indicators: {"code_col": "#indicator+code",
        "value_col": "#indicator+value+num", "date_col": "#date+year",
        "date_format": "%Y", "aggregate_col": "null"}.

        Creation of the resource view will be delayed until after the next
        dataset create or update if a resource id is not yet available and will
        be disabled if there are no valid charts to display.

        Args:
            resource (Union[Resource,Dict,str,int]): Either resource id or name, resource metadata from a Resource object or a dictionary or position. Defaults to 0.
            path (Optional[str]): Path to YAML resource view metadata. Defaults to None (config/hdx_resource_view_static.yaml or internal template).
            bites_disabled (Optional[ListTuple[bool]]): Which QC bites should be disabled. Defaults to None (all bites enabled).
            indicators (Optional[ListTuple[Dict]]): Indicator codes, QC titles and units for resource view template. Defaults to None (don't use template).
            findreplace (Optional[Dict]): Replacements for anything else in resource view. Defaults to None.

        Returns:
            resource_view.ResourceView: The resource view if QuickCharts created, None is not
        """
        resourceview = self._generate_resource_view(
            resource, path, bites_disabled, indicators, findreplace=findreplace
        )
        if resourceview is None:
            self.preview_off()
        return resourceview

    def get_name_or_id(self, prefer_name: bool = True) -> Optional[str]:
        """Get dataset name or id eg. for use in urls. If prefer_name is True,
        name is preferred over id if available, otherwise id is preferred over
        name if available.

        Args:
            prefer_name (bool): Whether name is preferred over id. Default to True.

        Returns:
            Optional[str]: HDX dataset id or name or None if not available
        """
        id = self.data.get("id")
        name = self.data.get("name")
        if prefer_name:
            if name:
                return name
            return id
        else:
            if id:
                return id
            return name

    def get_hdx_url(self, prefer_name: bool = True) -> Optional[str]:
        """Get the url of the dataset on HDX or None if the dataset name and
        id fields are missing. If prefer_name is True, name is preferred over
        id if available, otherwise id is preferred over name if available.

        Args:
            prefer_name (bool): Whether name is preferred over id in url. Default to True.

        Returns:
            Optional[str]: Url of the dataset on HDX or None if the dataset is missing fields
        """
        name_or_id = self.get_name_or_id(prefer_name)
        if not name_or_id:
            return None
        return f"{self.configuration.get_hdx_site_url()}/dataset/{name_or_id}"

    def get_api_url(self, prefer_name: bool = True) -> Optional[str]:
        """Get the API url of the dataset on HDX

        Args:
            prefer_name (bool): Whether name is preferred over id in url. Default to True.

        Returns:
            Optional[str]: API url of the dataset on HDX or None if the dataset is missing fields
        """
        name_or_id = self.get_name_or_id(prefer_name)
        if not name_or_id:
            return None
        return f"{self.configuration.get_hdx_site_url()}/api/3/action/package_show?id={name_or_id}"

    def remove_dates_from_title(
        self, change_title: bool = True, set_time_period: bool = False
    ) -> List[Tuple[datetime, datetime]]:
        """Remove dates from dataset title returning sorted the dates that were found in
        title. The title in the dataset metadata will be changed by default. The
        dataset's metadata field time period will not be changed by default, but if
        set_time_period is True, then the range with the lowest start date will be used
        to set the time period field.

        Args:
            change_title (bool): Whether to change the dataset title. Defaults to True.
            set_time_period (bool): Whether to set time period from date or range in title. Defaults to False.

        Returns:
            List[Tuple[datetime,datetime]]: Date ranges found in title
        """
        if "title" not in self.data:
            raise HDXError("Dataset has no title!")
        title = self.data["title"]
        newtitle, ranges = DatasetTitleHelper.get_dates_from_title(title)
        if change_title:
            self.data["title"] = newtitle
        if set_time_period and len(ranges) != 0:
            startdate, enddate = ranges[0]
            self.set_time_period(startdate, enddate)
        return ranges

    def generate_resource_from_rows(
        self,
        folder: str,
        filename: str,
        rows: List[ListTupleDict],
        resourcedata: Dict,
        headers: Optional[ListTuple[str]] = None,
        encoding: Optional[str] = None,
    ) -> "Resource":
        """Write rows to csv and create resource, adding it to the dataset.
        The headers argument is either a row number (rows start counting at
        1), or the actual headers defined as a list of strings. If not set, all
        rows will be treated as containing values.

        Args:
            folder (str): Folder to which to write file containing rows
            filename (str): Filename of file to write rows
            rows (List[ListTupleDict]): List of rows in dict or list form
            resourcedata (Dict): Resource data
            headers (Optional[ListTuple[str]]): List of headers. Defaults to None.
            encoding (Optional[str]): Encoding to use. Defaults to None (infer encoding).

        Returns:
            Resource: The created resource
        """
        filepath = join(folder, filename)
        write_list_to_csv(filepath, rows, columns=headers, encoding=encoding)
        resource = res_module.Resource(resourcedata)
        resource.set_format("csv")
        resource.set_file_to_upload(filepath)
        self.add_update_resource(resource)
        return resource

    def generate_qc_resource_from_rows(
        self,
        folder: str,
        filename: str,
        rows: List[Dict],
        resourcedata: Dict,
        hxltags: Dict[str, str],
        columnname: str,
        qc_identifiers: ListTuple[str],
        headers: Optional[ListTuple[str]] = None,
        encoding: Optional[str] = None,
    ) -> Optional["Resource"]:
        """Generate QuickCharts rows by cutting down input rows by relevant
        identifiers and optionally restricting to certain columns. Output to
        csv and create resource, adding it to the dataset.

        Args:
            folder (str): Folder to which to write file containing rows
            filename (str): Filename of file to write rows
            rows (List[Dict]): List of rows in dict form
            resourcedata (Dict): Resource data
            hxltags (Dict[str,str]): Header to HXL hashtag mapping
            columnname (str): Name of column containing identifier
            qc_identifiers (ListTuple[str]): List of ids to match
            headers (Optional[ListTuple[str]]): List of headers to output. Defaults to None (all headers).
            encoding (Optional[str]): Encoding to use. Defaults to None (infer encoding).

        Returns:
            Optional[Resource]: The created resource or None
        """
        qc_rows = []
        for row in rows:
            if row[columnname] in qc_identifiers:
                if headers:
                    qcrow = {x: row[x] for x in headers}
                else:
                    qcrow = row
                qc_rows.append(qcrow)
        if len(qc_rows) == 0:
            return None
        qc_rows.insert(0, hxltags)
        return self.generate_resource_from_rows(
            folder,
            filename,
            qc_rows,
            resourcedata,
            headers=headers,
            encoding=encoding,
        )

    def generate_resource_from_iterable(
        self,
        headers: ListTuple[str],
        iterable: Iterable[Union[ListTuple, Dict]],
        hxltags: Dict[str, str],
        folder: str,
        filename: str,
        resourcedata: Dict,
        datecol: Optional[Union[int, str]] = None,
        yearcol: Optional[Union[int, str]] = None,
        date_function: Optional[Callable[[Dict], Optional[Dict]]] = None,
        quickcharts: Optional[Dict] = None,
        encoding: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        """Given headers and an iterable, write rows to csv and create
        resource, adding to it the dataset. The returned dictionary will
        contain the resource in the key resource, headers in the key headers
        and list of rows in the key rows.

        The time period can optionally be set by supplying a column in
        which the date or year is to be looked up. Note that any timezone
        information is ignored and UTC assumed. Alternatively, a function can
        be supplied to handle any dates in a row. It should accept a row and
        should return None to ignore the row or a dictionary which can either
        be empty if there are no dates in the row or can be populated with
        keys startdate and/or enddate which are of type timezone-aware
        datetime. The lowest start date and highest end date are used to set
        the time period and are returned in the results dictionary in keys
        startdate and enddate.

        If the parameter quickcharts is supplied then various QuickCharts
        related actions will occur depending upon the keys given in the
        dictionary and the returned dictionary will contain the QuickCharts
        resource in the key qc_resource. If the keys: hashtag - the HXL hashtag
        to examine - and values - the 3 values to look for in that column - are
        supplied, then a list of booleans indicating which QuickCharts bites
        should be enabled will be returned in the key bites_disabled in the
        returned dictionary. For the 3 values, if the key: numeric_hashtag is
        supplied then if that column for a given value contains no numbers,
        then the corresponding bite will be disabled. If the key: cutdown is
        given, if it is 1, then a separate cut down list is created containing
        only columns with HXL hashtags and rows with desired values (if hashtag
        and values are supplied) for the purpose of driving QuickCharts. It is
        returned in the key qcrows in the returned dictionary with the matching
        headers in qcheaders. If cutdown is 2, then a resource is created using
        the cut down list. If the key cutdownhashtags is supplied, then only
        the provided hashtags are used for cutting down otherwise the full list
        of HXL tags is used.

        Args:
            headers (ListTuple[str]): Headers
            iterable (Iterable[Union[ListTuple,Dict]]): Iterable returning rows
            hxltags (Dict[str,str]): Header to HXL hashtag mapping
            folder (str): Folder to which to write file containing rows
            filename (str): Filename of file to write rows
            resourcedata (Dict): Resource data
            datecol (Optional[Union[int,str]]): Date column for setting time period. Defaults to None (don't set).
            yearcol (Optional[Union[int,str]]): Year column for setting dataset year range. Defaults to None (don't set).
            date_function (Optional[Callable[[Dict],Optional[Dict]]]): Date function to call for each row. Defaults to None.
            quickcharts (Optional[Dict]): Dictionary containing optional keys: hashtag, values, cutdown and/or cutdownhashtags
            encoding (Optional[str]): Encoding to use. Defaults to None (infer encoding).

        Returns:
            Tuple[bool, Dict]: (True if resource added, dictionary of results)
        """
        if [datecol, yearcol, date_function].count(None) < 2:
            raise HDXError("Supply one of datecol, yearcol or date_function!")
        retdict = {}
        if headers is None:
            return False, retdict
        rows = [Download.hxl_row(headers, hxltags, dict_form=True)]
        dates = [default_enddate, default_date]
        qc = {"cutdown": 0}
        bites_disabled = [True, True, True]
        if quickcharts is not None:
            hashtag = quickcharts.get("hashtag")
            if hashtag:
                qc["column"] = next(
                    key for key, value in hxltags.items() if value == hashtag
                )  # reverse dict lookup
            else:
                qc["column"] = None
            numeric_hashtag = quickcharts.get("numeric_hashtag")
            if numeric_hashtag:
                qc["numeric"] = next(
                    key for key, value in hxltags.items() if value == numeric_hashtag
                )  # reverse lookup
            else:
                qc["numeric"] = None
            cutdown = quickcharts.get("cutdown", 0)
            if cutdown:
                qc["cutdown"] = cutdown
                cutdownhashtags = quickcharts.get("cutdownhashtags")
                if cutdownhashtags is None:
                    cutdownhashtags = list(hxltags.keys())
                else:
                    cutdownhashtags = [
                        key
                        for key, value in hxltags.items()
                        if value in cutdownhashtags
                    ]
                if numeric_hashtag and qc["numeric"] not in cutdownhashtags:
                    cutdownhashtags.append(qc["numeric"])
                qc["headers"] = [x for x in headers if x in cutdownhashtags]
                qc["rows"] = [Download.hxl_row(qc["headers"], hxltags, dict_form=True)]

        if yearcol is not None:

            def yearcol_function(row):
                result = {}
                year = row[yearcol]
                if year:
                    result["startdate"], result["enddate"] = parse_date_range(
                        year,
                        zero_time=True,
                        max_endtime=True,
                    )
                return result

            date_function = yearcol_function
        elif datecol is not None:

            def datecol_function(row):
                result = {}
                date = row[datecol]
                if date:
                    date = parse_date(date)
                    result["startdate"] = date
                    result["enddate"] = date
                return result

            date_function = datecol_function

        for row in iterable:
            if date_function is not None:
                result = date_function(row)
                if result is None:
                    continue
                startdate = result.get("startdate")
                if startdate is not None:
                    if startdate < dates[0]:
                        dates[0] = startdate
                enddate = result.get("enddate")
                if enddate is not None:
                    if enddate > dates[1]:
                        dates[1] = enddate
            rows.append(row)
            if quickcharts is not None:
                if qc["column"] is None:
                    if qc["cutdown"]:
                        qcrow = {x: row[x] for x in qc["headers"]}
                        qc["rows"].append(qcrow)
                else:
                    value = row[qc["column"]]
                    for i, lookup in enumerate(quickcharts["values"]):
                        if value == lookup:
                            if qc["numeric"]:
                                try:
                                    float(row[qc["numeric"]])
                                except (TypeError, ValueError):
                                    continue
                            bites_disabled[i] = False
                            if qc["cutdown"]:
                                qcrow = {x: row[x] for x in qc["headers"]}
                                qc["rows"].append(qcrow)
        if len(rows) == 1:
            logger.error(f"No data rows in {filename}!")
            return False, retdict
        if yearcol is not None or date_function is not None:
            if dates[0] == default_enddate or dates[1] == default_date:
                logger.error(f"No dates in {filename}!")
                return False, retdict
            else:
                retdict["startdate"] = dates[0]
                retdict["enddate"] = dates[1]
                self.set_time_period(dates[0], dates[1])
        resource = self.generate_resource_from_rows(
            folder,
            filename,
            rows,
            resourcedata,
            headers=headers,
            encoding=encoding,
        )
        retdict["resource"] = resource
        retdict["headers"] = headers
        retdict["rows"] = rows
        if quickcharts is not None:
            retdict["bites_disabled"] = bites_disabled
            if qc["cutdown"]:
                retdict["qcheaders"] = qc["headers"]
                retdict["qcrows"] = qc["rows"]
                if qc["cutdown"] == 2:
                    qc_resourcedata = {
                        "name": f"QuickCharts-{resourcedata['name']}",
                        "description": "Cut down data for QuickCharts",
                    }
                    resource = self.generate_resource_from_rows(
                        folder,
                        f"qc_{filename}",
                        qc["rows"],
                        qc_resourcedata,
                        headers=qc["headers"],
                        encoding=encoding,
                    )
                    retdict["qc_resource"] = resource
        return True, retdict

    def generate_resource_from_iterator(
        self,
        headers: ListTuple[str],
        iterator: Iterator[Union[ListTuple, Dict]],
        hxltags: Dict[str, str],
        folder: str,
        filename: str,
        resourcedata: Dict,
        datecol: Optional[Union[int, str]] = None,
        yearcol: Optional[Union[int, str]] = None,
        date_function: Optional[Callable[[Dict], Optional[Dict]]] = None,
        quickcharts: Optional[Dict] = None,
        encoding: Optional[str] = None,
    ) -> Tuple[bool, Dict]:
        warnings.warn(
            "generate_resource_from_iterator() is deprecated, use generate_resource_from_iterable() instead",
            DeprecationWarning,
        )
        return self.generate_resource_from_iterable(
            headers,
            iterator,
            hxltags,
            folder,
            filename,
            resourcedata,
            datecol,
            yearcol,
            date_function,
            quickcharts,
            encoding,
        )

    def download_and_generate_resource(
        self,
        downloader: BaseDownload,
        url: str,
        hxltags: Dict[str, str],
        folder: str,
        filename: str,
        resourcedata: Dict,
        header_insertions: Optional[ListTuple[Tuple[int, str]]] = None,
        row_function: Optional[Callable[[List[str], Dict], Dict]] = None,
        datecol: Optional[str] = None,
        yearcol: Optional[str] = None,
        date_function: Optional[Callable[[Dict], Optional[Dict]]] = None,
        quickcharts: Optional[Dict] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Dict]:
        """Download url, write rows to csv and create resource, adding to it
        the dataset. The returned dictionary will contain the resource in the
        key resource, headers in the key headers and list of rows in the key
        rows.

        Optionally, headers can be inserted at specific positions. This is
        achieved using the header_insertions argument. If supplied, it is a
        list of tuples of the form (position, header) to be inserted. A
        function is called for each row. If supplied, it takes as arguments:
        headers (prior to any insertions) and row (which will be in dict or
        list form depending upon the dict_rows argument) and outputs a modified
        row.

        The time period can optionally be set by supplying a column in
        which the date or year is to be looked up. Note that any timezone
        information is ignored and UTC assumed. Alternatively, a function can
        be supplied to handle any dates in a row. It should accept a row and
        should return None to ignore the row or a dictionary which can either
        be empty if there are no dates in the row or can be populated with
        keys startdate and/or enddate which are of type timezone-aware
        datetime. The lowest start date and highest end date are used to set
        the time period and are returned in the results dictionary in keys
        startdate and enddate.

        If the parameter quickcharts is supplied then various QuickCharts
        related actions will occur depending upon the keys given in the
        dictionary and the returned dictionary will contain the QuickCharts
        resource in the key qc_resource. If the keys: hashtag - the HXL hashtag
        to examine - and values - the 3 values to look for in that column - are
        supplied, then a list of booleans indicating which QuickCharts bites
        should be enabled will be returned in the key bites_disabled in the
        returned dictionary. For the 3 values, if the key: numeric_hashtag is
        supplied then if that column for a given value contains no numbers,
        then the corresponding bite will be disabled. If the key: cutdown is
        given, if it is 1, then a separate cut down list is created containing
        only columns with HXL hashtags and rows with desired values (if hashtag
        and values are supplied) for the purpose of driving QuickCharts. It is
        returned in the key qcrows in the returned dictionary with the matching
        headers in qcheaders. If cutdown is 2, then a resource is created using
        the cut down list. If the key cutdownhashtags is supplied, then only
        the provided hashtags are used for cutting down otherwise the full list
        of HXL tags is used.

        Args:
            downloader (BaseDownload): A Download or Retrieve object
            url (str): URL to download
            hxltags (Dict[str,str]): Header to HXL hashtag mapping
            folder (str): Folder to which to write file containing rows
            filename (str): Filename of file to write rows
            resourcedata (Dict): Resource data
            header_insertions (Optional[ListTuple[Tuple[int,str]]]): List of (position, header) to insert. Defaults to None.
            row_function (Optional[Callable[[List[str],Dict],Dict]]): Function to call for each row. Defaults to None.
            datecol (Optional[str]): Date column for setting time period. Defaults to None (don't set).
            yearcol (Optional[str]): Year column for setting dataset year range. Defaults to None (don't set).
            date_function (Optional[Callable[[Dict],Optional[Dict]]]): Date function to call for each row. Defaults to None.
            quickcharts (Optional[Dict]): Dictionary containing optional keys: hashtag, values, cutdown and/or cutdownhashtags
            **kwargs: Any additional args to pass to downloader.get_tabular_rows

        Returns:
            Tuple[bool, Dict]: (True if resource added, dictionary of results)
        """
        headers, iterator = downloader.get_tabular_rows(
            url,
            dict_form=True,
            header_insertions=header_insertions,
            row_function=row_function,
            format="csv",
            **kwargs,
        )
        return self.generate_resource_from_iterable(
            headers,
            iterator,
            hxltags,
            folder,
            filename,
            resourcedata,
            datecol=datecol,
            yearcol=yearcol,
            date_function=date_function,
            quickcharts=quickcharts,
            encoding=kwargs.get("encoding", None),
        )

    def add_hapi_error(
        self,
        error_message: str,
        resource_name: Optional[str] = None,
        resource_id: Optional[str] = None,
    ) -> bool:
        """Writes error messages that were uncovered while processing data for
        the HAPI database to a resource's metadata on HDX. If the resource
        already has an error message, it is only overwritten if the two
        messages are different.

        Args:
            error_message (str): Error(s) uncovered
            resource_name (Optional[str]): Resource name. Defaults to None
            resource_id (Optional[str]): Resource id. Defaults to None

        Returns:
            bool: True if a message was added, False if not
        """
        if resource_name is None and resource_id is None:
            return False
        resource = None
        for res in self.get_resources():
            if res["name"] == resource_name or res["id"] == resource_id:
                resource = res
                break
        if not resource:
            return False
        resource_error = resource.get("qa_hapi_report")
        if resource_error and resource_error == error_message:
            return False
        resource["qa_hapi_report"] = error_message
        resource.update_in_hdx(operation="patch")
        return True
