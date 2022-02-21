"""HDXObject abstract class containing helper functions for creating, checking, and updating HDX objects.
New HDX objects should extend this in similar fashion to Resource for example.
"""
import copy
import logging
from abc import ABC, abstractmethod
from collections import UserDict
from typing import Any, Dict, List, Optional, Tuple, Union

from ckanapi.errors import NotFound
from hdx.utilities.dictandlist import merge_two_dictionaries
from hdx.utilities.loader import (
    load_json_into_existing_dict,
    load_yaml_into_existing_dict,
)

from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)


class HDXError(Exception):
    pass


class HDXObject(UserDict, ABC):
    """HDXObject abstract class containing helper functions for creating, checking, and updating HDX objects.
    New HDX objects should extend this in similar fashion to Resource for example.

    Args:
        initial_data (Dict): Initial metadata dictionary
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    @staticmethod
    @abstractmethod
    def actions() -> Dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """

    def __init__(
        self, initial_data: Dict, configuration: Optional[Configuration] = None
    ) -> None:
        self.old_data = None
        if configuration is None:
            self.configuration: Configuration = Configuration.read()
        else:
            self.configuration: Configuration = configuration
        super().__init__(initial_data)

    def get_old_data_dict(self) -> None:
        """Get previous internal dictionary

        Returns:
            dict: Previous internal dictionary
        """
        return self.old_data

    def update_from_yaml(self, path: str) -> None:
        """Update metadata with static metadata from YAML file

        Args:
            path (str): Path to YAML dataset metadata

        Returns:
            None
        """
        self.data = load_yaml_into_existing_dict(self.data, path)

    def update_from_json(self, path: str) -> None:
        """Update metadata with static metadata from JSON file

        Args:
            path (str): Path to JSON dataset metadata

        Returns:
            None
        """
        self.data = load_json_into_existing_dict(self.data, path)

    def _read_from_hdx(
        self,
        object_type: str,
        value: str,
        fieldname: str = "id",
        action: Optional[str] = None,
        **kwargs: Any,
    ) -> Tuple[bool, Union[Dict, str]]:
        """Makes a read call to HDX passing in given parameter.

        Args:
            object_type (str): Description of HDX object type (for messages)
            value (str): Value of HDX field
            fieldname (str): HDX field name. Defaults to id.
            action (Optional[str]): Replacement CKAN action url to use. Defaults to None.
            **kwargs: Other fields to pass to CKAN.

        Returns:
            Tuple[bool, Union[Dict, str]]: (True/False, HDX object metadata/Error)
        """
        if not fieldname:
            raise HDXError(f"Empty {object_type} field name!")
        if action is None:
            action = self.actions()["show"]
        data = {fieldname: value}
        data.update(kwargs)
        try:
            result = self.configuration.call_remoteckan(action, data)
            return True, result
        except NotFound:
            return False, f"{fieldname}={value}: not found!"
        except Exception as e:
            raise HDXError(
                f"Failed when trying to read: {fieldname}={value}! (POST)"
            ) from e

    def _load_from_hdx(self, object_type: str, id_field: str) -> bool:
        """Helper method to load the HDX object given by identifier from HDX

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field (str): HDX object identifier

        Returns:
            bool: True if loaded, False if not
        """
        success, result = self._read_from_hdx(object_type, id_field)
        if success:
            self.old_data = self.data
            self.data = result
            return True
        logger.debug(result)
        return False

    @staticmethod
    @abstractmethod
    def read_from_hdx(
        id_field: str, configuration: Optional[Configuration] = None
    ) -> Optional["HDXObject"]:
        """Abstract method to read the HDX object given by identifier from HDX and return it

        Args:
            id_field (str): HDX object identifier
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[HDXObject]: HDX object if successful read, None if not
        """

    @classmethod
    def _read_from_hdx_class(
        cls,
        object_type: str,
        identifier: str,
        configuration: Optional[Configuration] = None,
    ) -> Optional["HDXObject"]:
        """Reads the HDX object given by identifier from HDX and returns it

        Args:
            object_type (str): Description of HDX object type (for messages)
            identifier (str): Identifier
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[HDXObject]: HDX object if successful read, None if not
        """
        hdxobject = cls(configuration=configuration)
        result = hdxobject._load_from_hdx(object_type, identifier)
        if result:
            return hdxobject
        return None

    def _check_existing_object(
        self, object_type: str, id_field_name: str
    ) -> None:
        if not self.data:
            raise HDXError(f"No data in {object_type}!")
        if id_field_name not in self.data:
            raise HDXError(
                f"No {id_field_name} field (mandatory) in {object_type}!"
            )

    def _check_load_existing_object(
        self, object_type: str, id_field_name: str, operation: str = "update"
    ) -> None:
        """Check metadata exists and contains HDX object identifier, and if so load HDX object

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier
            operation (str): Operation to report if error. Defaults to update.

        Returns:
            None
        """
        self._check_existing_object(object_type, id_field_name)
        if not self._load_from_hdx(object_type, self.data[id_field_name]):
            raise HDXError(f"No existing {object_type} to {operation}!")

    @abstractmethod
    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Abstract method to check that metadata for HDX object is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """

    def _check_required_fields(
        self, object_type: str, ignore_fields: List[str]
    ) -> None:
        """Helper method to check that metadata for HDX object is complete

        Args:
            ignore_fields (List[str]): Any fields to ignore in the check

        Returns:
            None
        """
        for field in self.configuration[object_type]["required_fields"]:
            if field not in ignore_fields:
                if field not in self.data:
                    raise HDXError(
                        f"Field {field} is missing in {object_type}!"
                    )
                if not self.data[field] and not isinstance(
                    self.data[field], bool
                ):
                    raise HDXError(f"Field {field} is empty in {object_type}!")

    def _check_kwargs_fields(self, object_type: str, **kwargs: Any) -> None:
        """Helper method to check kwargs and set fields appropriately and to check metadata fields unless it is
        specified not to do so.

        Args:
            object_type (str): Description of HDX object type (for messages)
            **kwargs: See below
            ignore_field (str): Any field to ignore when checking dataset metadata. Defaults to None.

        Returns:
            None
        """
        if (
            "batch_mode" in kwargs
        ):  # Whether or not CKAN should change groupings of datasets on /datasets page
            self.data["batch_mode"] = kwargs["batch_mode"]
        if (
            "skip_validation" in kwargs
        ):  # Whether or not CKAN should perform validation steps (checking fields present)
            self.data["skip_validation"] = kwargs["skip_validation"]
        if "ignore_check" not in kwargs or not kwargs.get(
            "ignore_check"
        ):  # allow ignoring of field checks
            ignore_fields = kwargs.get("ignore_fields", list())
            ignore_field = self.configuration[object_type].get(
                "ignore_on_update"
            )
            if ignore_field and ignore_field not in ignore_fields:
                ignore_fields.append(ignore_field)
            ignore_field = kwargs.get("ignore_field")
            if ignore_field and ignore_field not in ignore_fields:
                ignore_fields.append(ignore_field)
            self.check_required_fields(ignore_fields=ignore_fields)

    def _hdx_update(
        self,
        object_type: str,
        id_field_name: str,
        files_to_upload: Dict = dict(),
        force_active: bool = False,
        **kwargs: Any,
    ) -> None:
        """Helper method to update HDX object

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier
            files_to_upload (Dict): Files to upload to HDX
            force_active (bool): Make object state active. Defaults to False.
            **kwargs: See below
            operation (str): Operation to perform eg. patch. Defaults to update.
            ignore_field (str): Any field to ignore when checking dataset metadata. Defaults to None.

        Returns:
            None
        """
        self._check_kwargs_fields(object_type, **kwargs)
        operation = kwargs.get("operation", "update")
        self._save_to_hdx(
            operation, id_field_name, files_to_upload, force_active
        )

    def _merge_hdx_update(
        self,
        object_type: str,
        id_field_name: str,
        files_to_upload: Dict = dict(),
        force_active: bool = False,
        **kwargs: Any,
    ) -> None:
        """Helper method to check if HDX object exists and update it

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier
            files_to_upload (Dict): Files to upload to HDX
            force_active (bool): Make object state active. Defaults to False.
            **kwargs: See below
            operation (str): Operation to perform eg. patch. Defaults to update.
            ignore_field (str): Any field to ignore when checking dataset metadata. Defaults to None.

        Returns:
            None
        """
        merge_two_dictionaries(self.data, self.old_data)
        self._hdx_update(
            object_type,
            id_field_name,
            files_to_upload=files_to_upload,
            force_active=force_active,
            **kwargs,
        )

    @abstractmethod
    def update_in_hdx(self) -> None:
        """Abstract method to check if HDX object exists in HDX and if so, update it

        Returns:
            None
        """

    def _update_in_hdx(
        self,
        object_type: str,
        id_field_name: str,
        files_to_upload: Dict = dict(),
        force_active: bool = True,
        **kwargs: Any,
    ) -> None:
        """Helper method to check if HDX object exists in HDX and if so, update it

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier
            files_to_upload (Dict): Files to upload to HDX
            force_active (bool): Make object state active. Defaults to True.
            **kwargs: See below
            operation (str): Operation to perform eg. patch. Defaults to update.
            ignore_field (str): Any field to ignore when checking dataset metadata. Defaults to None.

        Returns:
            None
        """

        self._check_load_existing_object(object_type, id_field_name)
        # We load an existing object even though it may well have been loaded already
        # to prevent an admittedly unlikely race condition where someone has updated
        # the object in the intervening time
        self._merge_hdx_update(
            object_type,
            id_field_name,
            files_to_upload,
            force_active=force_active,
            **kwargs,
        )

    def _write_to_hdx(
        self,
        action: str,
        data: Dict,
        id_field_name: str = None,
        files_to_upload: Dict = dict(),
    ) -> Union[Dict, List]:
        """Creates or updates an HDX object in HDX and return HDX object metadata dict

        Args:
            action (str): Action to perform eg. 'create', 'update'
            data (Dict): Data to write to HDX
            id_field_name (Optional[str]): Name of field containing HDX object identifier. Defaults to None.
            files_to_upload (Dict): Files to upload to HDX

        Returns:
            Union[Dict,List]: HDX object metadata
        """
        try:
            for key, value in files_to_upload.items():
                files_to_upload[key] = open(value, "rb")
            return self.configuration.call_remoteckan(
                self.actions()[action], data, files=files_to_upload
            )
        except Exception as e:
            if id_field_name:
                idstr = f" {data[id_field_name]}"
            else:
                idstr = ""
            raise HDXError(
                f"Failed when trying to {action}{idstr}! (POST)"
            ) from e
        finally:
            for file in files_to_upload.values():
                if isinstance(file, str):
                    continue
                file.close()

    def _save_to_hdx(
        self,
        action: str,
        id_field_name: str,
        files_to_upload: Dict = dict(),
        force_active: bool = False,
    ) -> None:
        """Creates or updates an HDX object in HDX, saving current data and replacing with returned HDX object data
        from HDX

        Args:
            action (str): Action to perform: 'create' or 'update'
            id_field_name (str): Name of field containing HDX object identifier
            files_to_upload (Dict): Files to upload to HDX
            force_active (bool): Make object state active. Defaults to False.

        Returns:
            None
        """
        if force_active:
            self.data["state"] = "active"
        result = self._write_to_hdx(
            action, self.data, id_field_name, files_to_upload
        )
        self.old_data = self.data
        self.data = result

    @abstractmethod
    def create_in_hdx(self) -> None:
        """Abstract method to check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """

    def _create_in_hdx(
        self,
        object_type: str,
        id_field_name: str,
        name_field_name: str,
        files_to_upload: Dict = dict(),
        force_active: bool = True,
        **kwargs: Any,
    ) -> None:
        """Helper method to check if resource exists in HDX and if so, update it, otherwise create it


        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier
            name_field_name (str): Name of field containing HDX object name
            files_to_upload (Dict): Files to upload to HDX
            force_active (bool): Make object state active. Defaults to True.

        Returns:
            None
        """
        if "ignore_check" not in kwargs:  # allow ignoring of field checks
            self.check_required_fields()
        if id_field_name in self.data and self._load_from_hdx(
            object_type, self.data[id_field_name]
        ):
            logger.warning(
                f"{object_type} exists. Updating {self.data[id_field_name]}"
            )
            self._merge_hdx_update(
                object_type,
                id_field_name,
                files_to_upload,
                force_active,
                **kwargs,
            )
        else:
            self._save_to_hdx(
                "create", name_field_name, files_to_upload, force_active
            )

    @abstractmethod
    def delete_from_hdx(self) -> None:
        """Abstract method to deletes a resource from HDX

        Returns:
            None
        """

    def _delete_from_hdx(self, object_type: str, id_field_name: str) -> None:
        """Helper method to deletes a resource from HDX

        Args:
            object_type (str): Description of HDX object type (for messages)
            id_field_name (str): Name of field containing HDX object identifier

        Returns:
            None
        """
        if id_field_name not in self.data:
            raise HDXError(
                f"No {id_field_name} field (mandatory) in {object_type}!"
            )
        self._save_to_hdx("delete", id_field_name)

    @classmethod
    def _autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Optional[Configuration] = None,
        **kwargs: Any,
    ) -> List:
        """Helper method to autocomplete a name and return matches

        Args:
            name (str): Name to autocomplete
            limit (int): Maximum number of matches to return
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs:
            offset (int): The offset to start returning tags from.

        Returns:
            List: Autocomplete matches
        """
        hdxobject = cls(configuration=configuration)
        data = {"q": name, "limit": limit}
        data.update(kwargs)
        return hdxobject._write_to_hdx("autocomplete", data)

    def _addupdate_hdxobject(
        self,
        hdxobjects: List["HDXObject"],
        id_field: str,
        new_hdxobject: "HDXObject",
    ) -> "HDXObject":
        """Helper function to add a new HDX object to a supplied list of HDX objects or update existing metadata if the object
        already exists in the list

        Args:
            hdxobjects (List[HDXObject]): list of HDX objects to which to add new objects or update existing ones
            id_field (str): Field on which to match to determine if object already exists in list
            new_hdxobject (HDXObject): The HDX object to be added/updated

        Returns:
            HDXObject: The HDX object which was added or updated
        """
        for hdxobject in hdxobjects:
            if hdxobject[id_field] == new_hdxobject[id_field]:
                merge_two_dictionaries(hdxobject, new_hdxobject)
                return hdxobject
        hdxobjects.append(new_hdxobject)
        return new_hdxobject

    def _remove_hdxobject(
        self,
        objlist: List[Union["HDXObject", Dict]],
        obj: Union["HDXObject", Dict, str],
        matchon: str = "id",
        delete: bool = False,
    ) -> bool:
        """Remove an HDX object from a list within the parent HDX object

        Args:
            objlist (List[Union[HDXObject,Dict]]): list of HDX objects
            obj (Union[HDXObject,Dict,str]): Either an id or hdx object metadata either from an HDX object or a dictionary
            matchon (str): Field to match on. Defaults to id.
            delete (bool): Whether to delete HDX object. Defaults to False.

        Returns:
            bool: True if object removed, False if not
        """
        if objlist is None:
            return False
        if isinstance(obj, str):
            obj_id = obj
        elif isinstance(obj, dict) or isinstance(obj, HDXObject):
            obj_id = obj.get(matchon)
        else:
            raise HDXError("Type of object not a string, dict or T<=HDXObject")
        if not obj_id:
            return False
        for i, objdata in enumerate(objlist):
            objid = objdata.get(matchon)
            if objid and objid == obj_id:
                if delete:
                    objlist[i].delete_from_hdx()
                del objlist[i]
                return True
        return False

    def _convert_hdxobjects(self, hdxobjects: List["HDXObject"]) -> List[Dict]:
        """Helper function to convert supplied list of HDX objects to a list of dict

        Args:
            hdxobjects (List[HDXObject]): List of HDX objects to convert

        Returns:
            List[Dict]: List of HDX objects converted to simple dictionaries
        """
        newhdxobjects = list()
        for hdxobject in hdxobjects:
            newhdxobjects.append(hdxobject.data)
        return newhdxobjects

    def _copy_hdxobjects(
        self,
        hdxobjects: List["HDXObject"],
        hdxobjectclass: type,
        attribute_to_copy: Optional[str] = None,
    ) -> List["HDXObject"]:
        """Helper function to make a deep copy of a supplied list of HDX objects

        Args:
            hdxobjects (List[HDXObject]): list of HDX objects to copy
            hdxobjectclass (type): Type of the HDX Objects to be copied
            attribute_to_copy (Optional[str]): An attribute to copy over from the HDX object. Defaults to None.

        Returns:
            List[HDXObject]: Deep copy of list of HDX objects
        """
        newhdxobjects = list()
        for hdxobject in hdxobjects:
            newhdxobjectdata = copy.deepcopy(hdxobject.data)
            newhdxobject = hdxobjectclass(
                newhdxobjectdata, configuration=self.configuration
            )
            if attribute_to_copy:
                value = getattr(hdxobject, attribute_to_copy)
                setattr(newhdxobject, attribute_to_copy, value)
            newhdxobjects.append(newhdxobject)
        return newhdxobjects

    def _separate_hdxobjects(
        self,
        hdxobjects: List["HDXObject"],
        hdxobjects_name: str,
        id_field: str,
        hdxobjectclass: type,
    ) -> None:
        """Helper function to take a list of HDX objects contained in the internal dictionary and add them to a
        supplied list of HDX objects or update existing metadata if any objects already exist in the list. The list in
        the internal dictionary is then deleted.

        Args:
            hdxobjects (List[HDXObject]): list of HDX objects to which to add new objects or update existing ones
            hdxobjects_name (str): Name of key in internal dictionary from which to obtain list of HDX objects
            id_field (str): Field on which to match to determine if object already exists in list
            hdxobjectclass (type): Type of the HDX Object to be added/updated

        Returns:
            None
        """
        new_hdxobjects = self.data.get(hdxobjects_name, list())
        """:type : List["HDXObject"]"""
        if new_hdxobjects:
            hdxobject_names = set()
            for hdxobject in hdxobjects:
                hdxobject_name = hdxobject[id_field]
                hdxobject_names.add(hdxobject_name)
                for new_hdxobject in new_hdxobjects:
                    if hdxobject_name == new_hdxobject[id_field]:
                        merge_two_dictionaries(hdxobject, new_hdxobject)
                        break
            for new_hdxobject in new_hdxobjects:
                if new_hdxobject[id_field] not in hdxobject_names:
                    hdxobjects.append(
                        hdxobjectclass(
                            new_hdxobject, configuration=self.configuration
                        )
                    )
            del self.data[hdxobjects_name]

    def _get_tags(self) -> List[str]:
        """Return the dataset's list of tags

        Returns:
            List[str]: list of tags or [] if there are none
        """
        tags = self.data.get("tags", None)
        if not tags:
            return list()
        return [x["name"] for x in tags]

    def _add_tag(self, tag: str, vocabulary_id: Optional[str] = None) -> bool:
        """Add a tag

        Args:
            tag (str): Tag to add
            vocabulary_id (Optional[str]): Vocabulary tag is in. Defaults to None.

        Returns:
            bool: True if tag added or False if tag already present
        """
        tag = tag.lower()
        tags = self.data.get("tags", None)
        if tags:
            if tag in [x["name"] for x in tags]:
                return False
        else:
            tags = list()
        tagdict = {"name": tag}
        if vocabulary_id is not None:
            tagdict["vocabulary_id"] = vocabulary_id
        tags.append(tagdict)
        self.data["tags"] = tags
        return True

    def _add_tags(
        self, tags: List[str], vocabulary_id: Optional[str] = None
    ) -> List[str]:
        """Add a list of tag

        Args:
            tags (List[str]): list of tags to add
            vocabulary_id (Optional[str]): Vocabulary tag is in. Defaults to None.

        Returns:
            List[str]: Tags that were successfully added
        """
        added_tags = list()
        for tag in tags:
            if self._add_tag(tag, vocabulary_id=vocabulary_id):
                added_tags.append(tag)
        return added_tags

    def _get_stringlist_from_commastring(self, field: str) -> List[str]:
        """Return list of strings from comma separated list

        Args:
            field (str): Field containing comma separated list

        Returns:
            List[str]: List of strings
        """
        strings = self.data.get(field)
        if strings:
            return strings.split(",")
        else:
            return list()

    def _add_string_to_commastring(self, field: str, string: str) -> bool:
        """Add a string to a comma separated list of strings

        Args:
            field (str): Field containing comma separated list
            string (str): String to add

        Returns:
            bool: True if string added or False if string already present
        """
        if string in self._get_stringlist_from_commastring(field):
            return False
        strings = f"{self.data.get(field, '')},{string}"
        if strings[0] == ",":
            strings = strings[1:]
        self.data[field] = strings
        return True

    def _add_strings_to_commastring(
        self, field: str, strings: List[str]
    ) -> bool:
        """Add a list of strings to a comma separated list of strings

        Args:
            field (str): Field containing comma separated list
            strings (List[str]): list of strings to add

        Returns:
            bool: True if all strings added or False if any already present.
        """
        allstringsadded = True
        for string in strings:
            if not self._add_string_to_commastring(field, string):
                allstringsadded = False
        return allstringsadded

    def _remove_string_from_commastring(self, field: str, string: str) -> bool:
        """Remove a string from a comma separated list of strings

        Args:
            field (str): Field containing comma separated list
            string (str): String to remove

        Returns:
            bool: True if string removed or False if not
        """
        commastring = self.data.get(field, "")
        if string in commastring:
            self.data[field] = commastring.replace(string, "")
            return True
        return False
