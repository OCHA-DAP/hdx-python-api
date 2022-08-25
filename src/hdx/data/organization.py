"""Organization class containing all logic for creating, checking, and updating organizations."""
import logging
from os.path import join
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from hdx.utilities.typehint import ListTuple

import hdx.data.dataset
import hdx.data.user as user_module
from hdx.api.configuration import Configuration
from hdx.data.hdxobject import HDXError, HDXObject

if TYPE_CHECKING:
    from hdx.data.user import User

logger = logging.getLogger(__name__)


class Organization(HDXObject):
    """Organization class containing all logic for creating, checking, and updating organizations.

    Args:
        initial_data (Optional[Dict]): Initial organization metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(
        self,
        initial_data: Optional[Dict] = None,
        configuration: Optional[Configuration] = None,
    ) -> None:
        if not initial_data:
            initial_data = dict()
        super().__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions() -> Dict[str, str]:
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            "show": "organization_show",
            "update": "organization_update",
            "create": "organization_create",
            "delete": "organization_delete",
            "list": "organization_list",
            "autocomplete": "organization_autocomplete",
        }

    def update_from_yaml(
        self, path: str = join("config", "hdx_organization_static.yml")
    ) -> None:
        """Update organization metadata with static metadata from YAML file

        Args:
            path (str): Path to YAML dataset metadata. Defaults to config/hdx_organization_static.yml.

        Returns:
            None
        """
        super().update_from_yaml(path)

    def update_from_json(
        self, path: str = join("config", "hdx_organization_static.json")
    ) -> None:
        """Update organization metadata with static metadata from JSON file

        Args:
            path (str): Path to JSON dataset metadata. Defaults to config/hdx_organization_static.json.

        Returns:
            None
        """
        super().update_from_json(path)

    @classmethod
    def read_from_hdx(
        cls, identifier: str, configuration: Optional[Configuration] = None
    ) -> Optional["Organization"]:
        """Reads the organization given by identifier from HDX and returns Organization object

        Args:
            identifier (str): Identifier of organization
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Organization]: Organization object if successful read, None if not
        """
        return cls._read_from_hdx_class(
            "organization", identifier, configuration
        )

    def check_required_fields(
        self, ignore_fields: ListTuple[str] = tuple()
    ) -> None:
        """Check that metadata for organization is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (ListTuple[str]): Fields to ignore. Default is tuple().

        Returns:
            None
        """
        self._check_required_fields("organization", ignore_fields)

    def update_in_hdx(self, **kwargs: Any) -> None:
        """Check if organization exists in HDX and if so, update organization

        Returns:
            None
        """
        self._update_in_hdx("organization", "id", **kwargs)

    def create_in_hdx(self, **kwargs: Any) -> None:
        """Check if organization exists in HDX and if so, update it, otherwise create organization

        Returns:
            None
        """
        self._create_in_hdx("organization", "id", "name", **kwargs)

    def delete_from_hdx(self) -> None:
        """Deletes a organization from HDX.

        Returns:
            None
        """
        self._delete_from_hdx("organization", "id")

    def get_users(self, capacity: Optional[str] = None) -> List["User"]:
        """Returns the organization's users.

        Args:
            capacity (Optional[str]): Filter by capacity eg. member, admin. Defaults to None.
        Returns:
            List[User]: Organization's users.
        """
        users = list()
        usersdicts = self.data.get("users")
        if usersdicts is not None:
            for userdata in usersdicts:
                if capacity is not None and userdata["capacity"] != capacity:
                    continue
                id = userdata.get("id")
                if id is None:
                    id = userdata["name"]
                user = user_module.User.read_from_hdx(
                    id, configuration=self.configuration
                )
                user["capacity"] = userdata["capacity"]
                users.append(user)
        return users

    def add_update_user(
        self,
        user: Union["User", Dict, str],
        capacity: Optional[str] = None,
    ) -> None:
        """Add new or update existing user in organization with new metadata. Capacity eg. member, admin
        must be supplied either within the User object or dictionary or using the capacity argument (which takes
        precedence).

        Args:
            user (Union[User,Dict,str]): Either a user id or user metadata either from a User object or a dictionary
            capacity (Optional[str]): Capacity of user eg. member, admin. Defaults to None.

        Returns:
            None

        """
        if isinstance(user, str):
            user = user_module.User.read_from_hdx(
                user, configuration=self.configuration
            )
        elif isinstance(user, dict):
            user = user_module.User(user, configuration=self.configuration)
        if isinstance(user, user_module.User):
            users = self.data.get("users")
            if users is None:
                users = list()
                self.data["users"] = users
            if capacity is not None:
                user["capacity"] = capacity
            self._addupdate_hdxobject(users, "name", user)
            return
        raise HDXError(
            f"Type {type(user).__name__} cannot be added as a user!"
        )

    def add_update_users(
        self,
        users: ListTuple[Union["User", Dict, str]],
        capacity: Optional[str] = None,
    ) -> None:
        """Add new or update existing users in organization with new metadata. Capacity eg. member, admin
        must be supplied either within the User object or dictionary or using the capacity argument (which takes
        precedence).

        Args:
            users (ListTuple[Union[User,Dict,str]]): A list of either user ids or users metadata from User objects or dictionaries
            capacity (Optional[str]): Capacity of users eg. member, admin. Defaults to None.

        Returns:
            None
        """
        for user in users:
            self.add_update_user(user, capacity)

    def remove_user(self, user: Union["User", Dict, str]) -> bool:
        """Remove a user from the organization

        Args:
            user (Union[User,Dict,str]): Either a user id or user metadata either from a User object or a dictionary

        Returns:
            bool: True if user removed or False if not
        """
        return self._remove_hdxobject(self.data.get("users"), user)

    def get_datasets(
        self, query: str = "*:*", **kwargs: Any
    ) -> List["Dataset"]:  # noqa: F821
        """Get list of datasets in organization

        Args:
            query (str): Restrict datasets returned to this query (in Solr format). Defaults to '*:*'.
            **kwargs: See below
            sort (string): Sorting of the search results. Defaults to 'relevance asc, metadata_modified desc'.
            rows (int): Number of matching rows to return. Defaults to all datasets (sys.maxsize).
            start (int): Offset in the complete result for where the set of returned datasets should begin
            facet (string): Whether to enable faceted results. Default to True.
            facet.mincount (int): Minimum counts for facet fields should be included in the results
            facet.limit (int): Maximum number of values the facet fields return (- = unlimited). Defaults to 50.
            facet.field (List[str]): Fields to facet upon. Default is empty.
            use_default_schema (bool): Use default package schema instead of custom schema. Defaults to False.

        Returns:
            List[Dataset]: List of datasets in organization
        """
        return hdx.data.dataset.Dataset.search_in_hdx(
            query=query,
            configuration=self.configuration,
            fq=f"organization:{self.data['name']}",
            **kwargs,
        )

    @staticmethod
    def get_all_organization_names(
        configuration: Optional[Configuration] = None, **kwargs: Any
    ) -> List[str]:
        """Get all organization names in HDX

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            **kwargs: See below
            sort (str): Sort the search results according to field name and sort-order. Allowed fields are ‘name’, ‘package_count’ and ‘title’. Defaults to 'name asc'.
            organizations (List[str]): List of names of the groups to return.
            all_fields (bool): Return group dictionaries instead of just names. Only core fields are returned - get some more using the include_* options. Defaults to False.
            include_extras (bool): If all_fields, include the group extra fields. Defaults to False.
            include_tags (bool): If all_fields, include the group tags. Defaults to False.
            include_groups: If all_fields, include the groups the groups are in. Defaults to False.
            include_users (bool): If all_fields, include the organization users. Defaults to False.

        Returns:
            List[str]: List of all organization names in HDX
        """
        organization = Organization(configuration=configuration)
        return organization._write_to_hdx("list", kwargs)

    @classmethod
    def autocomplete(
        cls,
        name: str,
        limit: int = 20,
        configuration: Optional[Configuration] = None,
    ) -> List:
        """Autocomplete an organization name and return matches

        Args:
            name (str): Name to autocomplete
            limit (int): Maximum number of matches to return
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List: Autocomplete matches
        """
        return cls._autocomplete(name, limit, configuration)
