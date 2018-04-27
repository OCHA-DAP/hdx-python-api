# -*- coding: utf-8 -*-
"""Organization class containing all logic for creating, checking, and updating organizations."""
import logging
from os.path import join
from typing import Optional, List, Dict, Union

import hdx.data.dataset
import hdx.data.user
from hdx.data.hdxobject import HDXObject, HDXError
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class Organization(HDXObject):
    """Organization class containing all logic for creating, checking, and updating organizations.

    Args:
        initial_data (Optional[Dict]): Initial organization metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Organization, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'organization_show',
            'update': 'organization_update',
            'create': 'organization_create',
            'delete': 'organization_delete',
            'list': 'organization_list'
        }

    def update_from_yaml(self, path=join('config', 'hdx_organization_static.yml')):
        # type: (str) -> None
        """Update organization metadata with static metadata from YAML file

        Args:
            path (str): Path to YAML dataset metadata. Defaults to config/hdx_organization_static.yml.

        Returns:
            None
        """
        super(Organization, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_organization_static.json')):
        # type: (str) -> None
        """Update organization metadata with static metadata from JSON file

        Args:
            path (str): Path to JSON dataset metadata. Defaults to config/hdx_organization_static.json.

        Returns:
            None
        """
        super(Organization, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['Organization']
        """Reads the organization given by identifier from HDX and returns Organization object

        Args:
            identifier (str): Identifier of organization
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Organization]: Organization object if successful read, None if not
        """

        organization = Organization(configuration=configuration)
        result = organization._load_from_hdx('organization', identifier)
        if result:
            return organization
        return None

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for organization is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('organization', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if organization exists in HDX and if so, update organization

        Returns:
            None
        """
        self._update_in_hdx('organization', 'id')

    def create_in_hdx(self):
        # type: () -> None
        """Check if organization exists in HDX and if so, update it, otherwise create organization

        Returns:
            None
        """
        self._create_in_hdx('organization', 'id', 'name')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a organization from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('organization', 'id')

    def get_users(self, capacity=None):
        # type: (Optional[str]) -> List[User]
        """Returns the organization's users.

        Args:
            capacity (Optional[str]): Filter by capacity eg. member, admin. Defaults to None.
        Returns:
            List[User]: Organization's users.
        """
        users = list()
        usersdicts = self.data.get('users')
        if usersdicts is not None:
            for userdata in usersdicts:
                if capacity is not None and userdata['capacity'] != capacity:
                    continue
                id = userdata.get('id')
                if id is None:
                    id = userdata['name']
                user = hdx.data.user.User.read_from_hdx(id, configuration=self.configuration)
                user['capacity'] = userdata['capacity']
                users.append(user)
        return users

    def add_update_user(self, user, capacity=None):
        # type: (Union[hdx.data.user.User,Dict,str],Optional[str]) -> None
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
            user = hdx.data.user.User.read_from_hdx(user, configuration=self.configuration)
        elif isinstance(user, dict):
            user = hdx.data.user.User(user, configuration=self.configuration)
        if isinstance(user, hdx.data.user.User):
            users = self.data.get('users')
            if users is None:
                users = list()
                self.data['users'] = users
            if capacity is not None:
                user['capacity'] = capacity
            self._addupdate_hdxobject(users, 'name', user)
            return
        raise HDXError('Type %s cannot be added as a user!' % type(user).__name__)

    def add_update_users(self, users, capacity=None):
        # type: (List[Union[hdx.data.user.User,Dict,str]],Optional[str]) -> None
        """Add new or update existing users in organization with new metadata. Capacity eg. member, admin
        must be supplied either within the User object or dictionary or using the capacity argument (which takes
        precedence).

        Args:
            users (List[Union[User,Dict,str]]): A list of either user ids or users metadata from User objects or dictionaries
            capacity (Optional[str]): Capacity of users eg. member, admin. Defaults to None.

        Returns:
            None
        """
        if not isinstance(users, list):
            raise HDXError('Users should be a list!')
        for user in users:
            self.add_update_user(user, capacity)

    def remove_user(self, user):
        # type: (Union[hdx.data.user.User,Dict,str]) -> bool
        """Remove a user from the organization

        Args:
            user (Union[User,Dict,str]): Either a user id or user metadata either from a User object or a dictionary

        Returns:
            bool: True if user removed or False if not
        """
        return self._remove_hdxobject(self.data.get('users'), user)

    def get_datasets(self, query='*:*', **kwargs):
        # type: (str, Any) -> List[hdx.data.dataset.Dataset]
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
        return hdx.data.dataset.Dataset.search_in_hdx(query=query,
                                                      configuration=self.configuration,
                                                      fq='organization:%s' % self.data['name'], **kwargs)

    @staticmethod
    def get_all_organization_names(configuration=None, **kwargs):
        # type: (Optional[Configuration], Any) -> List[str]
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

        Returns:
            List[str]: List of all organization names in HDX
        """
        organization = Organization(configuration=configuration)
        organization['id'] = 'all organizations'  # only for error message if produced
        return organization._write_to_hdx('list', kwargs, 'id')
