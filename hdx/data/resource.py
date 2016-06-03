#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Resource class containing all logic for creating, checking, and updating resources."""
import logging
from os.path import join

from typing import Optional, Any, List

from hdx.configuration import Configuration
from .hdxobject import HDXObject

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """Resource class containing all logic for creating, checking, and updating resources.

        Args:
            configuration (Configuration): HDX Configuration
            initial_data (Optional[dict]): Initial resource metadata dictionary. Defaults to None.
    """
    action_url = {
        'show': 'resource_show?id=',
        'update': 'resource_update',
        'create': 'resource_create'
    }

    def __init__(self, configuration: Configuration, initial_data: Optional[dict] = None):
        if not initial_data:
            initial_data = dict()
        super(Resource, self).__init__(configuration, self.action_url, initial_data)

    def update_yaml(self, path: str = join('config', 'hdx_resource_static.yml')) -> None:
        """Update resource metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_resource_static.yml.

        Returns:
            None
        """
        super(Resource, self).update_yaml(path)

    def update_json(self, path: str = join('config', 'hdx_resource_static.json')) -> None:
        """Update resource metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_resource_static.json.

        Returns:
            None
        """
        super(Resource, self).update_json(path)

    def load_from_hdx(self, identifier: str) -> bool:
        """Loads the resource given by identifier from HDX

        Args:
            identifier (str): Identifier of resource

        Returns:
            bool: True if loaded, False if not

        """

        return self._load_from_hdx('resource', identifier)

    def check_required_fields(self, ignore_fields: List[Any] = list()) -> None:
        """Check that metadata for resource is complete

        Args:
            ignore_fields (List[Any]): Any fields to ignore in the check. Default is empty list.

        Returns:
            None
        """
        self._check_required_fields('resource', ignore_fields)

    def update_in_hdx(self) -> None:
        """Check if resource exists in HDX and if so, update it

        Returns:
            None

        """
        self._update_in_hdx('resource', 'id')

    def create_in_hdx(self) -> None:
        """Check if resource exists in HDX and if so, update it, otherwise create it

        Returns:
            None

        """
        self._create_in_hdx('resource', 'url')

    def delete_from_hdx(self) -> None:
        """Deletes a resource from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('resource', 'id')

    def create_datastore(self) -> None:
        """TODO"""
        pass
