# -*- coding: utf-8 -*-
"""Resource view class containing all logic for creating, checking, and updating resource views."""
import logging
from os.path import join
from typing import Optional, List

from hdx.data.hdxobject import HDXObject
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class ResourceView(HDXObject):
    """ResourceView class containing all logic for creating, checking, and updating resource views.

    Args:
        initial_data (Optional[Dict]): Initial resource view metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(ResourceView, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'resource_view_show',
            'update': 'resource_view_update',
            'create': 'resource_view_create',
            'delete': 'resource_view_delete',
            'list': 'resource_view_list',
            'reorder': 'resource_view_reorder'
        }

    def update_from_yaml(self, path=join('config', 'hdx_resource_view_static.yml')):
        # type: (str) -> None
        """Update resource view metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_resource_view_static.yml.

        Returns:
            None
        """
        super(ResourceView, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_resource_view_static.json')):
        # type: (str) -> None
        """Update resource view metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_resource_view_static.json.

        Returns:
            None
        """
        super(ResourceView, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['ResourceView']
        """Reads the resource view given by identifier from HDX and returns ResourceView object

        Args:
            identifier (str): Identifier of resource view
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[ResourceView]: ResourceView object if successful read, None if not
        """

        resourceview = ResourceView(configuration=configuration)
        result = resourceview._load_from_hdx('resource view', identifier)
        if result:
            return resourceview
        return None

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for resource view is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('resource view', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if resource view exists in HDX and if so, update resource view

        Returns:
            None
        """
        self._update_in_hdx('resource view', 'id')

    def create_in_hdx(self):
        # type: () -> None
        """Check if resource view exists in HDX and if so, update it, otherwise create resource view

        Returns:
            None
        """
        self._create_in_hdx('resource view', 'id', 'title')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a resource view from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('resource view', 'id')
