# -*- coding: utf-8 -*-
"""Resource view class containing all logic for creating, checking, and updating resource views."""
import logging
from os.path import join
from typing import Optional, List, Any, Dict, Union

from hdx.utilities import is_valid_uuid

from hdx.data.hdxobject import HDXObject, HDXError
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
            path (Optional[str]): Path to YAML resource view metadata. Defaults to config/hdx_resource_view_static.yml.

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

    @classmethod
    def read_from_hdx(cls, identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['ResourceView']
        """Reads the resource view given by identifier from HDX and returns ResourceView object

        Args:
            identifier (str): Identifier of resource view
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[ResourceView]: ResourceView object if successful read, None if not
        """
        return cls._read_from_hdx_class('resource view', identifier, configuration)

    @staticmethod
    def get_all_for_resource(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> List['ResourceView']
        """Read all resource views for a resource given by identifier from HDX and returns list of ResourceView objects

        Args:
            identifier (str): Identifier of resource
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[ResourceView]: List of ResourceView objects
        """

        resourceview = ResourceView(configuration=configuration)
        success, result = resourceview._read_from_hdx('resource view', identifier, 'id', ResourceView.actions()['list'])
        resourceviews = list()
        if success:
            for resourceviewdict in result:
                resourceview = ResourceView(resourceviewdict, configuration=configuration)
                resourceviews.append(resourceview)
        return resourceviews

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

    def _update_resource_view(self, log=False, **kwargs):
        # type: (bool, Any) -> bool
        """Check if resource view exists in HDX and if so, update resource view

        Returns:
            bool: True if updated and False if not
        """
        update = False
        if 'id' in self.data and self._load_from_hdx('resource view', self.data['id']):
            update = True
        else:
            if 'resource_id' in self.data:
                resource_views = self.get_all_for_resource(self.data['resource_id'])
                for resource_view in resource_views:
                    if self.data['title'] == resource_view['title']:
                        self.old_data = self.data
                        self.data = resource_view.data
                        update = True
                        break
        if update:
            if log:
                logger.warning('resource view exists. Updating %s' % self.data['id'])
            self._merge_hdx_update('resource view', 'id', **kwargs)
        return update

    def update_in_hdx(self, **kwargs):
        # type: (Any) -> None
        """Check if resource view exists in HDX and if so, update resource view

        Returns:
            None
        """
        if not self._update_resource_view(**kwargs):
            raise HDXError('No existing resource view to update!')

    def create_in_hdx(self, **kwargs):
        # type: (Any) -> None
        """Check if resource view exists in HDX and if so, update it, otherwise create resource view

        Returns:
            None
        """
        if 'ignore_check' not in kwargs:  # allow ignoring of field checks
            self.check_required_fields()
        if not self._update_resource_view(log=True, **kwargs):
            self._save_to_hdx('create', 'title')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a resource view from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('resource view', 'id')

    def copy(self, resource_view):
        # type: (Union[ResourceView,Dict,str]) -> None
        """Copies all fields except id, resource_id and package_id from another resource view.

        Args:
            resource_view (Union[ResourceView,Dict,str]): Either a resource view id or resource view metadata either from a ResourceView object or a dictionary

        Returns:
            None
        """
        if isinstance(resource_view, str):
            if is_valid_uuid(resource_view) is False:
                raise HDXError('%s is not a valid resource view id!' % resource_view)
            resource_view = ResourceView.read_from_hdx(resource_view)
        if not isinstance(resource_view, dict) and not isinstance(resource_view, ResourceView):
            raise HDXError('%s is not a valid resource view!' % resource_view)
        for key in resource_view:
            if key not in ('id', 'resource_id', 'package_id'):
                self.data[key] = resource_view[key]