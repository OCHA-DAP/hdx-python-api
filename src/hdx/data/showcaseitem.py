# -*- coding: utf-8 -*-
"""ShowcaseItem class containing all logic for creating, checking, and updating showcase items."""
import logging
from os.path import join

from hdx.data.hdxobject import HDXObject

logger = logging.getLogger(__name__)


class ShowcaseItem(HDXObject):
    """ShowcaseItem class containing all logic for creating, checking, and updating showcase items.

    Args:
        initial_data (Optional[dict]): Initial showcase item metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(ShowcaseItem, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> dict
        """Dictionary of actions that can be performed on object

        Returns:
            dict: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'related_show',
            'update': 'related_update',
            'create': 'related_create',
            'delete': 'related_delete',
            'list': 'related_list'
        }

    def update_from_yaml(self, path=join('config', 'hdx_showcaseitem_static.yml')):
        # type: (str) -> None
        """Update showcase item metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_showcaseitem_static.yml.

        Returns:
            None
        """
        super(ShowcaseItem, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_showcaseitem_static.json')):
        # type: (str) -> None
        """Update showcase item metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_showcaseitem_static.json.

        Returns:
            None
        """
        super(ShowcaseItem, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['ShowcaseItem']
        """Reads the showcase item given by identifier from HDX and returns ShowcaseItem object

        Args:
            identifier (str): Identifier of showcase item
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[ShowcaseItem]: ShowcaseItem object if successful read, None if not
        """

        showcaseitem = ShowcaseItem(configuration=configuration)
        result = showcaseitem._load_from_hdx('showcaseitem', identifier)
        if result:
            return showcaseitem
        return None

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for showcase item is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('showcaseitem', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if showcase item exists in HDX and if so, update it

        Returns:
            None
        """
        self._update_in_hdx('showcaseitem', 'id')

    def create_in_hdx(self):
        # type: () -> None
        """Check if showcase item exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self._create_in_hdx('showcaseitem', 'id', 'title')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a showcase item from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('showcaseitem', 'id')
