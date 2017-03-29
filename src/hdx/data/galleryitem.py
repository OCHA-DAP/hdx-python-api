# -*- coding: utf-8 -*-
"""GalleryItem class containing all logic for creating, checking, and updating gallery items."""
import logging
from os.path import join

from typing import Optional

from hdx.configuration import Configuration
from hdx.data.hdxobject import HDXObject

logger = logging.getLogger(__name__)


class GalleryItem(HDXObject):
    """GalleryItem class containing all logic for creating, checking, and updating gallery items.

    Args:
        initial_data (Optional[dict]): Initial gallery item metadata dictionary. Defaults to None.
    """

    def __init__(self, initial_data=None):
        # type: (Optional[dict]) -> None
        if not initial_data:
            initial_data = dict()
        super(GalleryItem, self).__init__(initial_data)

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

    def update_from_yaml(self, path=join('config', 'hdx_galleryitem_static.yml')):
        # type: (str) -> None
        """Update gallery item metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_galleryitem_static.yml.

        Returns:
            None
        """
        super(GalleryItem, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_galleryitem_static.json')):
        # type: (str) -> None
        """Update gallery item metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_galleryitem_static.json.

        Returns:
            None
        """
        super(GalleryItem, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier):
        # type: (str) -> Optional['GalleryItem']
        """Reads the gallery item given by identifier from HDX and returns GalleryItem object

        Args:
            identifier (str): Identifier of gallery item

        Returns:
            Optional[GalleryItem]: GalleryItem object if successful read, None if not
        """

        galleryitem = GalleryItem()
        result = galleryitem._load_from_hdx('galleryitem', identifier)
        if result:
            return galleryitem
        return None

    def check_required_fields(self, ignore_dataset_id=False):
        # type: (Optional[bool]) -> None
        """Check that metadata for gallery item is complete. The parameter ignore_dataset_id should
        be set to True if you intend to add the object to a Dataset object (where it will be created during dataset
        creation).

        Args:
            ignore_dataset_id (bool): Whether to ignore the dataset id. Default is False.

        Returns:
            None
        """
        if ignore_dataset_id:
            ignore_fields = [Configuration.read()['galleryitem']['dataset_id']]
        else:
            ignore_fields = list()
        self._check_required_fields('galleryitem', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if gallery item exists in HDX and if so, update it

        Returns:
            None
        """
        self._update_in_hdx('galleryitem', 'id')

    def create_in_hdx(self):
        # type: () -> None
        """Check if gallery item exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self._create_in_hdx('galleryitem', 'id', 'title')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a gallery item from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('galleryitem', 'id')
