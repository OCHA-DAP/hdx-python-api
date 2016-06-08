#!/usr/bin/python
# -*- coding: utf-8 -*-
"""GalleryItem class containing all logic for creating, checking, and updating gallery items."""
import logging
from os.path import join

from typing import Optional, List

from hdx.configuration import Configuration
from .hdxobject import HDXObject

logger = logging.getLogger(__name__)


class GalleryItem(HDXObject):
    """GalleryItem class containing all logic for creating, checking, and updating gallery items.

        Args:
            configuration (Configuration): HDX Configuration
            initial_data (Optional[dict]): Initial gallery item metadata dictionary. Defaults to None.
    """
    _action_url = {
        'show': 'related_show?id=',
        'update': 'related_update',
        'create': 'related_create',
        'list': 'related_list?id='
    }

    def __init__(self, configuration: Configuration, initial_data: Optional[dict] = None):
        if not initial_data:
            initial_data = dict()
        super(GalleryItem, self).__init__(configuration, self._action_url, initial_data)

    def update_yaml(self, path: str = join('config', 'hdx_galleryitem_static.yml')) -> None:
        """Update gallery item metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_galleryitem_static.yml.

        Returns:
            None
        """
        super(GalleryItem, self).update_yaml(path)

    def update_json(self, path: str = join('config', 'hdx_galleryitem_static.json')) -> None:
        """Update gallery item metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_galleryitem_static.json.

        Returns:
            None
        """
        super(GalleryItem, self).update_json(path)

    @staticmethod
    def read_from_hdx(configuration: Configuration, identifier: str) -> Optional['GalleryItem']:
        """Reads the gallery item given by identifier from HDX and returns GalleryItem object

        Args:
            configuration (Configuration): HDX Configuration
            identifier (str): Identifier of gallery item

        Returns:
            Optional[GalleryItem]: GalleryItem object if successful read, None if not

        """

        galleryitem = GalleryItem(configuration)
        result = galleryitem._load_from_hdx('galleryitem', identifier)
        if result:
            return galleryitem
        return None

    def check_required_fields(self, ignore_fields: List[str] = list()) -> None:
        """Check that metadata for gallery item is complete

        Args:
            ignore_fields (List[str]): Any fields to ignore in the check. Default is empty list.

        Returns:
            None
        """
        self._check_required_fields('galleryitem', ignore_fields)

    def update_in_hdx(self) -> None:
        """Check if gallery item exists in HDX and if so, update it

        Returns:
            None

        """
        self._update_in_hdx('galleryitem', 'id')

    def create_in_hdx(self) -> None:
        """Check if gallery item exists in HDX and if so, update it, otherwise create it

        Returns:
            None

        """
        self._create_in_hdx('galleryitem', 'title')

    def delete_from_hdx(self) -> None:
        """Deletes a gallery item from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('galleryitem', 'id')
