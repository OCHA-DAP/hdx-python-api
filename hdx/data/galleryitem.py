#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
VISUALIZATION:
-------------

GalleryItem item; defines all logic for creating,
updating, and checking visualizations in galleries.

"""
import logging

from hdx.configuration import Configuration
from .hdxobject import HDXObject

logger = logging.getLogger(__name__)


class GalleryItem(HDXObject):
    """
    GalleryItem  class.

    """
    action_url = {
        'show': 'related_show?id=',
        'update': 'related_update',
        'create': 'related_create',
        'list': 'related_list?id='
    }

    def __init__(self, configuration: Configuration, initial_data=None):
        if not initial_data:
            initial_data = dict()
        super(GalleryItem, self).__init__(configuration, self.action_url, initial_data)

    def load_static(self, input_type: str = 'yaml', static_data='config/hdx_galleryitem_static.yml'):
        self.load(input_type, static_data)

    def load_from_hdx(self, identifier) -> bool:
        """
        Checks if the gallery item exists in HDX.

        """

        return self._load_from_hdx('gallery item', identifier)

    def check_required_fields(self, ignore_fields=list()):
        self._check_required_fields('galleryitem', ignore_fields)

    def update_in_hdx(self):
        """
        Updates a gallery item in HDX.

        """
        self._update_in_hdx('galleryitem', 'id')

    def create_in_hdx(self):
        """
        Creates a gallery item in HDX.

        """
        self._create_in_hdx('galleryitem', 'title')

    def delete_from_hdx(self):
        """
        Deletes a gallery item from HDX.

        """
        self._delete_from_hdx('galleryitem', 'id')
