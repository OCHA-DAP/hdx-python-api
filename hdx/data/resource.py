#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
RESOURCE:
--------

Resource item; defines all logic for creating,
updating, and checking resources.

"""
import logging

from hdx.configuration import Configuration
from .hdxobject import HDXObject

logger = logging.getLogger(__name__)


class Resource(HDXObject):
    """
    Resource class.

    """
    action_url = {
        'show': 'resource_show?id=',
        'update': 'resource_update',
        'create': 'resource_create'
    }

    def __init__(self, configuration: Configuration, initial_data=None):
        if not initial_data:
            initial_data = dict()
        super(Resource, self).__init__(configuration, self.action_url, initial_data)

    def load_static(self, input_type: str = 'yaml', static_data='config/hdx_resource_static.yml'):
        self.load(input_type, static_data)

    def load_from_hdx(self, identifier) -> bool:
        """
        Checks if the resource exists in HDX.

        """

        return self._load_from_hdx('resource', identifier)

    def check_required_fields(self, ignore_fields=list()):
        self._check_required_fields('resource', ignore_fields)

    def update_in_hdx(self):
        """
        Updates a resource in HDX.

        """
        self._update_in_hdx('resource', 'id')

    def create_in_hdx(self):
        """
        Creates a resource in HDX.

        """
        self._create_in_hdx('resource', 'url')

    def delete_from_hdx(self):
        """
        Deletes a resource from HDX.

        """
        self._delete_from_hdx('resource', 'id')

    def create_datastore(self):
        pass
