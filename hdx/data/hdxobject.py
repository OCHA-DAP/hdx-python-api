# -*- coding: utf-8 -*-
"""
DATASET:
-------

hdx_object class; contains all logic for creating,
checking, and updating datasets.

"""
import abc
import collections
import json
import logging

import requests

from hdx.configuration import Configuration
from hdx.utilities.dictionary import merge_two_dictionaries
from hdx.utilities.json import EnhancedJSONEncoder
from hdx.utilities.loader import load_yaml_into_existing_dict, load_json_into_existing_dict

logger = logging.getLogger(__name__)


class HDXError(Exception):
    pass


class HDXObject(collections.UserDict):
    __metaclass__ = abc.ABCMeta

    """
    HDXObject class.

    """
    action_api_url = '/api/3/action/'

    def __init__(self, configuration: Configuration, action_url: dict, initial_data: dict):
        super(HDXObject, self).__init__(initial_data)
        self.configuration = configuration
        self.old_data = None
        self.base_url = '%s%s' % (configuration.get_hdx_site(), self.action_api_url)
        self.url = dict()
        for key in action_url:
            self.url[key] = '%s%s' % (self.base_url, action_url[key])

        self.headers = {
            'X-CKAN-API-Key': configuration.get_api_key(),
            'content-type': 'application/json'
        }

    def get_old_data_dict(self):
        return self.old_data

    def update_yaml(self, path: str):
        self.data = load_yaml_into_existing_dict(self.data, path)

    def update_json(self, path: str):
        self.data = load_json_into_existing_dict(self.data, path)

    def _get_from_hdx(self, object_type, id_field, url=None):
        """
        Checks if the hdx object exists in HDX.

        """
        if not id_field:
            raise HDXError("Empty %s identifier!" % object_type)
        if url is None:
            url = self.url['show']
        check = requests.get(
            '%s%s' % (url, id_field),
            headers=self.headers, auth=('dataproject', 'humdata')).json()

        if check['success'] is True:
            return check['result']
        else:
            return None

    def _load_from_hdx(self, object_type, id_field) -> bool:
        """
        Load hdx object from HDX.

        """
        temp_data = self.old_data
        self.old_data = self.data
        self.data = None
        self.data = self._get_from_hdx(object_type, id_field)
        if not self.data:
            self.data = self.old_data
            self.old_data = temp_data
            return False
        return True

    @abc.abstractmethod
    def load_from_hdx(self, id_field: str) -> bool:
        return

    def _load_existing_object(self, object_type, id_field_name):
        if not self.data:
            raise HDXError("No data in %s!" % object_type)
        if id_field_name not in self.data:
            raise HDXError("No %s field (mandatory) in %s!" % (id_field_name, object_type))
        return self.load_from_hdx(self.data[id_field_name])

    def _check_load_existing_object(self, object_type, id_field_name):
        if not self._load_existing_object(object_type, id_field_name):
            raise HDXError("No existing %s to update!" % object_type)

    @abc.abstractmethod
    def check_required_fields(self, ignore_fields: list = list()):
        return

    def _check_required_fields(self, object_type, ignore_fields):
        for field in self.configuration['%s' % object_type]['required_fields']:
            if field not in self.data and field not in ignore_fields:
                raise HDXError("Field %s is missing in %s!" % (field, object_type))

    def _merge_hdx_update(self, object_type, id_field_name):
        merge_two_dictionaries(self.data, self.old_data)
        self.check_required_fields(self.configuration['%s' % object_type].get('ignore_on_update', []))
        self._save_to_hdx('update', id_field_name)

    @abc.abstractmethod
    def update_in_hdx(self):
        return

    def _update_in_hdx(self, object_type, id_field_name):
        """
        Updates an object in HDX.

        """

        self._check_load_existing_object(object_type, id_field_name)
        self._merge_hdx_update(object_type, id_field_name)

    def _post_to_hdx(self, action, data, id_field_name):
        """
        Creates or updates an HDX object in HDX.

        """
        result = requests.post(
            self.url[action], data=json.dumps(data, cls=EnhancedJSONEncoder),
            headers=self.headers, auth=('dataproject', 'humdata'))

        if result.status_code // 100 == 2:
            logger.info('%sd successfully %s' % (action, data[id_field_name]))
            return True, result.json()['result']
        else:
            return False, result.text

    def _save_to_hdx(self, action, id_field_name):
        """
        Creates, updates or deletes an HDX object in HDX.

        """
        success, result = self._post_to_hdx(action, self.data, id_field_name)

        if success:
            self.old_data = self.data
            self.data = result
        else:
            raise HDXError('failed to %s %s\n%s' % (action, self.data[id_field_name], result))

    @abc.abstractmethod
    def create_in_hdx(self):
        return

    def _create_in_hdx(self, object_type, id_field_name):
        """
        Creates an HDX object in HDX.

        """
        self.check_required_fields()
        if self._load_existing_object(object_type, id_field_name):
            logger.warning('%s exists. Updating. %s' % (object_type, self.data[id_field_name]))
            self._merge_hdx_update(object_type, id_field_name)
        else:
            self._save_to_hdx('create', id_field_name)

    @abc.abstractmethod
    def delete_from_hdx(self):
        return

    def _delete_from_hdx(self, object_type, id_field_name):
        """
        Deletes an HDX object from HDX.

        """
        if id_field_name not in self.data:
            raise HDXError("No %s field (mandatory) in %s!" % (id_field_name, object_type))
        self._save_to_hdx('delete', id_field_name)

    @staticmethod
    def _underlying_object(_, object):
        return object

    def _addupdate_hdxobject(self, hdxobjects, id_field, hdxobjectclass, new_hdxobject):
        found = False
        for hdxobject in hdxobjects:
            if hdxobject[id_field] == new_hdxobject[id_field]:
                merge_two_dictionaries(hdxobject, new_hdxobject)
                found = True
                break
        if not found:
            hdxobjects.append(hdxobjectclass(self.configuration, new_hdxobject))

    def _separate_hdxobjects(self, hdxobjects, hdxobjects_name, id_field, hdxobjectclass):
        new_hdxobjects = self.data.get(hdxobjects_name, None)
        if new_hdxobjects:
            hdxobject_names = set()
            for hdxobject in hdxobjects:
                hdxobject_name = hdxobject[id_field]
                hdxobject_names.add(hdxobject_name)
                for new_hdxobject in new_hdxobjects:
                    if hdxobject_name == new_hdxobject[id_field]:
                        merge_two_dictionaries(hdxobject, new_hdxobject)
                        break
            for new_hdxobject in new_hdxobjects:
                if not new_hdxobject[id_field] in hdxobject_names:
                    hdxobjects.append(hdxobjectclass(self.configuration, new_hdxobject))
            del self.data[hdxobjects_name]
