# -*- coding: utf-8 -*-
"""Showcase class containing all logic for creating, checking, and updating showcases."""
import logging
from os.path import join
from typing import List, Union, Optional, Dict

from hdx.utilities import is_valid_uuid

import hdx.data.dataset
import hdx.data.hdxobject
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class Showcase(hdx.data.hdxobject.HDXObject):
    """Showcase class containing all logic for creating, checking, and updating showcases.

    Args:
        initial_data (Optional[Dict]): Initial showcase metadata dictionary. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """
    dataset_ids_field = 'dataset_ids'

    def __init__(self, initial_data=None, configuration=None):
        # type: (Optional[Dict], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        super(Showcase, self).__init__(initial_data, configuration=configuration)

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'ckanext_showcase_show',
            'update': 'ckanext_showcase_update',
            'create': 'ckanext_showcase_create',
            'delete': 'ckanext_showcase_delete',
            'list': 'ckanext_showcase_list',
            'associate': 'ckanext_showcase_package_association_create',
            'disassociate': 'ckanext_showcase_package_association_delete',
            'list_datasets': 'ckanext_showcase_package_list',
            'list_showcases': 'ckanext_package_showcase_list'
        }

    def update_from_yaml(self, path=join('config', 'hdx_showcase_static.yml')):
        # type: (str) -> None
        """Update showcase metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_showcase_static.yml.

        Returns:
            None
        """
        super(Showcase, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_showcase_static.json')):
        # type: (str) -> None
        """Update showcase metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_showcase_static.json.

        Returns:
            None
        """
        super(Showcase, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['Showcase']
        """Reads the showcase given by identifier from HDX and returns Showcase object

        Args:
            identifier (str): Identifier of showcase
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Showcase]: Showcase object if successful read, None if not
        """

        showcase = Showcase(configuration=configuration)
        result = showcase._load_from_hdx('showcase', identifier)
        if result:
            return showcase
        return None

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for showcase is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('showcase', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if showcase exists in HDX and if so, update it

        Returns:
            None
        """
        self._update_in_hdx('showcase', 'name')

    def create_in_hdx(self):
        # type: () -> None
        """Check if showcase exists in HDX and if so, update it, otherwise create it

        Returns:
            None
        """
        self._create_in_hdx('showcase', 'name', 'title')

    def delete_from_hdx(self):
        # type: () -> None
        """Deletes a showcase from HDX.

        Returns:
            None
        """
        self._delete_from_hdx('showcase', 'id')

    def get_tags(self):
        # type: () -> List[str]
        """Return the dataset's list of tags

        Returns:
            List[str]: List of tags or [] if there are none
        """
        return self._get_tags()

    def add_tag(self, tag):
        # type: (str) -> bool
        """Add a tag

        Args:
            tag (str): Tag to add

        Returns:
            bool: True if tag added or False if tag already present
        """
        return self._add_tag(tag)

    def add_tags(self, tags):
        # type: (List[str]) -> bool
        """Add a list of tag

        Args:
            tags (List[str]): List of tags to add

        Returns:
            bool: True if all tags added or False if any already present.
        """
        return self._add_tags(tags)

    def remove_tag(self, tag):
        # type: (str) -> bool
        """Remove a tag

        Args:
            tag (str): Tag to remove

        Returns:
            bool: True if tag removed or False if not
        """
        return self._remove_hdxobject(self.data.get('tags'), tag, matchon='name')

    def get_datasets(self):
        # type: () -> List[hdx.data.dataset.Dataset]
        """Get any datasets in the showcase

        Returns:
            List[Dataset]: List of datasets
        """
        assoc_result, datasets_dicts = self._read_from_hdx('showcase', self.data['id'], fieldname='showcase_id',
                                                           action=self.actions()['list_datasets'])
        datasets = list()
        if assoc_result:
            for dataset_dict in datasets_dicts:
                dataset = hdx.data.dataset.Dataset(dataset_dict, configuration=self.configuration)
                datasets.append(dataset)
        return datasets

    def _get_showcase_dataset_dict(self, dataset):
        # type: (Union[hdx.data.dataset.Dataset,Dict,str]) -> Dict
        """Get showcase dataset dict

        Args:
            showcase (Union[Showcase,Dict,str]): Either a showcase id or Showcase metadata from a Showcase object or dictionary

        Returns:
            Dict: showcase dataset dict
        """
        if isinstance(dataset, hdx.data.dataset.Dataset) or isinstance(dataset, dict):
            if 'id' not in dataset:
                dataset = hdx.data.dataset.Dataset.read_from_hdx(dataset['name'])
            dataset = dataset['id']
        elif not isinstance(dataset, str):
            raise hdx.data.hdxobject.HDXError('Type %s cannot be added as a dataset!' % type(dataset).__name__)
        if is_valid_uuid(dataset) is False:
            raise hdx.data.hdxobject.HDXError('%s is not a valid dataset id!' % dataset)
        return {'showcase_id': self.data['id'], 'package_id': dataset}

    def add_dataset(self, dataset, datasets_to_check=None):
        # type: (Union[hdx.data.dataset.Dataset,Dict,str], List[hdx.data.dataset.Dataset]) -> bool
        """Add a dataset

        Args:
            dataset (Union[Dataset,Dict,str]): Either a dataset id or dataset metadata either from a Dataset object or a dictionary
            datasets_to_check (List[Dataset]): List of datasets against which to check existence of dataset. Defaults to datasets in showcase.

        Returns:
            bool: True if the dataset was added, False if already present
        """
        showcase_dataset = self._get_showcase_dataset_dict(dataset)
        if datasets_to_check is None:
            datasets_to_check = self.get_datasets()
        for dataset in datasets_to_check:
            if showcase_dataset['package_id'] == dataset['id']:
                return False
        self._write_to_hdx('associate', showcase_dataset, 'package_id')
        return True

    def add_datasets(self, datasets, datasets_to_check=None):
        # type: (List[Union[hdx.data.dataset.Dataset,Dict,str]], List[hdx.data.dataset.Dataset]) -> bool
        """Add multiple datasets

        Args:
            datasets (List[Union[Dataset,Dict,str]]): A list of either dataset ids or dataset metadata from Dataset objects or dictionaries
            datasets_to_check (List[Dataset]): List of datasets against which to check existence of dataset. Defaults to datasets in showcase.

        Returns:
            bool: True if all datasets added or False if any already present
        """
        if datasets_to_check is None:
            datasets_to_check = self.get_datasets()
        alldatasetsadded = True
        for dataset in datasets:
            if not self.add_dataset(dataset, datasets_to_check=datasets_to_check):
                alldatasetsadded = False
        return alldatasetsadded

    def remove_dataset(self, dataset):
        # type: (Union[Dataset,Dict,str]) -> None
        """Remove a dataset

        Args:
            dataset (Union[Dataset,Dict,str]): Either a dataset id or dataset metadata either from a Dataset object or a dictionary

        Returns:
            None
        """
        self._write_to_hdx('disassociate', self._get_showcase_dataset_dict(dataset), 'package_id')
