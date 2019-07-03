# -*- coding: utf-8 -*-
"""Vocabulary class containing all logic for creating, checking, and updating vocabularies."""
import logging
from os.path import join
from typing import Optional, List, Dict

from hdx.data.hdxobject import HDXObject
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class Vocabulary(HDXObject):
    """Vocabulary class containing all logic for creating, checking, and updating vocabularies.

    Args:
        initial_data (Optional[Dict]): Initial dataset metadata dictionary. Defaults to None.
        name (str): Name of vocabulary
        tags (Optional[List]): Initial list of tags. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """
    def __init__(self, initial_data=None, name=None, tags=None, configuration=None):
        # type: (Optional[Dict], str, Optional[List], Optional[Configuration]) -> None
        if not initial_data:
            initial_data = dict()
        if name:
            initial_data['name'] = name
        super(Vocabulary, self).__init__(initial_data, configuration=configuration)
        if tags:
            self.add_tags(tags)

    @staticmethod
    def actions():
        # type: () -> Dict[str, str]
        """Dictionary of actions that can be performed on object

        Returns:
            Dict[str, str]: Dictionary of actions that can be performed on object
        """
        return {
            'show': 'vocabulary_show',
            'update': 'vocabulary_update',
            'create': 'vocabulary_create',
            'delete': 'vocabulary_delete',
            'list': 'vocabulary_list'
        }

    def update_from_yaml(self, path=join('config', 'hdx_vocabulary_static.yml')):
        # type: (str) -> None
        """Update vocabulary metadata with static metadata from YAML file

        Args:
            path (Optional[str]): Path to YAML dataset metadata. Defaults to config/hdx_vocabulary_static.yml.

        Returns:
            None
        """
        super(Vocabulary, self).update_from_yaml(path)

    def update_from_json(self, path=join('config', 'hdx_vocabulary_static.json')):
        # type: (str) -> None
        """Update vocabulary metadata with static metadata from JSON file

        Args:
            path (Optional[str]): Path to JSON dataset metadata. Defaults to config/hdx_vocabulary_static.json.

        Returns:
            None
        """
        super(Vocabulary, self).update_from_json(path)

    @staticmethod
    def read_from_hdx(identifier, configuration=None):
        # type: (str, Optional[Configuration]) -> Optional['Vocabulary']
        """Reads the vocabulary given by identifier from HDX and returns Vocabulary object

        Args:
            identifier (str): Identifier of vocabulary
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Optional[Vocabulary]: Vocabulary object if successful read, None if not
        """

        vocabulary = Vocabulary(configuration=configuration)
        result = vocabulary._load_from_hdx('vocabulary', identifier)
        if result:
            return vocabulary
        return None

    @staticmethod
    def get_all_vocabularies(configuration=None):
        # type: (Optional[Configuration]) -> List['Vocabulary']
        """Get all vocabulary names in HDX

        Args:
            identifier (str): Identifier of resource
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[Vocabulary]: List of Vocabulary objects
        """

        vocabulary = Vocabulary(configuration=configuration)
        vocabulary['id'] = 'all vocabulary names'  # only for error message if produced
        vocabularies = list()
        for vocabularydict in vocabulary._write_to_hdx('list', {}):
            vocabularies.append(Vocabulary(vocabularydict, configuration=configuration))
        return vocabularies

    def check_required_fields(self, ignore_fields=list()):
        # type: (List[str]) -> None
        """Check that metadata for vocabulary is complete. The parameter ignore_fields should
        be set if required to any fields that should be ignored for the particular operation.

        Args:
            ignore_fields (List[str]): Fields to ignore. Default is [].

        Returns:
            None
        """
        self._check_required_fields('vocabulary', ignore_fields)

    def update_in_hdx(self):
        # type: () -> None
        """Check if vocabulary exists in HDX and if so, update vocabulary

        Returns:
            None
        """
        self._update_in_hdx('vocabulary', 'id', force_active=False)

    def create_in_hdx(self):
        # type: () -> None
        """Check if vocabulary exists in HDX and if so, update it, otherwise create vocabulary

        Returns:
            None
        """
        self._create_in_hdx('vocabulary', 'id', 'name', force_active=False)

    def delete_from_hdx(self, empty=True):
        # type: (bool) -> None
        """Deletes a vocabulary from HDX. Vocabulary can only be deleted if it ahs no tags.

        Args:
            empty (bool): Remove all tags and update before deleting. Defaults to True.

        Returns:
            None
        """
        if empty and len(self.data['tags']) != 0:
            self.data['tags'] = list()
            self.update_in_hdx()
        self._delete_from_hdx('vocabulary', 'id')

    def get_tags(self):
        # type: () -> List[str]
        """Lists tags in vocabulary

        Returns:
            List[str]: List of tags in vocabulary
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
        """Add a list of tags

        Args:
            tags (List[str]): list of tags to add

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
