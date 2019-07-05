# -*- coding: utf-8 -*-
"""Vocabulary class containing all logic for creating, checking, and updating vocabularies."""
import logging
from collections import OrderedDict
from os.path import join
from typing import Optional, List, Dict

from hdx.utilities.downloader import Download

from hdx.data.hdxobject import HDXObject, HDXObjectUpperBound
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class ChainRuleError(Exception):
    pass


class Vocabulary(HDXObject):
    """Vocabulary class containing all logic for creating, checking, and updating vocabularies.

    Args:
        initial_data (Optional[Dict]): Initial dataset metadata dictionary. Defaults to None.
        name (str): Name of vocabulary
        tags (Optional[List]): Initial list of tags. Defaults to None.
        configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
    """

    _approved_vocabulary = None
    _tags_dict = None

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

    @classmethod
    def get_approved_vocabulary(cls, configuration=None):
        # type: (Optional[Configuration]) -> Vocabulary
        """
        Get the HDX approved vocabulary

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Vocabulary: HDX Vocabulary object
        """
        if cls._approved_vocabulary is None:
            if configuration is None:
                configuration = Configuration.read()
            vocabulary_name = configuration['approved_tags_vocabulary']
            cls._approved_vocabulary = Vocabulary.read_from_hdx(vocabulary_name, configuration=configuration)
        return cls._approved_vocabulary

    @classmethod
    def _read_approved_tags(cls, url=None, configuration=None):
        # type: (Optional[str], Optional[Configuration]) -> List[str]
        """
        Read approved tags

        Args:
            url (Optional[str]): Url to read approved tags from. Defaults to None (url in internal configuration).
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[str]: List of approved tags
        """
        with Download(full_agent=configuration.get_user_agent()) as downloader:
            if url is None:
                url = configuration['tags_list_url']
            return list(OrderedDict.fromkeys(downloader.download(url).text.replace('"', '').splitlines()))

    @classmethod
    def create_approved_vocabulary(cls, url=None, configuration=None):
        # type: (Optional[str], Optional[Configuration]) -> Vocabulary
        """
        Create the HDX approved vocabulary

        Args:
            url (Optional[str]): Tag to check. Defaults to None (url in internal configuration).
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Vocabulary: HDX Vocabulary object
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary_name = configuration['approved_tags_vocabulary']
        tags = cls._read_approved_tags(url=url, configuration=configuration)
        cls._approved_vocabulary = Vocabulary(name=vocabulary_name, tags=tags, configuration=configuration)
        cls._approved_vocabulary.create_in_hdx()
        return cls._approved_vocabulary

    @classmethod
    def update_approved_vocabulary(cls, url=None, configuration=None):
        # type: (Optional[str], Optional[Configuration]) -> Vocabulary
        """
        Update the HDX approved vocabulary

        Args:
            url (Optional[str]): Tag to check. Defaults to None (url in internal configuration).
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            Vocabulary: HDX Vocabulary object
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary = cls.get_approved_vocabulary(configuration=configuration)
        vocabulary['tags'] = list()
        vocabulary.add_tags(cls._read_approved_tags(url=url, configuration=configuration))
        vocabulary.update_in_hdx()
        return vocabulary

    @classmethod
    def delete_approved_vocabulary(cls, configuration=None):
        # type: (Optional[Configuration]) -> None
        """
        Delete the approved vocabulary

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            None
        """
        if configuration is None:
            configuration = Configuration.read()
        vocabulary = cls.get_approved_vocabulary(configuration=configuration)
        vocabulary.delete_from_hdx()

    @classmethod
    def approved_tags(cls, configuration=None):
        # type: (Optional[Configuration]) -> List[str]
        """
        Return list of approved tags

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[str]: List of approved tags
        """
        return [x['name'] for x in cls.get_approved_vocabulary(configuration=configuration)['tags']]

    @classmethod
    def is_approved(cls, tag, configuration=None):
        # type: (str, Optional[Configuration]) -> bool
        """
        Return if tag is an approved one or not

        Args:
            tag (str): Tag to check
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            bool: True if tag is approved, False if not
        """
        if tag in cls.approved_tags(configuration):
            return True
        return False

    @classmethod
    def read_tags_mappings(cls, configuration=None, url=None, keycolumn=1, failchained=False):
        # type: (Optional[Configuration], Optional[str], int, bool) -> Dict
        """
        Read tag mappings and setup tags cleanup dictionaries

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            url (Optional[str]): Url of tags cleanup spreadsheet. Defaults to None (internal configuration parameter).
            keycolumn (int): Column number of tag column in spreadsheet. Defaults to 1.
            failchained (bool): Fail if chained rules found. Defaults to True.

        Returns:
            Dict: Returns Tags dictionary
        """
        if not cls._tags_dict:
            if configuration is None:
                configuration = Configuration.read()
            with Download(full_agent=configuration.get_user_agent()) as downloader:
                if url is None:
                    url = configuration['tags_mapping_url']
                cls._tags_dict = downloader.download_tabular_rows_as_dicts(url, keycolumn=keycolumn)
                keys = cls._tags_dict.keys()
                chainerror = False
                for i, tag in enumerate(keys):
                    whattodo = cls._tags_dict[tag]
                    action = whattodo[u'Action to Take']
                    final_tags = whattodo[u'New Tag(s)']
                    for final_tag in final_tags.split(';'):
                        if final_tag in keys:
                            index = list(keys).index(final_tag)
                            if index != i:
                                whattodo2 = cls._tags_dict[final_tag]
                                action2 = whattodo2[u'Action to Take']
                                if action2 != 'ok' and action2 != 'other':
                                    final_tags2 = whattodo2[u'New Tag(s)']
                                    if final_tag not in final_tags2.split(';'):
                                        chainerror = True
                                        if failchained:
                                            logger.error('Chained rules: %s (%s -> %s) | %s (%s -> %s)' %
                                                         (action, tag, final_tags, action2, final_tag, final_tags2))

                if failchained and chainerror:
                    raise ChainRuleError('Chained rules for tags detected!')
        return cls._tags_dict

    @classmethod
    def set_tagsdict(cls, tags_dict):
        # type: (Dict) -> None
        """
        Set tags dictionary

        Args:
            tags_dict (Dict): Tags dictionary

        Returns:
            None
        """
        cls._tags_dict = tags_dict

    @classmethod
    def get_mapped_tag(cls, tag):
        # type: (str) -> List[str]
        """Given a tag, return a list of tag(s) to which it maps

        Args:
            tags (str): Tag to map

        Returns:
            List[str]: List of mapped tag(s)
        """
        tags_dict = cls.read_tags_mappings()
        tags = list()
        if cls.is_approved(tag):
            tags.append(tag)
        elif tag not in tags_dict.keys():
            logger.error('Unapproved tag %s not in tag mapping!' % tag)
        else:
            whattodo = tags_dict[tag]
            action = whattodo[u'Action to Take']
            if action == u'ok':
                logger.error("Tag %s is not in CKAN approved tags but is in tags mappings!" % tag)
            elif action == u'delete':
                logger.info("Tag %s is invalid and won't be added!" % tag)
            elif action == u'merge':
                final_tags = whattodo['New Tag(s)'].split(';')
                tags.extend(final_tags)
            else:
                logger.error("Invalid action %s!" % action)
        return tags

    @classmethod
    def get_mapped_tags(cls, tags):
        # type: (List[str]) -> List[str]
        """Given a list of tags, return a list of tags to which they map

        Args:
            tags (List[str]): List of tags to map

        Returns:
            List[str]: List of mapped tags
        """
        new_tags = list()
        for tag in tags:
            mapped_tags = cls.get_mapped_tag(tag)
            new_tags.extend(mapped_tags)
        return list(OrderedDict.fromkeys(new_tags))

    @classmethod
    def add_mapped_tag(cls, hdxobject, tag, configuration=None):
        # type: (HDXObjectUpperBound, str, Optional[Configuration]) -> bool
        """Add a tag to an HDX object that has tags

        Args:
            hdxobject (T <= HDXObject): HDX object such as dataset
            tag (str): Tag to add
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            bool: True if tag added or False if tag already present
        """
        tags = cls.get_mapped_tag(tag)
        return hdxobject._add_tags(tags, cls.get_approved_vocabulary(configuration=configuration)['id'])

    @classmethod
    def add_mapped_tags(cls, hdxobject, tags, configuration=None):
        # type: (HDXObjectUpperBound, List[str], Optional[Configuration]) -> bool
        """Add a list of tag to an HDX object that has tags

        Args:
            hdxobject (T <= HDXObject): HDX object such as dataset
            tags (List[str]): List of tags to add
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            bool: True if all tags added or False if any already present.
        """
        new_tags = cls.get_mapped_tags(tags)
        return hdxobject._add_tags(new_tags, cls.get_approved_vocabulary(configuration=configuration)['id'])

    @classmethod
    def clean_tags(cls, hdxobject):
        # type: (HDXObjectUpperBound) -> List[str]
        """Clean tags in an HDX object according to tags cleanup spreadsheet

        Args:
            hdxobject (T <= HDXObject): HDX object such as dataset

        Returns:
            List[str]: Cleaned tags
        """
        tags = hdxobject._get_tags()
        new_tags = cls.get_mapped_tags(tags)
        hdxobject['tags'] = list()
        hdxobject._add_tags(new_tags, cls.get_approved_vocabulary(configuration=hdxobject.configuration)['id'])
        return new_tags
