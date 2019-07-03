# -*- coding: utf-8 -*-
"""Tags cleaning"""
import logging
from collections import OrderedDict
from typing import Dict, Optional, List

from hdx.utilities.downloader import Download

from hdx.data.hdxobject import HDXObjectUpperBound
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class ChainRuleError(Exception):
    pass


class ApprovedTags(object):
    """Methods to help with tags in the approved vocabulary and to handle mapping of tags
    """
    _approved_vocabulary = None
    _approved_tags = None
    _tags_dict = None

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
        if not cls._approved_tags:
            if configuration is None:
                configuration = Configuration.read()
            cls._approved_vocabulary = configuration['approved_tags_vocabulary']
            vocabulary = Vocabulary.read_from_hdx(cls._approved_vocabulary, configuration=configuration)
            cls._approved_tags = [x['name'] for x in vocabulary['tags']]
        return cls._approved_tags

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
    def read_approved_tags(cls, url=None, configuration=None):
        # type: (Optional[str], Optional[Configuration]) -> List[str]
        """
        Read approved tags

        Args:
            url (Optional[str]): Tag to check. Defaults to None (url in internal configuration).
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.

        Returns:
            List[str]: List of approved tags
        """
        with Download(full_agent=configuration.get_user_agent()) as downloader:
            if url is None:
                url = configuration['tags_list_url']
            cls._approved_tags = list(OrderedDict.fromkeys(downloader.download(url).text.replace('"', '').split('\n')))
            return cls._approved_tags

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
        if not cls._approved_vocabulary:
            if configuration is None:
                configuration = Configuration.read()
            cls._approved_vocabulary = configuration['approved_tags_vocabulary']
        return Vocabulary.read_from_hdx(cls._approved_vocabulary, configuration=configuration)

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
        if not cls._approved_vocabulary:
            if configuration is None:
                configuration = Configuration.read()
            cls._approved_vocabulary = configuration['approved_tags_vocabulary']

        cls.read_approved_tags(url=url, configuration=configuration)
        vocabulary = Vocabulary(name=cls._approved_vocabulary, tags=cls._approved_tags, configuration=configuration)
        vocabulary.create_in_hdx()
        return vocabulary

    @classmethod
    def update_approved_vocabulary(cls, url=None, configuration=None):
        # type: (Optional[str], Optional[Configuration]) -> None
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
        vocabulary.add_tags(cls.read_approved_tags(url=url, configuration=configuration))
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
        vocabulary = cls.get_approved_vocabulary(configuration=configuration)
        vocabulary.delete_from_hdx()

    @classmethod
    def read_tags_mappings(cls, configuration=None, url=None, keycolumn=1, failchained=False):  # Remember to set back to True!!!
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
                tags.append(tag)
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
        return new_tags

    @classmethod
    def add_mapped_tag(cls, hdxobject, tag):
        # type: (HDXObjectUpperBound, str) -> bool
        """Add a tag to an HDX object that has tags

        Args:
            hdxobject (T <= HDXObject): HDX object such as dataset
            tag (str): Tag to add

        Returns:
            bool: True if tag added or False if tag already present
        """
        tags = cls.get_mapped_tag(tag)
        return hdxobject._add_tags(tags)

    @classmethod
    def add_mapped_tags(cls, hdxobject, tags):
        # type: (HDXObjectUpperBound, List[str]) -> bool
        """Add a list of tag to an HDX object that has tags

        Args:
            hdxobject (T <= HDXObject): HDX object such as dataset
            tags (List[str]): List of tags to add

        Returns:
            bool: True if all tags added or False if any already present.
        """
        new_tags = cls.get_mapped_tags(tags)
        return hdxobject._add_tags(new_tags)

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
        hdxobject._add_tags(new_tags)
        return new_tags

