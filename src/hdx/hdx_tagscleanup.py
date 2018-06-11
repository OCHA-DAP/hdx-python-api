# -*- coding: utf-8 -*-
"""Tags cleaning"""
import logging
from typing import Tuple, Dict, List

from hdx.utilities.downloader import Download

from hdx.hdx_configuration import Configuration

logger = logging.getLogger(__name__)


class ChainRuleError(Exception):
    pass


class Tags(object):
    """Methods to help with tags
    """
    _tags_dict = None
    _wildcard_tags = None

    @staticmethod
    def tagscleanupdicts(configuration=None, url=None, keycolumn=5, failchained=True):
        # type: () -> Tuple[Dict,List]
        """
        Get tags cleanup dictionaries

        Args:
            configuration (Optional[Configuration]): HDX configuration. Defaults to global configuration.
            url (Optional[str]): Url of tags cleanup spreadsheet. Defaults to None (internal configuration parameter).
            keycolumn (int): Column number of tag column in spreadsheet
            failchained (bool): Fail if chained rules found. Defaults to True.

        Returns:
            Tuple[Dict,List]: Returns (Tags dictionary, Wildcard tags list)
        """
        if not Tags._tags_dict:
            with Download() as downloader:
                if url is None:
                    if configuration is None:
                        configuration = Configuration.read()
                    url = configuration['tags_cleanup_url']
                Tags._tags_dict = downloader.download_tabular_rows_as_dicts(url, keycolumn=keycolumn)
                keys = Tags._tags_dict.keys()
                chainerror = False
                for i, tag in enumerate(keys):
                    whattodo = Tags._tags_dict[tag]
                    action = whattodo[u'action']
                    final_tags = whattodo[u'final tags (semicolon separated)']
                    for final_tag in final_tags.split(';'):
                        if final_tag in keys:
                            index = list(keys).index(final_tag)
                            if index != i:
                                whattodo2 = Tags._tags_dict[final_tag]
                                action2 = whattodo2[u'action']
                                if action2 != 'OK' and action2 != 'Other':
                                    final_tags2 = whattodo2[u'final tags (semicolon separated)']
                                    if final_tag not in final_tags2.split(';'):
                                        chainerror = True
                                        if failchained:
                                            logger.error('Chained rules: %s (%s -> %s) | %s (%s -> %s)' %
                                                         (action, tag, final_tags, action2, final_tag, final_tags2))

                if failchained and chainerror:
                    raise ChainRuleError('Chained rules for tags detected!')
                Tags._wildcard_tags = list()
                for tag in Tags._tags_dict:
                    if '*' in tag:
                        Tags._wildcard_tags.append(tag)
        return Tags._tags_dict, Tags._wildcard_tags

    @staticmethod
    def set_tagsdict(tags_dict):
        # type: (Dict) -> None
        """
        Set tags dictionary

        Args:
            tags_dict (Dict): Tags dictionary

        Returns:
            None
        """
        Tags._tags_dict = tags_dict

    @staticmethod
    def set_wildcard_tags(wildcard_tags):
        # type: (Dict) -> None
        """
        Set wildcard tags dictionary

        Args:
            wildcard_tags (Dict): Wildcard tags dictionary

        Returns:
            None
        """
        Tags._wildcard_tags = wildcard_tags
