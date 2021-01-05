# -*- coding: utf-8 -*-
"""Helper to the Dataset class for handling matching resources.
"""
import collections
from typing import List, Optional, Tuple

import hdx.data.resource


class ResourceMatcher(object):
    @staticmethod
    def match_resource_list(resources1, resource2):
        # type: (List[hdx.data.resource.Resource], hdx.data.resource.Resource) -> Optional[int]
        """Helper method to find the index of a resource that matches a given resource

        Args:
            resources1 (List[hdx.data.resource.Resource]): List of resources
            resource2 (hdx.data.resource.Resource): Resource to match with list

        Returns:
            Optional[int]: Index of resource that matches in list or None
        """
        id2 = resource2.get('id')
        grouping2 = resource2.get('grouping')
        index1_match = None
        if id2 is not None:
            ids1 = [(x.get('id'), x.get('grouping')) for x in resources1]
            id2 = (resource2.get('id'), grouping2)

            if id2 is not None:
                try:
                    return ids1.index(id2)
                except ValueError:
                    pass
        names1 = [(x['name'], x.get('grouping')) for x in resources1]
        name2 = (resource2['name'], grouping2)
        formats1 = [x['format'] for x in resources1]
        format2 = resource2['format'].lower()

        dupnames = {item for item, count in collections.Counter(names1).items() if count > 1}
        for i, name1 in enumerate(names1):
            if name1 != name2:
                continue
            if name1 in dupnames:
                if formats1[i].lower() != format2:
                    continue
            index1_match = i
        return index1_match

    @staticmethod
    def match_resource_lists(resources1, resources2):
        # type: (List[hdx.data.resource.Resource], List[hdx.data.resource.Resource]) -> Tuple[List, List, List, List]
        """Helper method to match two lists of resources returning the indices that match in two lists and
        that don't match in two more lists

        Args:
            resources1 (List[hdx.data.resource.Resource]): List of resources
            resources2 (List[hdx.data.resource.Resource]): List of resources to match with first list

        Returns:
            Tuple[List, List, List, List]: Returns indices that match (2 lists) and that don't match (2 lists)
        """
        ids1 = [x.get('id') for x in resources1]
        groups1 = [x.get('grouping') for x in resources1]
        names1 = [x['name'] for x in resources1]
        formats1 = [x['format'] for x in resources1]
        ids2 = [x.get('id') for x in resources2]
        groups2 = [x.get('grouping') for x in resources2]
        names2 = [x['name'] for x in resources2]
        formats2 = [x['format'] for x in resources2]
        index1_matches = list()
        index2_matches = list()
        for i, id1 in enumerate(ids1):
            if id1 is None:
                continue
            for j, id2 in enumerate(ids2):
                if id2 is None:
                    continue
                if id1 == id2:
                    if groups1[i] == groups2[j]:
                        index1_matches.append(i)
                        index2_matches.append(j)
        dupnames1 = {item for item, count in collections.Counter(names1).items() if count > 1}
        dupnames2 = {item for item, count in collections.Counter(names2).items() if count > 1}
        dupnames = dupnames1.union(dupnames2)
        for i, name1 in enumerate(names1):
            if i in index1_matches:
                continue
            for j, name2 in enumerate(names2):
                if j in index2_matches:
                    continue
                if name1 != name2:
                    continue
                if name1 in dupnames:
                    if formats1[i].lower() != formats2[j].lower():
                        continue
                if groups1[i] == groups2[j]:
                    index1_matches.append(i)
                    index2_matches.append(j)
        index1_nomatches = [i for i, _ in enumerate(ids1) if i not in index1_matches]
        index2_nomatches = [i for i, _ in enumerate(ids2) if i not in index2_matches]
        return index1_matches, index2_matches, index1_nomatches, index2_nomatches