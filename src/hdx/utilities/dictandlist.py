# -*- coding: utf-8 -*-
"""Dict and List utilities"""
import itertools
from typing import List, Optional, TypeVar, Any, Callable

import six
from six.moves import UserDict, zip_longest

DictUpperBound = TypeVar('T', bound='dict')


def merge_two_dictionaries(a, b, merge_lists=False):
    # type: (DictUpperBound, DictUpperBound, bool) -> DictUpperBound
    """Merges b into a and returns merged result

    NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen

    Args:
        a (DictUpperBound): dictionary to merge into
        b (DictUpperBound): dictionary to merge from
        merge_lists (bool): Whether to merge lists (True) or replace lists (False). Default is False.

    Returns:
        DictUpperBound: Merged dictionary
    """
    key = None
    # ## debug output
    # sys.stderr.write("DEBUG: %s to %s\n" %(b,a))
    try:
        if a is None or isinstance(a, (six.string_types, six.text_type, six.integer_types, float)):
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be appended or replaced
            if isinstance(b, list):
                if merge_lists:
                    # merge lists
                    a.extend(b)
                else:
                    # replace list
                    a = b
            else:
                # append to list
                a.append(b)
        elif isinstance(a, (dict, UserDict)):
            # dicts must be merged
            if isinstance(b, (dict, UserDict)):
                for key in b:
                    if key in a:
                        a[key] = merge_two_dictionaries(a[key], b[key], merge_lists=merge_lists)
                    else:
                        a[key] = b[key]
            else:
                raise ValueError('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
        else:
            raise ValueError('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
    except TypeError as e:
        raise ValueError('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
    return a


def merge_dictionaries(dicts, merge_lists=False):
    # type: (List[DictUpperBound], bool) -> DictUpperBound
    """Merges all dictionaries in dicts into a single dictionary and returns result

    Args:
        dicts (List[DictUpperBound]): Dictionaries to merge into the first one in the list
        merge_lists (bool): Whether to merge lists (True) or replace lists (False). Default is False.

    Returns:
        DictUpperBound: Merged dictionary

    """
    dict1 = dicts[0]
    for other_dict in dicts[1:]:
        merge_two_dictionaries(dict1, other_dict, merge_lists=merge_lists)
    return dict1


def dict_diff(d1, d2, no_key='<KEYNOTFOUND>'):
    # type: (DictUpperBound, DictUpperBound, Optional[str]) -> dict
    """Compares two dictionaries

    Args:
        d1 (DictUpperBound): First dictionary to compare
        d2 (DictUpperBound): Second dictionary to compare
        no_key (Optional[str]): What value to use if key is not found Defaults to '<KEYNOTFOUND>'.

    Returns:
        dict: Comparison dictionary

    """
    d1keys = set(d1.keys())
    d2keys = set(d2.keys())
    both = d1keys & d2keys
    diff = {k: (d1[k], d2[k]) for k in both if d1[k] != d2[k]}
    diff.update({k: (d1[k], no_key) for k in d1keys - both})
    diff.update({k: (no_key, d2[k]) for k in d2keys - both})
    return diff


def dict_of_lists_add(dictionary, key, value):
    # type: (DictUpperBound, Any, Any) -> None
    """Add value to a list in a dictionary by key

    Args:
        dictionary (DictUpperBound): Dictionary to which to add values
        key (Any): Key within dictionary
        value (Any): Value to add to list in dictionary

    Returns:
        None

    """
    list_objs = dictionary.get(key, list())
    list_objs.append(value)
    dictionary[key] = list_objs


def list_distribute_contents_simple(input_list, function=lambda x: x):
    # type: (List[Any], Callable[[Any], Any]) -> List[Any]
    """Distribute the contents of a list eg. [1, 1, 1, 2, 2, 3] -> [1, 2, 3, 1, 2, 1]. List can contain complex types
    like dictionaries in which case the function can return the appropriate value eg.  lambda x: x[KEY]

    Args:
        input_list (List[Any]): Dictionary to which to add values
        function (Callable[[Any], Any]): Return value to use for distributing. Defaults to lambda x: x.

    Returns:
        List[Any]: Distributed list

    """
    dictionary = dict()
    for obj in input_list:
        dict_of_lists_add(dictionary, function(obj), obj)
    output_list = list()
    i = 0
    done = False
    while not done:
        found = False
        for key in sorted(dictionary):
            if i < len(dictionary[key]):
                output_list.append(dictionary[key][i])
                found = True
        if found:
            i += 1
        else:
            done = True
    return output_list


def list_distribute_contents(input_list, function=lambda x: x):
    # type: (List[Any], Callable[[Any], Any]) -> List[Any]
    """Distribute the contents of a list eg. [1, 1, 1, 2, 2, 3] -> [1, 2, 1, 2, 1, 3]. List can contain complex types
    like dictionaries in which case the function can return the appropriate value eg.  lambda x: x[KEY]

    Args:
        input_list (List[Any]): Dictionary to which to add values
        function (Callable[[Any], Any]): Return value to use for distributing. Defaults to lambda x: x.

    Returns:
        List[Any]: Distributed list

    """

    def riffle_shuffle(piles_list):
        def grouper(n, iterable, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(fillvalue=fillvalue, *args)

        if not piles_list:
            return []
        piles_list.sort(key=len, reverse=True)
        width = len(piles_list[0])
        pile_iters_list = [iter(pile) for pile in piles_list]
        pile_sizes_list = [[pile_position] * len(pile) for pile_position, pile in enumerate(piles_list)]
        grouped_rows = grouper(width, itertools.chain.from_iterable(pile_sizes_list))
        grouped_columns = zip_longest(*grouped_rows)
        shuffled_pile = [next(pile_iters_list[position]) for position in itertools.chain.from_iterable(grouped_columns)
                         if position is not None]
        return shuffled_pile

    dictionary = dict()
    for obj in input_list:
        dict_of_lists_add(dictionary, function(obj), obj)
    intermediate_list = list()
    for key in sorted(dictionary):
        intermediate_list.append(dictionary[key])
    return riffle_shuffle(intermediate_list)
