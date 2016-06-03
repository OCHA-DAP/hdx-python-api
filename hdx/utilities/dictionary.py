#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Dict utilities"""
from collections import UserDict

from typing import List, Optional


def merge_two_dictionaries(a: dict, b: dict) -> dict:
    """Merges b into a and returns merged result

    NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen

    Args:
        a (dict): dictionary to merge into
        b: (dict): dictionary to merge from

    Returns:
        dict: Merged dictionary
    """
    key = None
    # ## debug output
    # sys.stderr.write("DEBUG: %s to %s\n" %(b,a))
    try:
        if a is None or isinstance(a, str) or isinstance(a, str) or isinstance(a, int) or isinstance(a, int) \
                or isinstance(a, float):
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be only appended
            if isinstance(b, list):
                # merge lists
                a.extend(b)
            else:
                # append to list
                a.append(b)
        elif isinstance(a, dict) or isinstance(a, UserDict):
            # dicts must be merged
            if isinstance(b, dict) or isinstance(b, UserDict):
                for key in b:
                    if key in a:
                        a[key] = merge_two_dictionaries(a[key], b[key])
                    else:
                        a[key] = b[key]
            else:
                raise ValueError('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
        else:
            raise ValueError('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
    except TypeError as e:
        raise ValueError('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
    return a


def merge_dictionaries(dicts: List[dict]) -> dict:
    """Merges all dictionaries in dicts into a single dictionary and returns result

    Args:
        dicts (List[dict]): Dictionaries to merge into the first one in the list

    Returns:
        dict: Merged dictionary

    """
    dict1 = dicts[0]
    for otherconfig in dicts[1:]:
        merge_two_dictionaries(dict1, otherconfig)
    return dict1


def dict_diff(d1: dict, d2: dict, no_key: Optional[str] = '<KEYNOTFOUND>') -> dict:
    """Compares two dictionaries

    Args:
        d1 (dict): First dictionary to compare
        d2 (dict): Second dictionary to compare
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
