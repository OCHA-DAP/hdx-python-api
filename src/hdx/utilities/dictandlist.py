# -*- coding: utf-8 -*-
"""Dict and List utilities"""
from __future__ import division

import itertools
from os.path import join
from typing import List, TypeVar, Callable, Dict, Any, Union, Optional

import six
from six.moves import UserDict, zip_longest
from tabulator import Stream

DictUpperBound = TypeVar('T', bound='dict')
ExceptionUpperBound = TypeVar('TE', bound='Exception')


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
    # sys.stderr.write('DEBUG: %s to %s\n' %(b,a))
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
    # type: (DictUpperBound, DictUpperBound, str) -> Dict
    """Compares two dictionaries

    Args:
        d1 (DictUpperBound): First dictionary to compare
        d2 (DictUpperBound): Second dictionary to compare
        no_key (str): What value to use if key is not found Defaults to '<KEYNOTFOUND>'.

    Returns:
        Dict: Comparison dictionary

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
    # type: (List, Callable[[Any], Any]) -> List
    """Distribute the contents of a list eg. [1, 1, 1, 2, 2, 3] -> [1, 2, 3, 1, 2, 1]. List can contain complex types
    like dictionaries in which case the function can return the appropriate value eg.  lambda x: x[KEY]

    Args:
        input_list (List): List to distribute values
        function (Callable[[Any], Any]): Return value to use for distributing. Defaults to lambda x: x.

    Returns:
        List: Distributed list

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
    # type: (List, Callable[[Any], Any]) -> List
    """Distribute the contents of a list eg. [1, 1, 1, 2, 2, 3] -> [1, 2, 1, 2, 1, 3]. List can contain complex types
    like dictionaries in which case the function can return the appropriate value eg.  lambda x: x[KEY]

    Args:
        input_list (List): List to distribute values
        function (Callable[[Any], Any]): Return value to use for distributing. Defaults to lambda x: x.

    Returns:
        List: Distributed list

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


def extract_list_from_list_of_dict(list_of_dict, key):
    # type: (List[DictUpperBound], Any) -> List
    """Extract a list by looking up key in each member of a list of dictionaries

    Args:
        list_of_dict (List[DictUpperBound]): List of dictionaries
        key (Any): Key to find in each dictionary

    Returns:
        List: List containing values returned from each dictionary

    """
    result = list()
    for dictionary in list_of_dict:
        result.append(dictionary[key])
    return result


def key_value_convert(dictin, keyfn=lambda x: x, valuefn=lambda x: x, dropfailedkeys=False, dropfailedvalues=False,
                      exception=ValueError):
    # type: (DictUpperBound, Callable[[Any], Any], Callable[[Any], Any], bool, bool, ExceptionUpperBound) -> Dict
    """Convert keys and/or values of dictionary using functions passed in as parameters

    Args:
        dictin (DictUpperBound): Input dictionary
        keyfn (Callable[[Any], Any]): Function to convert keys. Defaults to lambda x: x
        valuefn (Callable[[Any], Any]): Function to convert values. Defaults to lambda x: x
        dropfailedkeys (bool): Whether to drop dictionary entries where key conversion fails. Defaults to False.
        dropfailedvalues (bool): Whether to drop dictionary entries where value conversion fails. Defaults to False.
        exception (ExceptionUpperBound): The exception to expect if keyfn or valuefn fail. Defaults to ValueError.

    Returns:
        Dict: Dictionary with converted keys and/or values

    """
    dictout = dict()
    for key in dictin:
        try:
            new_key = keyfn(key)
        except exception:
            if dropfailedkeys:
                continue
            new_key = key
        value = dictin[key]
        try:
            new_value = valuefn(value)
        except exception:
            if dropfailedvalues:
                continue
            new_value = value
        dictout[new_key] = new_value
    return dictout


def integer_key_convert(dictin, dropfailedkeys=False):
    # type: (DictUpperBound, bool) -> Dict
    """Convert keys of dictionary to integers

    Args:
        dictin (DictUpperBound): Input dictionary
        dropfailedkeys (bool): Whether to drop dictionary entries where key conversion fails. Defaults to False.

    Returns:
        Dict: Dictionary with keys converted to integers

    """
    return key_value_convert(dictin, keyfn=int, dropfailedkeys=dropfailedkeys)


def integer_value_convert(dictin, dropfailedvalues=False):
    # type: (DictUpperBound, bool) -> Dict
    """Convert values of dictionary to integers

    Args:
        dictin (DictUpperBound): Input dictionary
        dropfailedkeys (bool): Whether to drop dictionary entries where key conversion fails. Defaults to False.

    Returns:
        Dict: Dictionary with values converted to integers

    """
    return key_value_convert(dictin, valuefn=int, dropfailedvalues=dropfailedvalues)


def float_value_convert(dictin, dropfailedvalues=False):
    # type: (DictUpperBound, bool) -> Dict
    """Convert values of dictionary to floats

    Args:
        dictin (DictUpperBound): Input dictionary
        dropfailedkeys (bool): Whether to drop dictionary entries where key conversion fails. Defaults to False.

    Returns:
        Dict: Dictionary with values converted to floats

    """
    return key_value_convert(dictin, valuefn=float, dropfailedvalues=dropfailedvalues)


def avg_dicts(dictin1, dictin2):
    # type: (DictUpperBound, DictUpperBound) -> Dict
    """Create a new dictionary from two dictionaries by averaging values

    Args:
        dictin1 (DictUpperBound): First input dictionary
        dictin2 (DictUpperBound): Second input dictionary

    Returns:
        Dict: Dictionary with values being average of 2 input dictionaries

    """
    dictout = dict()
    for key in dictin1:
        dictout[key] = (dictin1[key] + dictin2[key]) / 2
    return dictout


def read_list_from_csv(folder, filename, dict_form=False, headers=None):
    # type: (str, str, bool, Optional[int]) -> List[Union[Dict, List]]
    """Read a list of rows in dict or list form from a csv.

    Args:
        folder (str): Folder to write to
        filename (str): Name of file to write to
        dict_form (bool): Return in dict form. Defaults to False.
        headers (Optional[List[str]]): Row number of headers. Defaults to None.

    Returns:
        List[Union[Dict, List]]: List of rows in dict or list form

    """
    filepath = join(folder, filename)
    stream = Stream(filepath, headers=headers)
    stream.open()
    result = stream.read(keyed=dict_form)
    stream.close()
    return result


def write_list_to_csv(list_of_rows, folder, filename, headers=None):
    # type: (List[Union[DictUpperBound, List]], str, str, Optional[List[str]]) -> str
    """Write a list of rows in dict or list form to a csv.

    Args:
        list_of_rows (List[Union[DictUpperBound, List]]): List of rows in dict or list form
        folder (str): Folder to write to
        filename (str): Name of file to write to
        headers (Optional[List[str]]): Headers to write. Defaults to None.

    Returns:
        str: Path of file

    """
    filepath = join(folder, filename)
    stream = Stream(list_of_rows, headers=headers)
    stream.open()
    stream.save(filepath, format='csv')
    stream.close()
    return filepath
