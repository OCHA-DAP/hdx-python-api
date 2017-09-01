# -*- coding: utf-8 -*-
"""Text processing utilities"""
import difflib
from typing import List


def get_matching_text(a, b, match_min_size=30, ignore=''):
    # type: (str, str, int, str) -> List[str]
    """Returns a list of matching blocks of text in a and b

    Args:
        a (str): First string to match
        b (str): Second string to match
        match_min_size (int): Minimum block size to match on. Defaults to 30.
        ignore (str): Any characters to ignore at the start and end of a block. Defaults to ''.

    Returns:
        List[str]: List of matching blocks of text

    """
    compare = difflib.SequenceMatcher()
    compare.set_seqs(a=a, b=b)
    matching_text = list()
    for match in compare.get_matching_blocks():
        start = match.a
        text = a[start: start+match.size]
        while len(text) >= match_min_size and text[0] in ignore:
            text = text[1:]
        while len(text) >= match_min_size and text[-1] in ignore:
            text = text[:-1]
        if len(text) >= match_min_size:
            matching_text.append(text)
    return matching_text


def get_matching_then_nonmatching_text(string_list, separator='', match_min_size=30, ignore=''):
    # type: (List[str], str, int, str) -> str
    """Returns a string containing matching blocks of text in a list of strings followed by non-matching.

    Args:
        string_list (List[str]): List of strings to match
        separator (str): Separator to add between blocks of text. Defaults to ''.
        match_min_size (int): Minimum block size to match on. Defaults to 30.
        ignore (str): Any characters to ignore at the start and end of a block. Defaults to ''.

    Returns:
        str: String containing matching blocks of text followed by non-matching

    """
    a = string_list[0]
    for i in range(1, len(string_list)):
        b = string_list[i]
        result = get_matching_text(a, b, match_min_size=match_min_size, ignore=ignore)
        new_a = a
        new_b = b
        for text in result:
            new_a = new_a.replace(text, '')
            new_b = new_b.replace(text, '')
        if new_a:
            result.append(new_a)
        if new_b:
            result.append(new_b)
        a = separator.join(result)
    return a
