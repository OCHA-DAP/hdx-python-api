# -*- coding: utf-8 -*-
"""Text processing utilities"""
import difflib
from typing import List


def get_matching_text_in_strs(a, b, match_min_size=30, ignore='', end_characters=''):
    # type: (str, str, int, str, str) -> List[str]
    """Returns a list of matching blocks of text in a and b

    Args:
        a (str): First string to match
        b (str): Second string to match
        match_min_size (int): Minimum block size to match on. Defaults to 30.
        ignore (str): Any characters to ignore in matching. Defaults to ''.
        end_characters (str): End characters to look for. Defaults to ''

    Returns:
        List[str]: List of matching blocks of text

    """
    compare = difflib.SequenceMatcher(lambda x: x in ignore)
    compare.set_seqs(a=a, b=b)
    matching_text = list()

    for match in compare.get_matching_blocks():
        start = match.a
        text = a[start: start+match.size]
        if end_characters:
            prev_text = text
            while len(text) != 0 and text[0] in end_characters:
                text = text[1:]
            while len(text) != 0 and text[-1] not in end_characters:
                text = text[:-1]
            if len(text) == 0:
                text = prev_text
        if len(text) >= match_min_size:
            matching_text.append(text)
    return matching_text


def get_matching_text(string_list, match_min_size=30, ignore='', end_characters='.!\r\n'):
    # type: (List[str], int, str, str) -> str
    """Returns a string containing matching blocks of text in a list of strings followed by non-matching.

    Args:
        string_list (List[str]): List of strings to match
        match_min_size (int): Minimum block size to match on. Defaults to 30.
        ignore (str): Any characters to ignore in matching. Defaults to ''.
        end_characters (str): End characters to look for. Defaults to '.\r\n'

    Returns:
        str: String containing matching blocks of text followed by non-matching

    """
    a = string_list[0]
    for i in range(1, len(string_list)):
        b = string_list[i]
        result = get_matching_text_in_strs(a, b, match_min_size=match_min_size, ignore=ignore,
                                           end_characters=end_characters)
        a = ''.join(result)
    return a


def get_matching_then_nonmatching_text(string_list, separator='', match_min_size=30, ignore='',
                                       end_characters='.!\r\n'):
    # type: (List[str], str, int, str, str, bool) -> str
    """Returns a string containing matching blocks of text in a list of strings followed by non-matching.

    Args:
        string_list (List[str]): List of strings to match
        separator (str): Separator to add between blocks of text. Defaults to ''.
        match_min_size (int): Minimum block size to match on. Defaults to 30.
        ignore (str): Any characters to ignore in matching. Defaults to ''.
        end_characters (str): End characters to look for. Defaults to '.\r\n'

    Returns:
        str: String containing matching blocks of text followed by non-matching

    """
    def add_separator_if_needed(text_list):
        if separator and len(text_list) > 0 and text_list[-1][-len(separator):] != separator:
            text_list.append(separator)

    a = string_list[0]
    for i in range(1, len(string_list)):
        b = string_list[i]
        combined_len = len(a) + len(b)
        result = get_matching_text_in_strs(a, b, match_min_size=match_min_size, ignore=ignore,
                                           end_characters=end_characters)
        new_a = a
        new_b = b
        for text in result:
            new_a = new_a.replace(text, '')
            new_b = new_b.replace(text, '')
        if new_a and new_a in a:
            pos_a = a.index(new_a)
        else:
            pos_a = combined_len
        if new_b and new_b in b:
            pos_b = b.index(new_b)
        else:
            pos_b = combined_len
        if pos_b > pos_a:
            text_1 = new_b
            pos_1 = pos_b
            text_2 = new_a
            pos_2 = pos_a
        else:
            text_1 = new_a
            pos_1 = pos_a
            text_2 = new_b
            pos_2 = pos_b
        output = list()
        pos = 0
        for text in result:
            output.append(text)
            pos += len(text)
            if text_1 and pos >= pos_1:
                add_separator_if_needed(output)
                output.append(text_1)
                pos += len(text_1)
                text_1 = None
            if text_2 and pos >= pos_2:
                add_separator_if_needed(output)
                output.append(text_2)
                pos += len(text_2)
                text_2 = None
        if text_1 and pos_1 == combined_len:
            add_separator_if_needed(output)
            output.append(text_1)
        if text_2 and pos_2 == combined_len:
            add_separator_if_needed(output)
            output.append(text_2)
        a = ''.join(output)
    return a
