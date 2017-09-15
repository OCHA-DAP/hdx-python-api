# -*- coding: UTF-8 -*-
"""Dictionary Tests"""
from os import unlink
from os.path import join
from tempfile import gettempdir

import pytest

from hdx.utilities.dictandlist import merge_dictionaries, dict_diff, dict_of_lists_add, list_distribute_contents, \
    list_distribute_contents_simple, extract_list_from_list_of_dict, avg_dicts, key_value_convert, integer_key_convert, \
    integer_value_convert, float_value_convert, write_list_to_csv, read_list_from_csv


class TestDictAndList:
    def test_merge_dictionaries(self):
        d1 = {1: 1, 2: 2, 3: 3, 4: {'a': 1, 'b': 'c'}}
        d2 = {2: 6, 4: 8, 6: 9, 9: {'c': 12, 'e': 'h'}}
        d3 = {4: {'g': 3}, 5: 7, 8: {3: 12, 'k': 'b'}}
        with pytest.raises(ValueError):
            result = merge_dictionaries([d1, d2, d3])
        d2[4] = {8: '8'}
        result = merge_dictionaries([d1, d2, d3])
        assert result == {1: 1, 2: 6, 3: 3, 4: {8: '8', 'g': 3, 'b': 'c', 'a': 1}, 5: 7, 6: 9, 8: {'k': 'b', 3: 12},
                          9: {'e': 'h', 'c': 12}}
        d1 = {1: 1, 2: 2, 3: 3, 4: ['a', 'b', 'c']}
        d2 = {2: 6, 4: 8, 6: 9, 4: ['d', 'e']}
        result = merge_dictionaries([d1, d2])
        assert result == {1: 1, 2: 6, 3: 3, 4: ['d', 'e'], 6: 9}
        d1 = {1: 1, 2: 2, 3: 3, 4: ['a', 'b', 'c']}
        result = merge_dictionaries([d1, d2], merge_lists=True)
        assert result == {1: 1, 2: 6, 3: 3, 4: ['a', 'b', 'c', 'd', 'e'], 6: 9}

    def test_dict_diff(self):
        d1 = {1: 1, 2: 2, 3: 3, 4: {'a': 1, 'b': 'c'}}
        d2 = {4: {'a': 1, 'b': 'c'}, 2: 2, 3: 3, 1: 1}
        diff = dict_diff(d1, d2)
        assert diff == {}
        d2[3] = 4
        diff = dict_diff(d1, d2)
        assert diff == {3: (3, 4)}
        del d2[3]
        diff = dict_diff(d1, d2)
        assert diff == {3: (3, '<KEYNOTFOUND>')}
        d2[3] = 3
        del d1[4]
        diff = dict_diff(d1, d2)
        assert diff == {4: ('<KEYNOTFOUND>', {'a': 1, 'b': 'c'})}

    def test_dict_of_lists_add(self):
        d = dict()
        dict_of_lists_add(d, 'a', 1)
        assert d == {'a': [1]}
        dict_of_lists_add(d, 2, 'b')
        assert d == {'a': [1], 2: ['b']}
        dict_of_lists_add(d, 'a', 2)
        assert d == {'a': [1, 2], 2: ['b']}
        dict_of_lists_add(d, 2, 'c')
        assert d == {'a': [1, 2], 2: ['b', 'c']}

    def test_list_distribute_contents_simple(self):
        input_list = [3, 1, 1, 1, 2, 2]
        result = list_distribute_contents_simple(input_list)
        assert result == [1, 2, 3, 1, 2, 1]
        input_list = [('f', 1), ('d', 7), ('d', 2), ('c', 2), ('c', 5), ('c', 4), ('f', 2)]
        result = list_distribute_contents_simple(input_list, lambda x: x[0])
        assert result == [('c', 2), ('d', 7), ('f', 1), ('c', 5), ('d', 2), ('f', 2), ('c', 4)]
        input_list = [{'key': 'd', 'data': 5}, {'key': 'd', 'data': 1}, {'key': 'g', 'data': 2},
                      {'key': 'a', 'data': 2}, {'key': 'a', 'data': 3}, {'key': 'b', 'data': 5}]
        result = list_distribute_contents_simple(input_list, lambda x: x['key'])
        assert result == [{'key': 'a', 'data': 2}, {'key': 'b', 'data': 5}, {'key': 'd', 'data': 5},
                          {'key': 'g', 'data': 2}, {'key': 'a', 'data': 3}, {'key': 'd', 'data': 1}]

    def test_list_distribute_contents(self):
        input_list = [3, 1, 1, 1, 2, 2]
        result = list_distribute_contents(input_list)
        assert result == [1, 2, 1, 2, 1, 3]
        input_list = [('f', 1), ('d', 7), ('d', 2), ('c', 2), ('c', 5), ('c', 4), ('f', 2)]
        result = list_distribute_contents(input_list, lambda x: x[0])
        assert result == [('c', 2), ('d', 7), ('f', 1), ('c', 5), ('d', 2), ('c', 4), ('f', 2)]
        input_list = [{'key': 'd', 'data': 5}, {'key': 'd', 'data': 1}, {'key': 'g', 'data': 2},
                      {'key': 'a', 'data': 2}, {'key': 'a', 'data': 3}, {'key': 'b', 'data': 5}]
        result = list_distribute_contents(input_list, lambda x: x['key'])
        assert result == [{'key': 'a', 'data': 2}, {'key': 'd', 'data': 5}, {'key': 'b', 'data': 5},
                          {'key': 'a', 'data': 3}, {'key': 'd', 'data': 1}, {'key': 'g', 'data': 2}]

    def test_extract_list_from_list_of_dict(self):
        input_list = [{'key': 'd', 1: 5}, {'key': 'd', 1: 1}, {'key': 'g', 1: 2},
                      {'key': 'a', 1: 2}, {'key': 'a', 1: 3}, {'key': 'b', 1: 5}]
        result = extract_list_from_list_of_dict(input_list, 'key')
        assert result == ['d', 'd', 'g', 'a', 'a', 'b']
        result = extract_list_from_list_of_dict(input_list, 1)
        assert result == [5, 1, 2, 2, 3, 5]

    def test_key_value_convert(self):
        d1 = {1: 1, 2: 1.0, 3: 3, 4: 4}
        assert key_value_convert(d1) == d1
        d1 = {1: 2, 2: 2.0, 3: 5, 'la': 4}
        assert key_value_convert(d1, keyfn=int) == {1: 2, 2: 2.0, 3: 5, 'la': 4}
        assert key_value_convert(d1, keyfn=int, dropfailedkeys=True) == {1: 2, 2: 2.0, 3: 5}
        d1 = {1: 2, 2: 2.0, 3: 5, 4: 'la'}
        assert key_value_convert(d1, valuefn=int) == {1: 2, 2: 2.0, 3: 5, 4: 'la'}
        assert key_value_convert(d1, valuefn=int, dropfailedvalues=True) == {1: 2, 2: 2.0, 3: 5}

    def test_integer_key_convert(self):
        d1 = {1: 1, 2: 1.5, 3.5: 3, '4': 4}
        assert integer_key_convert(d1) == {1: 1, 2: 1.5, 3: 3, 4: 4}

    def test_integer_value_convert(self):
        d1 = {1: 1, 2: 1.5, 3: '3', 4: 4}
        assert integer_value_convert(d1) == {1: 1, 2: 1, 3: 3, 4: 4}

    def test_float_value_convert(self):
        d1 = {1: 1, 2: 1.5, 3: '3', 4: 4}
        assert float_value_convert(d1) == {1: 1.0, 2: 1.5, 3: 3.0, 4: 4.0}

    def test_avg_dicts(self):
        d1 = {1: 1, 2: 1.0, 3: 3, 4: 4}
        d2 = {1: 2, 2: 2.0, 3: 5, 4: 4}
        assert avg_dicts(d1, d2) == {1: 1.5, 2: 1.5, 3: 4, 4: 4}

    def test_read_write_list_to_csv(self):
        l = [[1, 2, 3, 'a'],
             [4, 5, 6, 'b'],
             [7, 8, 9, 'c']]
        folder = gettempdir()
        filename = 'test_read_write_list_to_csv.csv'
        filepath = join(folder, filename)
        assert write_list_to_csv(l, folder, filename, headers=['h1', 'h2', 'h3', 'h4']) == filepath
        newll = read_list_from_csv(folder, filename)
        newld = read_list_from_csv(folder, filename, dict_form=True, headers=1)
        unlink(filepath)
        assert newll == [['h1', 'h2', 'h3', 'h4'], ['1', '2', '3', 'a'], ['4', '5', '6', 'b'], ['7', '8', '9', 'c']]
        assert newld == [{'h1': '1', 'h2': '2', 'h4': 'a', 'h3': '3'},
                        {'h1': '4', 'h2': '5', 'h4': 'b', 'h3': '6'},
                        {'h1': '7', 'h2': '8', 'h4': 'c', 'h3': '9'}]
