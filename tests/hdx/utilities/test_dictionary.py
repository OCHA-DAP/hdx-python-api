import pytest

from hdx.utilities.dictionary import merge_dictionaries, dict_diff


class TestDictionary():
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
