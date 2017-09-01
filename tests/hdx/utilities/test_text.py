# -*- coding: UTF-8 -*-
"""Text Processing Tests"""
from hdx.utilities.text import get_matching_text, get_matching_then_nonmatching_text


class TestText:
    a = 'The quick brown fox jumped over the lazy dog'
    b = 'The quicker brown fox leapt over the slower fox'
    c = 'The quick brown fox climbed over the lazy dog.'

    def test_get_matching_text(self):
        result = get_matching_text(TestText.a, TestText.b)
        assert result == []
        result = get_matching_text(TestText.a, TestText.b, match_min_size=10)
        assert result == [' brown fox ', ' over the ']
        result = get_matching_text(TestText.a, TestText.b, match_min_size=9, ignore=' ')
        assert result == ['The quick', 'brown fox']
        result = get_matching_text(TestText.a, TestText.c, match_min_size=5)
        assert result == ['The quick brown fox ', 'ed over the lazy dog']
        result = get_matching_text('%s.' % TestText.a, TestText.c, match_min_size=5)
        assert result == ['The quick brown fox ', 'ed over the lazy dog.']
        result = get_matching_text('%s.' % TestText.a, TestText.c, match_min_size=5, ignore='.')
        assert result == ['The quick brown fox ', 'ed over the lazy dog']

    def test_get_matching_then_nonmatching_text(self):
        l = [TestText.a, TestText.b, TestText.c]
        result = get_matching_then_nonmatching_text(l, separator='. ')
        assert result == 'The quick brown fox jumped over the lazy dog. The quicker brown fox leapt over the slower fox. The quick brown fox climbed over the lazy dog.'
        result = get_matching_then_nonmatching_text(l, separator='. ', match_min_size=10)
        assert result == ' brown fox .  over the . . . The quickjumpedlazy dog. The quickerleaptslower fox. The quickclimbedlazy dog.'
        result = get_matching_then_nonmatching_text(l, separator='. ', match_min_size=10, ignore=' ')
        assert result == 'The quick brown fox. ed over the lazy dog..  jump The quicker brown fox leapt over the slower fox.  climb'
        result = get_matching_then_nonmatching_text(l, separator='. ', match_min_size=10, ignore=' .')
        assert result == 'The quick brown fox. ed over the lazy dog.  jump. The quicker brown fox leapt over the slower fox.  climb.'
        result = get_matching_then_nonmatching_text(l, separator=' ', match_min_size=5, ignore=' ')
        assert result == 'The quick brown fox over the lazy dog     jumped   er  leapt  slower fox  climbed  .'
