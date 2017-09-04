# -*- coding: UTF-8 -*-
"""Text Processing Tests"""
from hdx.utilities.text import get_matching_text_in_strs, get_matching_then_nonmatching_text, get_matching_text


class TestText:
    a = 'The quick brown fox jumped over the lazy dog. It was so fast!'
    b = 'The quicker brown fox leapt over the slower fox. It was so fast!'
    c = 'The quick brown fox climbed over the lazy dog. It was so fast!'

    def test_get_matching_text_in_strs(self):
        result = get_matching_text_in_strs(TestText.a, TestText.b)
        assert result == []
        result = get_matching_text_in_strs(TestText.a, TestText.b, match_min_size=10)
        assert result == [' brown fox ', ' over the ', '. It was so fast!']
        result = get_matching_text_in_strs(TestText.a, TestText.b, match_min_size=9, end_characters='.!\r\n')
        assert result == ['The quick', ' brown fox ', ' over the ', ' It was so fast!']
        result = get_matching_text_in_strs(TestText.a, TestText.c, match_min_size=5)
        assert result == ['The quick brown fox ', 'ed over the lazy dog. It was so fast!']
        result = get_matching_text_in_strs(TestText.a, TestText.c, match_min_size=5, end_characters='.\r\n')
        assert result == ['The quick brown fox ', 'ed over the lazy dog.']
        result = get_matching_text_in_strs(TestText.a, TestText.c, match_min_size=5, end_characters='.!\r\n')
        assert result == ['The quick brown fox ', 'ed over the lazy dog. It was so fast!']

    def test_get_matching_text(self):
        l = [TestText.a, TestText.b, TestText.c]
        result = get_matching_text(l, match_min_size=10)
        assert result == ' brown fox  over the  It was so fast!'
        description = ['Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"People Displaced" refers to the number of people living in displacement as of the end of each year.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).', 'Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"New Displacement" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).', 'Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"New Displacement" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).']
        result = get_matching_text(description, ignore='\n', end_characters='.!')
        assert result == '''Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.

Contains data from IDMC's [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).'''

    def test_get_matching_then_nonmatching_text(self):
        l = [TestText.a, TestText.b, TestText.c]
        result = get_matching_then_nonmatching_text(l, match_min_size=10)
        assert result == ' brown fox  over the  It was so fast!The quickjumpedlazy dog.The quickerleaptslower fox.The quickclimbedlazy dog.'
        description = ['Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"People Displaced" refers to the number of people living in displacement as of the end of each year.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).', 'Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"New Displacement" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).', 'Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n"New Displacement" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\nContains data from IDMC\'s [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).']
        result = get_matching_then_nonmatching_text(description, separator='\n\n', ignore='\n')
        print(result)
        assert result == '''Internally displaced persons are defined according to the 1998 Guiding Principles (http://www.internal-displacement.org/publications/1998/ocha-guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.

"People Displaced" refers to the number of people living in displacement as of the end of each year.

"New Displacement" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.

Contains data from IDMC's [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).'''
