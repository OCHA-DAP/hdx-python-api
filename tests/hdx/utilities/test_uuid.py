# -*- coding: UTF-8 -*-
"""UUID Tests"""

from hdx.utilities import is_valid_uuid


class TestUUID:
    def test_is_valid_uuid(self):
        assert is_valid_uuid('a232') is False
        assert is_valid_uuid(True) is False
        assert is_valid_uuid(123) is False
        assert is_valid_uuid('') is False
        assert is_valid_uuid('jpsmith') is False
        assert is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a') is True
