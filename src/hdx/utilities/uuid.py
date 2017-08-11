# -*- coding: utf-8 -*-
"""UUID utilities"""
from typing import Optional
from uuid import UUID


def is_valid_uuid(uuid_to_test, version=4):
    # type: (str, Optional[int]) -> bool
    """
    Check if uuid_to_test is a valid UUID.

    Args:
        uuid_to_test (str): UUID to test for validity
        version (Optional[int]): UUID version. Defaults to 4.

    Returns:
        str: Current script's directory
    """
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except:
        return False
    return str(uuid_obj) == uuid_to_test
