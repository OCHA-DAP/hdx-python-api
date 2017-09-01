import sys
from uuid import UUID

import six


def raisefrom(exc_type, message, exc):
    # type: (Any, str, BaseException) -> None
    """Call Python 3 raise from or emulate it for Python 2

    Args:
        exc_type (Any): Type of Exception
        message (str): Error message to display
        exc (BaseException): original exception

    Returns:
        None

    """
    if sys.version_info[:2] >= (3, 2):
        six.raise_from(exc_type(message), exc)
    else:
        six.reraise(exc_type, '%s - %s' % (message, exc), sys.exc_info()[2])


def is_valid_uuid(uuid_to_test, version=4):
    # type: (str, int) -> bool
    """
    Check if uuid_to_test is a valid UUID.

    Args:
        uuid_to_test (str): UUID to test for validity
        version (int): UUID version. Defaults to 4.

    Returns:
        str: Current script's directory
    """
    try:
        uuid_obj = UUID(uuid_to_test, version=version)
    except:
        return False
    return str(uuid_obj) == uuid_to_test
