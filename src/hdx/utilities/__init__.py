import sys
from typing import Any

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
