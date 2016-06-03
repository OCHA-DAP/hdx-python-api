#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Extend JSON encoding to support UserDict

Usage example: json.dumps(data, cls=EnhancedJSONEncoder)
"""
from collections import UserDict
from json import JSONEncoder

from typing import Any


class EnhancedJSONEncoder(JSONEncoder):
    """Extend JSON encoding to support UserDict

    Usage example: json.dumps(data, cls=EnhancedJSONEncoder)
    """

    def default(self, obj: Any):
        """Override default method of JSONEncoder

        Args:
            obj (Any): Any object

        Returns:
            Any: Internal object dictionary if a UserDict or result of call to JSONEncoder.default

        """
        if isinstance(obj, UserDict):
            return obj.data
        return JSONEncoder.default(self, obj)
