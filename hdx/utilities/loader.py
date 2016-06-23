#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Loading utilities for YAML, JSON etc."""

import inspect
import json
import os
import sys

import yaml
from typing import List, Any, Optional

from .dictionary import merge_two_dictionaries, merge_dictionaries


class LoadError(Exception):
    pass


def load_and_merge_yaml(paths: List[str]) -> dict:
    """Load multiple YAML files and merge into one dictionary

    Args:
        paths (List[str]): Paths to YAML files

    Returns:
        dict: Dictionary of merged YAML files

    """
    configs = [load_yaml(path) for path in paths]
    return merge_dictionaries(configs)


def load_and_merge_json(paths: List[str]) -> dict:
    """Load multiple JSON files and merge into one dictionary

    Args:
        paths (List[str]): Paths to JSON files

    Returns:
        dict: Dictionary of merged JSON files

    """
    configs = [load_json(path) for path in paths]
    return merge_dictionaries(configs)


def load_yaml_into_existing_dict(data: dict, path: str) -> dict:
    """Merge YAML file into existing dictionary

    Args:
        data (dict): Dictionary to merge into
        path (str): YAML file to load and merge

    Returns:
        dict: YAML file merged into dictionary
    """
    yamldict = load_yaml(path)
    return merge_two_dictionaries(data, yamldict)


def load_json_into_existing_dict(data: dict, path: str) -> dict:
    """Merge JSON file into existing dictionary

    Args:
        data (dict): Dictionary to merge into
        path (str): JSON file to load and merge

    Returns:
        dict: JSON file merged into dictionary
    """
    jsondict = load_json(path)
    return merge_two_dictionaries(data, jsondict)


def load_yaml(path: str) -> dict:
    """Load YAML file into dictionary

    Args:
        path (str): Path to YAML file

    Returns:
        dict: Dictionary containing loaded YAML file
    """
    with open(path, 'rt') as f:
        yamldict = yaml.safe_load(f.read())
    if not yamldict:
        raise (LoadError('YAML file: %s is empty!' % path))
    return yamldict


def load_json(path: str) -> dict:
    """Load JSON file into dictionary

    Args:
        path (str): Path to JSON file

    Returns:
        dict: Dictionary containing loaded JSON file
    """
    with open(path, 'rt') as f:
        jsondict = json.loads(f.read())
    if not jsondict:
        raise (LoadError('JSON file: %s is empty!' % path))
    return jsondict


def script_dir_plus_file(filename: str, pyobject: Any, follow_symlinks: Optional[bool] = True) -> str:
    """Get current script's directory and then append a filename

    Args:
        filename (str): Filename to append to directory path
        pyobject (Any): Any Python object in the script
        follow_symlinks (Optional[bool]): Follow symlinks or not. Defaults to True.

    Returns:
        str: Current script's directory and with filename appended
    """
    if getattr(sys, 'frozen', False):  # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(pyobject)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.join(os.path.dirname(path), filename)
