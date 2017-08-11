# -*- coding: utf-8 -*-
"""Loading utilities for YAML, JSON etc."""

import json
from typing import List

import yaml

from hdx.utilities.dictandlist import merge_two_dictionaries, merge_dictionaries


class LoadError(Exception):
    pass


def load_and_merge_yaml(paths):
    # type: (List[str]) -> Dict
    """Load multiple YAML files and merge into one dictionary

    Args:
        paths (List[str]): Paths to YAML files

    Returns:
        Dict: Dictionary of merged YAML files

    """
    configs = [load_yaml(path) for path in paths]
    return merge_dictionaries(configs)


def load_and_merge_json(paths):
    # type: (List[str]) -> Dict
    """Load multiple JSON files and merge into one dictionary

    Args:
        paths (List[str]): Paths to JSON files

    Returns:
        Dict: Dictionary of merged JSON files

    """
    configs = [load_json(path) for path in paths]
    return merge_dictionaries(configs)


def load_yaml_into_existing_dict(data, path):
    # type: (dict, str) -> Dict
    """Merge YAML file into existing dictionary

    Args:
        data (dict): Dictionary to merge into
        path (str): YAML file to load and merge

    Returns:
        Dict: YAML file merged into dictionary
    """
    yamldict = load_yaml(path)
    return merge_two_dictionaries(data, yamldict)


def load_json_into_existing_dict(data, path):
    # type: (dict, str) -> Dict
    """Merge JSON file into existing dictionary

    Args:
        data (dict): Dictionary to merge into
        path (str): JSON file to load and merge

    Returns:
        dict: JSON file merged into dictionary
    """
    jsondict = load_json(path)
    return merge_two_dictionaries(data, jsondict)


def load_yaml(path):
    # type: (str) -> Dict
    """Load YAML file into dictionary

    Args:
        path (str): Path to YAML file

    Returns:
        Dict: Dictionary containing loaded YAML file
    """
    with open(path, 'rt') as f:
        yamldict = yaml.safe_load(f.read())
    if not yamldict:
        raise (LoadError('YAML file: %s is empty!' % path))
    return yamldict


def load_json(path):
    # type: (str) -> Dict
    """Load JSON file into dictionary

    Args:
        path (str): Path to JSON file

    Returns:
        Dict: Dictionary containing loaded JSON file
    """
    with open(path, 'rt') as f:
        jsondict = json.loads(f.read())
    if not jsondict:
        raise (LoadError('JSON file: %s is empty!' % path))
    return jsondict


def load_file_to_str(path):
    # type: (str) -> str
    """
    Load file into a string

    Args:
        path (str): Path to file

    Returns:
        str: String contenats of file

    """
    with open(path, 'rt') as f:
        string = f.read().replace('\n', '')
    if not string:
        raise LoadError('%s file is empty!' % path)
    return string
