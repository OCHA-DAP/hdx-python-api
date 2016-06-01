#!/usr/bin/env python

import inspect
import os
import sys


import json

import yaml

from .dictionary import merge_two_dictionaries, merge_dictionaries


class LoadError(Exception):
    pass


def load_and_merge_yaml(paths: list):
    configs = [load_yaml(path) for path in paths]
    return merge_dictionaries(configs)


def load_and_merge_json(paths: list):
    configs = [load_json(path) for path in paths]
    return merge_dictionaries(configs)


def load_yaml_into_existing_dict(data1: dict, path: str):
    data2 = load_yaml(path)
    return merge_two_dictionaries(data1, data2)


def load_json_into_existing_dict(data1: dict, path: str):
    data2 = load_json(path)
    return merge_two_dictionaries(data1, data2)


def load_yaml(path: str):
    file = None
    with open(path, 'rt') as f:
        file = yaml.safe_load(f.read())
    if not file:
        raise (LoadError('Configuration is empty!'))
    return file


def load_json(path: str):
    file = None
    with open(path, 'rt') as f:
        file = json.loads(f.read())
    if not file:
        raise (LoadError('Configuration is empty!'))
    return file

def script_dir_plus_file(file, object, follow_symlinks=True):
    if getattr(sys, 'frozen', False): # py2exe, PyInstaller, cx_Freeze
        path = os.path.abspath(sys.executable)
    else:
        path = inspect.getabsfile(object)
    if follow_symlinks:
        path = os.path.realpath(path)
    return os.path.join(os.path.dirname(path), file)