import json

import yaml

from .dictionary import merge_two_dictionaries, merge_dictionaries


class LoadError(Exception):
    pass


def load_data(input_type: str, input_data):
    input_type = input_type.lower()
    if input_type == "yaml":
        return load_yaml(input_data)
    elif input_type == "json":
        return load_json(input_data)
    elif input_type == "dict":
        return input_data
    else:
        raise LoadError("input_type %s not supported!" % input_type)


def load_and_merge_data(input_type: str, paths: list):
    configs = [load_data(input_type, path) for path in paths]
    return merge_dictionaries(configs)


def load_data_into_existing_dict(data1: dict, input_type: str, input_data):
    data2 = load_data(input_type, input_data)
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
