import collections


def merge_two_dictionaries(a: dict, b: dict):
    """merges b into a and return merged result

    NOTE: tuples and arbitrary objects are not handled as it is totally ambiguous what should happen"""
    key = None
    # ## debug output
    # sys.stderr.write("DEBUG: %s to %s\n" %(b,a))
    try:
        if a is None or isinstance(a, str) or isinstance(a, str) or isinstance(a, int)\
                or isinstance(a, int) or isinstance(
            a, float):
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be only appended
            if isinstance(b, list):
                # merge lists
                a.extend(b)
            else:
                # append to list
                a.append(b)
        elif isinstance(a, dict) or isinstance(a, collections.UserDict):
            # dicts must be merged
            if isinstance(b, dict) or isinstance(b, collections.UserDict):
                for key in b:
                    if key in a:
                        a[key] = merge_two_dictionaries(a[key], b[key])
                    else:
                        a[key] = b[key]
            else:
                raise ValueError('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
        else:
            raise ValueError('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
    except TypeError as e:
        raise ValueError('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
    return a


def merge_dictionaries(dicts: list) -> dict:
    dict1 = dicts[0]
    for otherconfig in dicts[1:]:
        merge_two_dictionaries(dict1, otherconfig)
    return dict1


def dict_diff(d1: dict, d2: dict, no_key: str = '<KEYNOTFOUND>'):
    both = d1.keys() & d2.keys()
    diff = {k: (d1[k], d2[k]) for k in both if d1[k] != d2[k]}
    diff.update({k: (d1[k], no_key) for k in d1.keys() - both})
    diff.update({k: (no_key, d2[k]) for k in d2.keys() - both})
    return diff
