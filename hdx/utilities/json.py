import collections
from json import JSONEncoder


class EnhancedJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, collections.UserDict):
            return obj.data
        return JSONEncoder.default(self, obj)
