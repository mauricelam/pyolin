import collections
import collections.abc
import json
from typing import Any

from .util import _UNDEFINED_


class CustomJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder that accepts more types (at the cost of less type
    safety)"""

    def iterencode(self, o: Any, _one_shot=False):
        yield from super().iterencode(_WrappedValue(o), _one_shot=_one_shot)

    def default(self, o: Any):
        if isinstance(o, _WrappedValue):
            return o.unwrap()
        if isinstance(o, collections.abc.Iterable):
            return list(o)
        try:
            return json.JSONEncoder.default(self, o)
        except TypeError:
            return repr(o)


class _WrappedValue:
    def __init__(self, value: Any):
        self.value = value

    def unwrap(self) -> Any:
        if isinstance(self.value, dict):
            return { k: _WrappedValue(v) for k, v in self.value.items() if v is not _UNDEFINED_ }
        elif isinstance(self.value, (list, tuple)):
            return [ _WrappedValue(v) for v in self.value if v is not _UNDEFINED_ ]
        elif isinstance(self.value, (str, bytes)):
            try:
                self.value = int(self.value)
            except ValueError:
                try:
                    self.value = float(self.value)
                except ValueError:
                    pass
            return self.value
        elif isinstance(self.value, collections.abc.Iterable):
            return [ _WrappedValue(v) for v in self.value if v is not _UNDEFINED_ ]
        else:
            return self.value
