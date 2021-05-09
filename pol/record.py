import itertools

from .field import Field
from .util import LazySequence, cached_property


class HasHeader:
    @staticmethod
    def get(o):
        if isinstance(o, HasHeader):
            return o.header


class Record(tuple, HasHeader):
    def __new__(cls, *args, recordstr='', header=None):
        return super().__new__(cls, tuple(Field(f) for f in args))

    def __init__(self, *args, recordstr='', header=None):
        self.str = recordstr
        self.header = header


class Header(Record):
    pass


class RecordSequence(LazySequence, HasHeader):
    def __init__(self, records_iter):
        seq1, self._seq = itertools.tee(records_iter)
        super().__init__(r for r in seq1 if not isinstance(r, Header))

    @cached_property
    def header(self):
        firstrow = next(self._seq, None)
        if isinstance(firstrow, Header):
            return firstrow
        else:
            return None
