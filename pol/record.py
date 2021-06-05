import abc
import itertools
from itertools import zip_longest
from typing import Iterable, Optional

from .field import Field
from .util import StreamingSequence, cached_property


class HasHeader:
    @staticmethod
    def get(o):
        if isinstance(o, HasHeader):
            return o.header

    @abc.abstractproperty
    def header(self) -> Iterable[str]:
        raise NotImplementedError()


class Record(tuple):
    def __new__(cls, *args, recordstr='', header=None):
        return super().__new__(
            cls,
            tuple(Field(f, header=h) for f, h in zip_longest(args, header or ())))

    def __init__(self, *args, recordstr='', header=None):
        self.str = recordstr


class Header(Record):
    pass


class RecordSequence(StreamingSequence, HasHeader):
    def __init__(self, records_iter: Iterable[Record]):
        seq1, self._seq_for_header = itertools.tee(records_iter)
        super().__init__(r for r in seq1 if not isinstance(r, Header))

    @cached_property
    def header(self) -> Optional[Header]:
        firstrow = next(self._seq_for_header, None)
        if isinstance(firstrow, Header):
            return firstrow
        else:
            return None
