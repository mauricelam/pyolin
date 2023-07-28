import abc
import itertools
from itertools import zip_longest
from typing import Iterable, Optional, Sequence, TypeVar

from .field import Field
from .util import StreamingSequence, cached_property


class HasHeader:
    @staticmethod
    def get(o: 'HasHeader') -> Sequence[str]:
        if isinstance(o, HasHeader):
            return o.header

    @abc.abstractproperty
    def header(self) -> Sequence[str]:
        raise NotImplementedError()


class Record(tuple):
    def __new__(cls, *args, recordstr="", header: Optional['Header']=None):
        return super().__new__(
            cls, tuple(Field(f, header=h) for f, h in zip_longest(args, header or ()))
        )

    def __init__(self, *args, recordstr="", header: Optional['Header']=None):
        self.str = recordstr
        self.num: int = -1

    def set_num(self, num: int):
        """The index number of the record in the sequence."""
        self.num = num

    @property
    def first(self) -> bool:
        """Whether this is the first record in the sequence."""
        return self.num == 0


class Header(Record):
    pass


T = TypeVar('T')
class RecordSequence(StreamingSequence[T], HasHeader):
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
