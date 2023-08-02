"""Representations of a Record (a.k.a. a row) coming from a parser."""
import abc
import itertools
from itertools import zip_longest
from typing import Iterable, Optional, TypeVar

from .field import Field
from .util import StreamingSequence, cache


class HasHeader:
    """A "marker interface" marking that the implemented object has a header."""

    @staticmethod
    def get(has_header: "HasHeader") -> Optional["Header"]:
        """Gets the header from the given `has_header` object."""
        if isinstance(has_header, HasHeader):
            return has_header.header

    @property
    @abc.abstractmethod
    def header(self) -> Optional["Header"]:
        """The header for this object."""
        raise NotImplementedError()


class Record(tuple):
    """A record (a.k.a. a row) in the output result."""

    def __new__(cls, *args, recordstr="", header: Optional["Header"] = None):
        _ = recordstr  # Only used in __init__
        return super().__new__(
            cls,
            tuple(
                Field(f, header=h) for f, h in zip_longest(args, header or ())
            ),  # type: ignore
        )

    def __init__(self, *args, recordstr="", header: Optional["Header"] = None):
        _ = header, args  # Only used in __new__
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
    """A header row. Structurally this is the same as a Record, but the type
    marks it for special treatment in certain printers (e.g. in Markdown
    formatting.)"""


T = TypeVar("T")


class RecordSequence(StreamingSequence[T], HasHeader):
    """A sequence of records."""

    def __init__(self, records_iter: Iterable[Record]):
        seq1, self._seq_for_header = itertools.tee(records_iter)
        super().__init__(r for r in seq1 if not isinstance(r, Header))

    @property
    @cache
    def header(self) -> Optional[Header]:
        firstrow = next(self._seq_for_header, None)
        if isinstance(firstrow, Header):
            return firstrow
        else:
            return None
