"""Module containing parsers and printers for IO formatting."""

import abc
import collections
import collections.abc
from dataclasses import dataclass
import itertools
import re
import sys
import typing
from typing import (
    Any,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from .record import Field, Record, Header, HasHeader
from .util import _UNDEFINED_, clean_close_stdout_and_stderr
from . import header_detector

__all__ = [
    "AbstractParser",
    "new_parser",
    "PARSERS",
    "Printer",
    "new_printer",
    "PRINTERS",
]


class LimitReached(Exception):
    """Limit reached without seeing a delimiter"""

    def __init__(self, read_bytes: bytes):
        self.read_bytes = read_bytes


def gen_split(
    stream: typing.BinaryIO, delimiter: str, *, limit: Optional[int] = None
) -> Generator[bytes, None, None]:
    """
    Read the stream "line by line", where line is defined by the delimiter.

    list(_gen_split(stream, delimiter)) is similar to stream.read().split(delimiter)

    `limit` is the number of bytes to read up to. If this limit is reached, this
    generator will throw an error
    """
    buf = bytearray()
    binary_delimiter = delimiter.encode("utf-8")
    yielded = False
    i = 0
    while True:
        chunk = stream.read(1)
        i += 1
        if limit and i > limit and not yielded:
            # If no lines found when the limit is hit, raise exception
            raise LimitReached(bytes(chunk))
        if not chunk:
            if buf:
                yield buf
            break
        buf.extend(chunk)
        while True:
            match = re.search(binary_delimiter, buf)
            if not match:
                break
            yielded = True
            yield buf[: match.start()]
            buf = bytearray()


class AbstractParser(abc.ABC):
    """An abstract parser to be extended by concrete parser implementations.

    Implementations should implement `gen_records()`, which will be called
    when a record is requested from the parser."""

    def __init__(self, record_separator: str, field_separator: Optional[str]):
        self.has_header: Union[bool, None] = None
        self.record_separator = record_separator
        self.field_separator = field_separator

    def records(self, stream: typing.BinaryIO) -> Generator[Record, None, None]:
        """A generator for records by a parser.

        It delegates to `gen_records` for most of the work, but will process it
        with `header_detector` to see if the generated records has a header or
        not."""
        result = self.gen_records(stream)
        has_header = self.has_header
        if self.has_header is None:
            # Try to automatically detect whether there is a header
            result, result2 = itertools.tee(result)
            preview = itertools.islice((r for r in result2), 0, 10)
            has_header = header_detector.has_header(preview)
        if has_header:
            header = None
            for i, record in enumerate(result):
                if not i:
                    header = Header(*[r for r in record], source=record.source)
                    yield header
                else:
                    yield Record(
                        *[r for r in record], source=record.source, header=header
                    )
        else:
            yield from result

    @abc.abstractmethod
    def gen_records(self, stream: typing.BinaryIO) -> Generator[Record, None, None]:
        """
        Yields records in the format of (record, line_content)
        """
        raise NotImplementedError()


class UnexpectedDataFormat(RuntimeError):
    """Error raised when the input data format is unexpected"""


# See PluginContext.export_parsers
PARSERS = {}


def new_parser(
    input_format: str, record_separator: str, field_separator: Optional[str]
) -> AbstractParser:
    """Creates a parser from the given `input_format`."""
    try:
        return PARSERS[input_format](record_separator, field_separator)
    except KeyError as exc:
        raise ValueError(f"Unknown input format {input_format}") from exc


@dataclass
class PrinterConfig:
    header: Optional[Header] = None
    suggested_printer: Optional[str] = None


class Printer(abc.ABC):
    """A printer that defines how to turn the result of the pyolin program into output to stdout."""

    def format_value(self, value: Any) -> str:
        """Formats a "single value", as opposed to compound values like lists or iterables."""
        if isinstance(value, str):
            return value  # String is a sequence too. Handle it first
        elif isinstance(value, bytes):
            return value.decode("utf-8", "backslashreplace")
        elif isinstance(value, float):
            return f"{value:.6g}"
        else:
            return str(value)

    def format_record(self, record: Any) -> List[Union[str, Field]]:
        """Formats a record, which is a sequence of fields."""
        if isinstance(record, (str, bytes)):
            return [self.format_value(record)]
        elif isinstance(record, collections.abc.Iterable):
            if isinstance(record, dict):
                return [
                    Field(self.format_value(v), header=Field(str(k)))
                    for k, v in record.items()
                ]
            return [self.format_value(i) for i in record]
        else:
            return [self.format_value(record)]

    def _generate_header(self, first_row: Sequence[Any]) -> Sequence[str]:
        header = []
        has_real_header = False
        for i, column in enumerate(first_row):
            header_item = None
            if isinstance(column, Field):
                if column.header is not None and column.header.str:
                    header_item = column.header
                    has_real_header = True
            if header_item is None:
                if len(first_row) == 1:
                    header_item = "value"
                else:
                    header_item = str(i)
            header.append(header_item)
        return header if has_real_header else SynthesizedHeader(header)

    def print_result(self, result: Any, config: PrinterConfig):
        """Prints the result out to stdout according to the concrete
        implementation."""
        try:
            for line in self.gen_result(result, config=config):
                print(line, flush=True, end="")
        except BrokenPipeError as e:
            clean_close_stdout_and_stderr()
            sys.exit(141)

    def to_table(
        self, result: Any, *, header: Optional[Sequence[str]] = None
    ) -> Tuple[Sequence[str], Iterable[Any]]:
        """Turns the given `result` into a table format: (header, records)"""
        header = header or HasHeader.get(result)
        if "pandas" in sys.modules and isinstance(
            result, sys.modules["pandas"].DataFrame
        ):
            header = header or SynthesizedHeader([str(i) for i in result.columns])
            result = (self.format_record(row) for _, row in result.iterrows())
            return (header, result)
        elif isinstance(result, collections.abc.Iterable):
            if isinstance(result, (str, Record, tuple, bytes)):
                result = (self.format_record(result),)
                header = header or self._generate_header(result[0])
                return (header, result)
            if isinstance(result, dict):
                header = header or [str(k) for k in result.keys()]
                result = (result.values(),)
            result = (self.format_record(r) for r in result if r is not _UNDEFINED_)
            result, result_tee = itertools.tee(result)
            first_row = list(next(result_tee, []))
            header = header or self._generate_header(first_row)
            return (header, result)
        else:
            header = header or SynthesizedHeader(["value"])
            result = (self.format_record(result),)
            return (header, result)

    @abc.abstractmethod
    def gen_result(
        self, result: Any, config: PrinterConfig
    ) -> Generator[str, None, None]:
        """Generates a string to be printed to stdout. This string can be a
        partial result that is continued by the next yielded string from this
        generator."""
        raise NotImplementedError()


ListItemType = TypeVar("ListItemType")


class SynthesizedHeader(List[ListItemType]):
    """A header that is synthesized (e.g. numbered 0, 1, 2), not from actual input data"""


# See PluginContext.export_printers
PRINTERS = {}


def new_printer(output_format: str) -> Printer:
    """Creates a new printer with the given output format."""
    return PRINTERS[output_format]()
