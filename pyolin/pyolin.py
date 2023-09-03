"""Main entry point for Pyolin, the utility to easily write Python one-liners."""

import argparse
from dataclasses import dataclass
import importlib
from io import TextIOWrapper
import itertools
import sys
import json

from contextlib import contextmanager
from typing import (
    Any,
    Generator,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
import typing
from hashbang import command, Argument

from .field import DeferredType
from .ioformat import (
    PARSERS,
    PRINTERS,
    AbstractParser,
    Printer,
    create_parser,
    gen_split,
    new_printer,
)
from .plugins import auto_parser, json as json_plugin
from .util import (
    LazyItem,
    Item,
    ItemDict,
    StreamingSequence,
    _UNDEFINED_,
    NoMoreRecords,
    peek_iter,
)
from .record import Header, RecordSequence
from .parser import Prog


json_plugin.register()
auto_parser.register()


@contextmanager
def get_io(
    input_: Union[typing.BinaryIO, str, None]
) -> Generator[typing.BinaryIO, None, None]:
    """Get the IO from the given input filename, or from stdin if `input_file`
    is None."""
    if isinstance(input_, str):
        mode = "rb"
        with open(input_, mode) as input_file:
            yield input_file
    elif input_ is not None:
        yield input_
    else:
        yield sys.stdin.buffer


T = TypeVar("T")
_SENTINEL = object()


class ReplayIter(Iterator[T]):

    def __init__(self, iterator):
        self._curval: Union[T, object] = _SENTINEL
        self._iter = iterator

    def __next__(self) -> T:
        self._curval = next(self._iter)
        return typing.cast(T, self._curval)

    def current_or_first_value(self) -> T:
        return typing.cast(T, self._curval) if self._curval is not _SENTINEL else next(self)

    def has_multiple_items(self) -> bool:
        preview, self._iter = peek_iter(self._iter, 2)
        return len(preview) == 2


@dataclass
class ScopeIterator:
    """An optional iterator that a pyolin program can set (typically via
    accessing a provided variable), so that the program will continue executing
    multiple times until the iterator is exhausted."""
    iterator: Optional[Iterator[Any]]
    # A name for the scope, for comparison and to display in error messages.
    name: str


class PyolinConfig:
    """Configuration of Pyolin, available to the Pyolin program as `cfg`."""

    _parser: Optional[AbstractParser] = None
    _parser_frozen: bool = False
    header: Optional[Header] = None
    _scope_iterator: Optional[ScopeIterator] = None

    def __init__(
        self,
        printer: Printer,
        record_separator: str,
        field_separator: Optional[str],
        input_format: str,
    ):
        self.printer = printer
        self._record_separator = record_separator
        self._field_separator = field_separator
        self._input_format = input_format

    @property
    def parser(self) -> AbstractParser:
        """The parser for parsing input files, if `records` or `record` is used."""
        if not self._parser:
            self._parser = self.new_parser(self._input_format)
        return self._parser

    @parser.setter
    def parser(self, value):
        if self._parser_frozen:
            raise RuntimeError("Parsing already started, cannot set parser")
        if isinstance(value, str):
            self._parser = self.new_parser(value)
        elif isinstance(value, AbstractParser):
            self._parser = value
        else:
            raise TypeError(
                f"Expect `parser` to be an `AbstractParser`. Found `{value.__class__}` instead"
            )

    def new_parser(
        self,
        parser_format: str,
        *,
        record_separator: Optional[str] = None,
        field_separator: Optional[str] = None,
    ) -> AbstractParser:
        """Create a new parser based on the given format and the current configuration."""
        return create_parser(
            parser_format,
            record_separator or self._record_separator,
            field_separator or self._field_separator,
        )

    def _freeze_parser(self) -> AbstractParser:
        self._parser_frozen = True
        return self.parser

    def set_scope(self, scope_iterator: Optional[Iterator[Any]], name: str):
        """Set the scope of the pyolin program execution.

        The scope can only be set once per pyolin program. When set, pyolin will
        execute the given program in a loop until the iterator is exhausted.
        Therefore, the pyolin program and/or the registerer of this scope should
        ensure that the iterator is advanced on every invocation."""
        if self._scope_iterator is not None and self._scope_iterator.name != name:
            raise RuntimeError(
                f"Cannot change scope from "
                f"\"{self._scope_iterator.name}\" to \"{name}\""
            )
        self._scope_iterator = ScopeIterator(scope_iterator, name)


def _execute_internal(
    prog,
    *args,
    input_: Union[str, typing.BinaryIO, None] = None,
    field_separator: Optional[str] = None,
    record_separator="\n",
    input_format="auto",
    output_format="auto",
) -> Tuple[Any, PyolinConfig]:
    prog = Prog(prog)

    try:
        config = PyolinConfig(
            new_printer(output_format), record_separator, field_separator, input_format
        )
    except KeyError:
        raise ValueError(f'Unrecognized output format "{output_format}"') from None

    def gen_records(input_file: Union[str, typing.BinaryIO, None]):
        parser = config._freeze_parser()  # pylint:disable=protected-access
        with get_io(input_file) as io_stream:
            for i, record in enumerate(parser.records(io_stream)):
                record.set_num(i)
                yield record

    def get_contents(input_file: Union[str, typing.BinaryIO, None]) -> DeferredType:
        config._freeze_parser()  # pylint:disable=protected-access
        with get_io(input_file) as io_stream:
            return DeferredType(io_stream.read())

    record_seq = RecordSequence(gen_records(input_))

    def get_dataframe():
        import pandas as pd  # pylint:disable=import-outside-toplevel

        header = [f.str for f in record_seq.header] if record_seq.header else None
        dataframe = pd.DataFrame(record_seq, columns=header)
        return dataframe.apply(pd.to_numeric, errors="ignore")  # type: ignore

    def file_scoped(func):
        return LazyItem(
            func, on_accessed=lambda: config.set_scope(None, "file")
        )

    iter_record_seq = ReplayIter(iter(record_seq))

    def access_record_var():
        config.set_scope(iter_record_seq, "record")
        try:
            return iter_record_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    def gen_lines():
        with get_io(input_) as io_stream:
            for bytearr in gen_split(io_stream, "\n"):
                yield bytearr.decode('utf-8')
    iter_line_seq = ReplayIter(gen_lines())

    def access_line_var():
        config.set_scope(iter_line_seq, "line")
        try:
            return iter_line_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    def json_seq():
        with get_io(input_) as io_stream:
            text_stream = TextIOWrapper(io_stream)
            finder = json_plugin.JsonFinder()
            while line := text_stream.readline(1024):
                yield from finder.add_input(line)

    iter_json_seq = ReplayIter(json_seq())

    def access_json():
        if config._scope_iterator or iter_json_seq.has_multiple_items():
            # Special case: Don't set scope if there is only one item, so that
            # the output of a program using `jsonobj` will not present itself as
            # a sequence.
            config.set_scope(iter_json_seq, "json")
        try:
            return iter_json_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    global_dict = ItemDict(
        {
            # Record scoped
            "record": Item(access_record_var),
            "fields": Item(access_record_var),
            "line": Item(access_line_var),
            "jsonobj": Item(access_json),
            # File scoped
            "lines": file_scoped(
                lambda: StreamingSequence(r.source for r in record_seq)
            ),
            "jsonobjs": file_scoped(json_seq),
            "records": file_scoped(lambda: record_seq),
            "file": file_scoped(lambda: get_contents(input_)),
            "contents": file_scoped(lambda: get_contents(input_)),
            "df": file_scoped(get_dataframe),
            # Other
            "filename": input_,
            "_UNDEFINED_": _UNDEFINED_,
            "new_printer": new_printer,
            "new_parser": config.new_parser,
            # Modules
            "pd": Item(lambda: importlib.import_module("pandas")),
            "np": Item(lambda: importlib.import_module("numpy")),
            "pyolin": Item(lambda: importlib.import_module("pyolin")),
            # Config (which contains writable attributes)
            "cfg": config,
        }
    )

    # Shift argv results
    sys.argv = ["pyolin", *args]

    try:
        result = prog.exec(global_dict)
        if config._scope_iterator is not None and config._scope_iterator.iterator is not None:
            result = itertools.chain(
                (result,), (prog.exec(global_dict) for _ in config._scope_iterator.iterator)
            )
        return result, config
    except NoMoreRecords:
        return _UNDEFINED_, config


def run(*args, **kwargs):
    """
    Run pyolin from another Python script. This is designed to ease the transition from
    the one-liner to a more full-fledged script file. By running pyolin in Python, you get the
    results in Python list or objects, while still keeping the input parsing and output formatting
    capabilities of pyolin. Serious scripts should migrate away from those as well, perhaps
    outputting json and then using pyolin as a data-formatting pass-through.
    """
    result, _ = _execute_internal(*args, **kwargs)
    if isinstance(result, (str, bytes)):
        return result
    if isinstance(result, Iterable):
        return list(result)
    else:
        return result


@command(
    Argument("field_separator", aliases="F"),
    Argument("input_format", choices=list(PARSERS)),
    Argument("output_format", choices=list(PRINTERS)),
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
def _command_line(
    prog,
    *_REMAINDER_,
    input_: Union[str, typing.BinaryIO, None] = None,
    field_separator=None,
    record_separator="\n",
    input_format="auto",
    output_format="auto",
):
    """
    pyolin - Python one liners to easily parse and process data in Python.

    Pyolin processes text information from stdin or a given file and evaluates
    the given input `prog` and prints the result.

    Example:
        pyolin 'record[0] + record[1] if record[2] > 50'

    In Pyolin, the input file is treated as a table, which consists of many
    records (lines). Each record is then consisted of many fields (columns).
    The separator for records and fields are configurable through the
    --record_separator and --field_separator options.

    Available variables:
      - Record scoped:
        record, fields - A tuple of the fields in the current line.
            Additionally, `record.str` gives the original string of the given
            line before processing.
        line – Alias for `record.str`.

        When referencing a variable in record scope, `prog` must not access
        any other variables in table scope. In this mode, Pyolin iterates through
        each record from the input file and prints the result of `prog`.

      - Table scoped:
        records – A sequence of records (as described in "Record scoped"
            section above).
        lines – A sequence of lines (as described in "Record scoped" section
            above).
        file, contents – Contents of the entire file as a single string.
        df – Contents of the entire file as a pandas.DataFrame. (Available
            only if pandas is installed).
      - General:
        filename – The name of the file being processed, possibly None if
            reading from stdin.
        re – The regex module.
        pd – The pandas module, if installed.
    """
    result, config = _execute_internal(
        prog,
        *_REMAINDER_,
        input_=input_,
        field_separator=field_separator,
        record_separator=record_separator,
        input_format=input_format,
        output_format=output_format,
    )
    printer = config.printer
    if not isinstance(printer, Printer):
        raise RuntimeError(
            f'printer must be an instance of Printer. Found "{printer!r}" instead'
        )
    printer.print_result(result, header=config.header)
