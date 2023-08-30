"""Main entry point for Pyolin, the utility to easily write Python one-liners."""

import argparse
import importlib
import itertools
import sys
import json

from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Generator,
    Generic,
    Iterable,
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


class RecordScoped(LazyItem, Generic[T]):
    """An Item in the global scope that is record scoped, which will cause the
    program to be executed multiple times until the input is exhausted."""

    _on_accessed: Callable[[], None]

    def __init__(self, generator: RecordSequence, *, on_accessed: Callable[[], None]):
        super().__init__(self._get_first_time, on_accessed=on_accessed)
        self._iter = iter(generator)

    def _get_first_time(self):
        self._on_accessed()
        next(self)
        return self._val

    def __iter__(self):
        return self

    def __next__(self):
        """
        Advances to the next record in the generator. This is done manually so
        that the value can be accessed multiple times within the same
        iteration.
        """
        try:
            self._val = next(self._iter)
            return self._val
        except StopIteration as exc:
            raise NoMoreRecords() from exc


class PyolinConfig:
    """Configuration of Pyolin, available to the Pyolin program as `cfg`."""

    _parser: Optional[AbstractParser] = None
    _parser_frozen: bool = False
    header: Optional[Header] = None

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

    scope = "undecided"

    def set_scope(newscope):
        nonlocal scope
        if scope != newscope and scope != "undecided":
            raise RuntimeError(
                "Cannot access both record scoped and table scoped variables"
            )
        scope = newscope

    def table_scoped(func):
        return LazyItem(func, on_accessed=lambda: set_scope("table"))

    record_var = RecordScoped(record_seq, on_accessed=lambda: set_scope("record"))

    global_dict = ItemDict(
        {
            # Record scoped
            "record": record_var,
            "fields": record_var,
            "line": Item(lambda: record_var().source),
            # TODO: Refactor this so that I can make jsonobj "line scoped" (not record scoped), to
            #   avoid triggering the JSON parser. (JSON parser doesn't give real `.source` values)
            "jsonobj": LazyItem(lambda: json.loads(get_contents(input_))),
            # Table scoped
            "lines": table_scoped(
                lambda: StreamingSequence(r.source for r in record_seq)
            ),
            "records": table_scoped(lambda: record_seq),
            "file": table_scoped(lambda: get_contents(input_)),
            "contents": table_scoped(lambda: get_contents(input_)),
            "df": table_scoped(get_dataframe),
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
    except NoMoreRecords:
        return _UNDEFINED_, config

    if scope == "record":
        result = itertools.chain(
            (result,), (prog.exec(global_dict) for _ in record_var)
        )

    return result, config


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
