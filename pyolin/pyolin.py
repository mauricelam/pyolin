"""Main entry point for Pyolin, the utility to easily write Python one-liners."""

import argparse
import importlib
import itertools
import sys

from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    ContextManager,
    Generator,
    Iterable,
    Optional,
    Tuple,
    Union,
)
import typing
from hashbang import command, Argument

from pyolin.core import PluginRegistration, PyolinConfig

from .field import DeferredType
from .ioformat import (
    PARSERS,
    PRINTERS,
    Printer,
    gen_split,
    new_printer,
)
from .plugins import PLUGINS
from .util import (
    LazyItem,
    Item,
    ItemDict,
    ReplayIter,
    StreamingSequence,
    _UNDEFINED_,
    NoMoreRecords,
)
from .record import RecordSequence
from .parser import Prog

PLUGIN_REGISTRATION = PluginRegistration()


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

    @contextmanager
    def input_stream() -> Generator[typing.BinaryIO, None, None]:
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

    try:
        config = PyolinConfig(
            new_printer(output_format),
            record_separator,
            field_separator,
            input_format,
        )
    except KeyError:
        raise ValueError(f'Unrecognized output format "{output_format}"') from None

    def gen_records(input_stream: Callable[[], ContextManager[typing.BinaryIO]]):
        with input_stream() as io_stream:
            parser = config._freeze_parser()  # pylint:disable=protected-access
            for i, record in enumerate(parser.records(io_stream)):
                record.set_num(i)
                yield record

    def get_contents(input_stream: Callable[[], ContextManager[typing.BinaryIO]]) -> DeferredType:
        with input_stream() as io_stream:
            config._freeze_parser()  # pylint:disable=protected-access
            return DeferredType(io_stream.read())

    record_seq = RecordSequence(gen_records(input_stream))

    def get_dataframe():
        import pandas as pd  # pylint:disable=import-outside-toplevel

        header = [f.str for f in record_seq.header] if record_seq.header else None
        dataframe = pd.DataFrame(record_seq, columns=header)
        return dataframe.apply(pd.to_numeric, errors="ignore")  # type: ignore

    def file_scoped(func):
        return LazyItem(func, on_accessed=lambda: config.set_scope(None, "file"))

    iter_record_seq = ReplayIter(iter(record_seq))

    def access_record_var():
        config.set_scope(iter_record_seq, "record")
        try:
            return iter_record_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    def gen_lines(input_stream: Callable[[], ContextManager[typing.BinaryIO]]):
        with input_stream() as io_stream:
            for bytearr in gen_split(io_stream, "\n"):
                yield bytearr.decode("utf-8")

    iter_line_seq = ReplayIter(gen_lines(input_stream))

    def access_line_var():
        config.set_scope(iter_line_seq, "line")
        try:
            return iter_line_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    for plugin in PLUGINS:
        plugin.register(PLUGIN_REGISTRATION, input_stream, config)

    global_dict = ItemDict(
        {
            **PLUGIN_REGISTRATION._globals,
            # Record scoped
            "record": Item(access_record_var),
            "fields": Item(access_record_var),
            "line": Item(access_line_var),
            # File scoped
            "lines": file_scoped(
                lambda: StreamingSequence(r.source for r in record_seq)
            ),
            "records": file_scoped(lambda: record_seq),
            "file": file_scoped(lambda: get_contents(input_stream)),
            "contents": file_scoped(lambda: get_contents(input_stream)),
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
        if (
            config._scope_iterator is not None
            and config._scope_iterator.iterator is not None
        ):
            result = itertools.chain(
                (result,),
                (prog.exec(global_dict) for _ in config._scope_iterator.iterator),
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
