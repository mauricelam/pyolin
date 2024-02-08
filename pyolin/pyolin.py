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

from pyolin.core import PluginContext, PyolinConfig

from .field import DeferredType
from .ioformat import (
    PRINTERS,
    Printer,
    new_printer,
)
from .plugins import PLUGINS
from .util import (
    CachedItem,
    Item,
    ItemDict,
    ReplayIter,
    _UNDEFINED_,
    NoMoreRecords,
)
from .record import RecordSequence
from .parser import Prog

PLUGIN_CONTEXT = PluginContext()


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

    config = PyolinConfig(
        output_format,
        record_separator,
        field_separator,
        input_format,
    )

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
        return CachedItem(func, on_accessed=lambda: config.set_scope(None, "file"))

    iter_record_seq = ReplayIter(iter(record_seq))

    def access_record_var():
        config.set_scope(iter_record_seq, "record")
        try:
            return iter_record_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    for plugin in PLUGINS:
        plugin.register(PLUGIN_CONTEXT, input_stream, config)

    global_dict = ItemDict(
        {
            **PLUGIN_CONTEXT._globals,
            # Record scoped
            "record": Item(access_record_var),
            "fields": Item(access_record_var),
            # File scoped
            "records": file_scoped(lambda: record_seq),
            "file": file_scoped(lambda: get_contents(input_stream)),
            "contents": file_scoped(lambda: get_contents(input_stream)),
            "df": file_scoped(get_dataframe),
            # Other
            "filename": input_ if isinstance(input_, str) else None,
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
    Argument("input_format"),
    Argument("output_format"),
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

    Scope:
        It is possible for the Pyolin program to run multiple times over an
        iterable sequence of data, called a scope. `record` is a scope that runs
        the given program multiple times based on the parser, for example.

    Available variables:
      - Record parsing (for table-like data):
        - `records` – Parses the input data into a sequence of records according
            to `cfg.parser`, and generates this `records` sequence. Each
            `record` is a tuple (often parsed from one line) that consists of
            many fields (columns). The separator for records and fields are
            configurable through the `--record_separator` and
            `--field_separator` options.
        - `record`, `fields` – A scope that will run the given program
            iteratively for each record. Additionally, `record.source` gives the
            original string of the given line before processing.
      - Line by line
        - `lines` – A sequence of lines separated by the newline character. For
            other line separators, use `contents.split(separator)`.
        - `line` – A scoped version of `lines` that iterates over each line,
            running the Pyolin program repeatedly.
      - File scope:
        - `file`, `contents` – Contents of the entire file as a single string.
        - `df` – Contents of the entire file as a pandas.DataFrame. (Available
            only if pandas is installed).
      - JSON scope:
        - `jsonobjs` – Reads one or more concatenated JSON objects from the
            input file.
        - `jsonobj` – Scoped version of `jsonobjs`. Note that if the input data
            contains only one JSON object, the result will return a single item
            rather than a sequence. To always return a sequence, use
            `foo(jsonobj) for jsonobj in jsonobjs`, or to always return a single
            value, use `jsonobj[0]`.
      - General:
        - `filename` – The name of the file being processed, possibly None if
            reading from stdin.
        - `cfg` – The Pyolin program configuration that can configure various
            beahviors of the program
          - `cfg.header` – A tuple that contains the headers of the columns in
            the output data. This assumes the output format is a table (list of
            tuples).
            If `None` (the default) and the header cannot be inferred from the
            input data, the columns will be numbered from zero.
          - `cfg.parser` – A parser instance that is used to parse the data. Any
            changes made to this field must be made before the input file
            contents are accessed.
              See the Parsers section for more.
          - `cfg.printer` – A printer instance that determines the format of the
            output data.
              See the Printers section for more.
        - Common module aliases
            - `pd` – pandas.
            - `np` – numpy.
            - All other modules can be directly referenced by name without
              explicitly using an import statement.

    Only one scope can be accessed in a Pyolin program. An exception will be
    raised if multiple scopes are mixed.
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
    printer.print_result(result, config.printer_config())
