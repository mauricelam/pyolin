import argparse
import ast
import importlib
import io
import itertools
import re
import sys
import textwrap

from contextlib import contextmanager
from hashbang import command, Argument

from .util import debug, BoxedItem, LazyItem, Item, ItemDict, StreamingSequence, _UNDEFINED_, NoMoreRecords
from .ioformat import *
from .record import Record, HasHeader, Header, RecordSequence
from .parser import Prog


@contextmanager
def get_io(input_file, *, binary=False):
    if input_file:
        mode = 'rb' if binary else 'r'
        with open(input_file, mode) as f:
            yield f
    else:
        yield sys.stdin


class RecordScoped(LazyItem):
    def __init__(self, generator, *, on_accessed):
        super().__init__(self._get_first_time, on_accessed=on_accessed)
        self._iter = iter(generator)

    def _get_first_time(self):
        self._on_accessed()
        next(self)
        return self._val

    def __iter__(self):
        return self

    def __next__(self):
        '''
        Advances to the next record in the generator. This is done manually so
        that the value can be accessed multiple times within the same
        iteration.
        '''
        try:
            self._val = next(self._iter)
            return self._val
        except StopIteration as e:
            raise NoMoreRecords() from e


@command(
    Argument('field_separator', aliases='F'),
    Argument('input_format', choices=list(PARSERS)),
    Argument('output_format', choices=list(PRINTERS)),
    formatter_class=argparse.RawDescriptionHelpFormatter)
def pol(prog, input_file=None, *,
        field_separator=None,
        record_separator='\n',
        input_format='awk',
        output_format='auto'):
    '''
    pol - Python one liners to easily parse and process data in Python.

    Pol processes text information from stdin or a given file and evaluates
    the given input `prog` and prints the result.

    Example:
        pol 'record[0] + record[1] if record[2] > 50'

    In pol, the input file is treated as a table, which consists of many
    records (lines). Each record is then consisted of many fields (columns).
    The separator for records and fields are configurable. (Using what???)

    Available variables:
      - Record scoped:
        record, fields - A tuple of the fields in the current line.
            Additionally, `record.str` gives the original string of the given
            line before processing.
        line – Alias for `record.str`.

        When referencing a variable in record scope, `prog` must not access
        any other variables in table scope. In this mode, pol iterates through
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
    '''
    prog = Prog(prog)
    parser_box = BoxedItem(lambda: create_parser(input_format, record_separator, field_separator))

    def gen_records():
        parser_box.frozen = True
        parser = parser_box()
        with get_io(input_file, binary=parser.binary) as f:
            for record in parser.records(f):
                yield record

    def get_contents(input_file):
        parser_box.frozen = True
        parser = parser_box()
        with get_io(input_file, binary=parser.binary) as f:
            return f.read()

    record_seq = RecordSequence(gen_records())

    def get_dataframe():
        import pandas as pd
        header = [f.str for f in record_seq.header] if record_seq.header else None
        df = pd.DataFrame(record_seq, columns=header)
        return df.apply(pd.to_numeric, errors='ignore')

    scope = 'undecided'

    def set_scope(newscope):
        nonlocal scope
        if scope != newscope and scope != 'undecided':
            raise RuntimeError('Cannot access both record scoped and table scoped variables')
        scope = newscope

    def table_scoped(func):
        return LazyItem(func, on_accessed=lambda: set_scope('table'))

    record_var = RecordScoped(record_seq, on_accessed=lambda: set_scope('record'))
    try:
        printer = PRINTERS[output_format]()
    except KeyError:
        raise ValueError(f'Unrecognized output format "{output_format}"')
    global_dict = ItemDict({
        'lines': table_scoped(lambda: StreamingSequence(r.str for r in record_seq)),
        'records': table_scoped(lambda: record_seq),
        'filename': input_file,
        'file': table_scoped(lambda: get_contents(input_file)),
        'contents': table_scoped(lambda: get_contents(input_file)),
        'df': table_scoped(get_dataframe),
        'record': record_var,
        'fields': record_var,
        'line': Item(lambda: record_var().str),
        'printer': printer,
        'parser': parser_box,
        're': re,
        'pd': Item(lambda: importlib.import_module('pandas')),
        'np': Item(lambda: importlib.import_module('numpy')),
        'Header': Header,
        '_UNDEFINED_': _UNDEFINED_,
        'header': None,

        'AutoPrinter': AutoPrinter,
        'AwkPrinter': AwkPrinter,
        'CsvPrinter': CsvPrinter,
        'MarkdownPrinter': MarkdownPrinter,
        'JsonPrinter': JsonPrinter,
        'ReprPrinter': ReprPrinter,
        'StrPrinter': StrPrinter,

        'AwkParser': AwkParser,
        'CsvParser': CsvParser,
        'CsvDialectParser': CsvDialectParser,
        'JsonParser': JsonParser,
        'binary': BinaryParser,
    })

    try:
        result = prog.exec(global_dict)
    except NoMoreRecords:
        pass
    else:
        if scope == 'record':
            result = itertools.chain((result,), (prog.exec(global_dict) for _ in record_var))

        printer = global_dict['printer']
        if not isinstance(printer, Printer):
            raise RuntimeError(
                f'printer must be an instance of Printer. Found "{repr(printer)}" instead')
        global_dict['printer'].print_result(result, header=global_dict.get('header'))
