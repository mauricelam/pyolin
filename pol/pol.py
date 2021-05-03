#!/usr/bin/env python3

import ast
import collections.abc
import csv
import functools
import importlib
import io
import itertools
from itertools import zip_longest
from math import floor
import os
import re
import shutil
import sys
import textwrap
import token
import tokenize
import traceback


from contextlib import contextmanager


pd = None


@contextmanager
def get_io(input_file):
    if input_file:
        with open(input_file, 'r') as f:
            yield f
    else:
        yield sys.stdin


def get_contents(input_file):
    with get_io(input_file) as f:
        return f.read()


def gen_split(stream, delimiter):
    buf = ''
    while True:
        chunk = stream.read(1)
        if not chunk:
            if buf:
                yield buf
            break
        buf += chunk
        while True:
            match = re.search(delimiter, buf)
            if not match:
                break
            yield buf[:match.start()]
            buf = ''


class AbstractParser:
    default_fs = r'[ \t]+'
    default_rs = r'\n'

    def __init__(self, record_separator, field_separator):
        self.record_separator = record_separator or self.default_rs
        self.field_separator = field_separator or self.default_fs

    def parserecord(self, recordstr):
        raise NotImplemented()

    def records(self, stream):
        for recordstr in gen_split(stream, self.record_separator):
            yield Record(self.parserecord(recordstr), recordstr)


class AwkParser(AbstractParser):
    def parserecord(self, recordstr):
        return re.split(self.field_separator, recordstr)


class CustomSniffer(csv.Sniffer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dialect = None

    def sniff(self, *args, **kwargs):
        if self.dialect is not None:
            return self.dialect
        self.dialect = super().sniff(*args, **kwargs)
        self.dialect.doublequote = 'undecided'  # Truthy-value
        return self.dialect

    def update_dialect(self, line):
        if self.dialect.doublequote != 'undecided':
            return False
        if re.search(r'[^\\]""', line):
            self.dialect.doublequote = True
            return False
        elif '\\"' in line:
            self.dialect.doublequote = False
            self.dialect.escapechar = '\\'
            return True


class CsvParser(AbstractParser):
    default_fs = r','

    def records(self, stream):
        stream1, stream2, stream3 = itertools.tee(
            gen_split(stream, self.record_separator), 3)
        sniff_sample = '\n'.join(
            line for line in itertools.islice(stream3, 0, 5))
        sniffer = CustomSniffer()
        dialect = sniffer.sniff(sniff_sample, delimiters=self.field_separator)
        csv_reader = csv.reader(stream2, sniffer.dialect)
        for lineno, line in enumerate(stream1):
            if sniffer.update_dialect(line):
                csv_reader = csv.reader(
                    stream2, sniffer.dialect, delimiter=self.field_separator)
            fields = next(csv_reader)
            if lineno == 0 and sniffer.has_header(sniff_sample):
                yield Header(fields, line)
            else:
                yield Record(fields, line)


class CsvDialectParser(AbstractParser):
    default_fs = r','

    def __init__(self, *args, dialect, **kwargs):
        super().__init__(*args, **kwargs)
        self.dialect = dialect

    def records(self, stream):
        stream1, stream2 = itertools.tee(
            gen_split(stream, self.record_separator))
        csv_reader = csv.reader(
            stream2, self.dialect, delimiter=self.field_separator)
        for line in stream1:
            fields = next(csv_reader)
            yield Record(fields, line)


def create_parser(input_format, record_separator, field_separator):
    if input_format == 'awk':
        return AwkParser(record_separator, field_separator)
    elif input_format == 'csv':
        return CsvParser(record_separator, field_separator)
    elif input_format == 'csv_excel':
        return CsvDialectParser(
            record_separator, field_separator, dialect=csv.excel)
    elif input_format == 'csv_unix':
        return CsvDialectParser(
            record_separator, field_separator, dialect=csv.unix_dialect)
    elif input_format == 'tsv':
        return CsvParser(record_separator, '\t')
    else:
        raise ValueError(f'Unknown input format {input_format}')


def gen_records(
        input_file, *, input_format, record_separator, field_separator):
    with get_io(input_file) as f:
        parser = create_parser(input_format, record_separator, field_separator)
        for record in parser.records(f):
            yield record


class LazySequence(collections.abc.Iterable):
    def __init__(self, iterator):
        self._iter = iterator
        self._list = None
        self._itered = False

    @property
    def list(self):
        if not self._list:
            self._list = list(iter(self))
        return self._list

    def __iter__(self):
        if self._list:
            return iter(self._list)
        # Tee the iterator so every call to iter starts from the beginning
        result, self._iter = itertools.tee(self._iter, 2)
        return result

    def __getitem__(self, key):
        if self._list:
            return self._list.__getitem__(key)
        if isinstance(key, slice):
            return itertools.islice(iter(self), key.start, key.stop, key.step)
        try:
            return next(itertools.islice(iter(self), key, key + 1))
        except StopIteration:
            raise IndexError('list index out of range')

    def __reversed__(self):
        # Not necessary, but is probably (slightly) faster than the default
        # implementation that uses __getitem__ and __len__
        return reversed(self.list)

    def __len__(self):
        return len(self.list)

    def __add__(self, other):
        return LazySequence(itertools.chain(self, other))

    def __radd__(self, other):
        return LazySequence(itertools.chain(other, self))


def debug(*args):
    if os.getenv('DEBUG'):
        print(*args, file=sys.stderr)


class Printer:
    def format_table(self, table, header):
        raise NotImplemented()

    def format_value(self, value):
        if isinstance(value, str):
            return value  # String is a sequence too. Handle it first
        elif isinstance(value, bytes):
            return value.decode('utf-8')
        elif isinstance(value, float):
            return '{:.6g}'.format(value)
        else:
            return str(value)

    def format_record(self, record):
        if isinstance(record, (str, bytes)):
            return [self.format_value(record)]
        elif isinstance(record, collections.abc.Iterable):
            return [self.format_value(i) for i in record]
        else:
            return [self.format_value(record)]

    def print_result(self, result):
        header = []
        if pd and isinstance(result, pd.DataFrame):
            header = list(result.columns)
            result = (self.format_record(row) for i, row in result.iterrows())
        elif isinstance(result, collections.abc.Iterable) \
                and not isinstance(result, (str, Record, tuple, bytes)):
            result = (self.format_record(r) for r in result if r is not None)
            result, result_tee = itertools.tee(result)
            num_columns = len(list(next(result_tee, [])))
            header = [str(i) for i in range(num_columns)]
        else:
            header = ['value']
            result = (self.format_record(result),)

        for line in self.format_table(result, header):
            print(line, flush=True, end='')


class AwkPrinter(Printer):
    def format_table(self, table, header):
        for record in table:
            yield ' '.join(record) + '\n'


class CsvPrinter(Printer):
    def __init__(self, *, header=False, delimiter=','):
        self._header = header
        self._delimiter = delimiter

    def format_table(self, table, header):
        output = io.StringIO()
        self.writer = csv.writer(output, delimiter=self._delimiter)
        if self._header:
            self.writer.writerow(header)
            yield self._pop_value(output)
        for record in table:
            self.writer.writerow(record)
            yield self._pop_value(output)

    def _pop_value(self, stringio):
        value = stringio.getvalue()
        stringio.seek(0)
        stringio.truncate(0)
        return value


class MarkdownPrinter(Printer):
    def _markdown_row(self, cells, widths, wrappers):
        cell_lines = (wrapper.wrap(cell)
                      for wrapper, cell in zip(wrappers, cells))
        line_cells = zip_longest(*cell_lines, fillvalue='')
        width_formats = ['{:%d}' % w for w in widths]
        row_template = '| ' + ' | '.join(width_formats) + ' |\n'
        cont_row_template = ': ' + ' : '.join(width_formats) + ' :\n'
        result = ''
        for line_cell in line_cells:
            result += row_template.format(*line_cell)
            row_template = cont_row_template
        return result

    def _allocate_width(self, header, table):
        if sys.stdout.isatty():
            available_width, _ = shutil.get_terminal_size((100, 24))
        else:
            available_width = 100
        # Subtract number of characters used by markdown
        available_width -= 2 + 3 * (len(header) - 1) + 2
        widths = [len(h) for h in header]
        for record in table:
            for i, c in enumerate(record):
                widths[i] = max(widths[i], len(c))
        wrap_candidates = [i for i, w in enumerate(widths)
                           if w > available_width / len(header)]
        for i in wrap_candidates:
            widths[i] = 0
        remaining_space = available_width - sum(widths)
        if wrap_candidates:
            divided_space = floor(remaining_space / len(wrap_candidates))
            remaining_space = remaining_space % len(wrap_candidates)
        else:
            divided_space = 0
        for i, wrap_candidate in enumerate(wrap_candidates):
            widths[wrap_candidate] = \
                divided_space + 1 if i < remaining_space else divided_space
        return widths

    def format_table(self, table, header):
        table1, table2 = itertools.tee(table)
        widths = self._allocate_width(header, itertools.islice(table2, 10))
        wrappers = [
            textwrap.TextWrapper(
                width=w, expand_tabs=False, replace_whitespace=False,
                drop_whitespace=False)
            for w in widths
        ]
        yield self._markdown_row(header, widths, wrappers)
        yield '| ' + ' | '.join('-' * w for w in widths) + ' |\n'
        for record in table1:
            yield self._markdown_row(record, widths, wrappers)


PRINTERS = {
    'awk': AwkPrinter(),
    'unix': AwkPrinter(),
    'csv': CsvPrinter(),
    'csv_header': CsvPrinter(header=True),
    'tsv': CsvPrinter(delimiter='\t'),
    'tsv_header': CsvPrinter(delimiter='\t', header=True),
    'markdown': MarkdownPrinter(),
    'table': MarkdownPrinter(),
}


def print_result(result, *, output_format):
    try:
        printer = PRINTERS[output_format]
    except KeyError:
        raise ValueError(f'Unrecognized output format "{output_format}"')
    printer.print_result(result)


class Field(str):

    @property
    def bool(self):
        if self.lower() in ('true', 't', 'y', 'yes', '1', 'on'):
            return True
        elif self.lower() in ('false', 'f', 'n', 'no', '0', 'off'):
            return False
        else:
            raise ValueError(f'Cannot convert "{self}" to bool')

    @property
    def int(self):
        return int(self)

    @property
    def float(self):
        return float(self)

    @property
    def str(self):
        return str(self)

    @property
    def bytes(self):
        return bytes(self)

    def _isnumber(self):
        try:
            float(self)
            return True
        except ValueError:
            return False

    def _coerce_to_number(self):
        try:
            try:
                return int(self)
            except ValueError:
                return float(self)
        except ValueError:
            raise ValueError(f'Cannot convert "{self}" to int or float')

    def _coerce_with_type_check(self, other):
        '''
        Perform numeric type coercion via type check. If we can coerce to the
        "other" type, perform the coercion. Otherwise use the super
        implementation.
        '''
        if isinstance(other, Field) and self._isnumber() and other._isnumber():
            return self._coerce_to_number(), other._coerce_to_number()
        elif isinstance(other, (int, float)):
            return self._coerce_to_number(), other
        else:
            return super(), other

    def _coerce_with_fallback(self, other):
        '''
        Perform numeric type coercion by converting self to a numeric value
        (without checking the `other` type). Falls back to the super
        implementation if the coercion failed.
        '''
        try:
            return self._coerce_assuming_numeric(other)
        except ValueError:
            return super(), other

    def _coerce_assuming_numeric(self, other):
        '''
        Perform numeric type coercion assuming both self and other can be
        successfully converted to numeric types. An exception will be thrown
        if the coercion failed.
        '''
        if isinstance(other, Field):
            other = other._coerce_to_number()
        return self._coerce_to_number(), other

    def __gt__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__gt__(modified_other)

    def __ge__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__ge__(modified_other)

    def __lt__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__lt__(modified_other)

    def __le__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__le__(modified_other)

    def __eq__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__eq__(modified_other)

    def __add__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__add__(modified_other)

    def __radd__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__radd__(modified_other)

    def __sub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__sub__(modified_other)

    def __rsub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rsub__(modified_other)

    def __mul__(self, other):
        '''
        Multiplication is one special case where the numeric operation takes
        precendence over the string operation, since the former is more common.
        '''
        modified_self, modified_other = self._coerce_with_fallback(other)
        return modified_self.__mul__(modified_other)

    def __matmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__matmul__(modified_other)

    def __truediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__truediv__(modified_other)

    def __floordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__floordiv__(modified_other)

    def __mod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__mod__(modified_other)

    def __divmod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__divmod__(modified_other)

    def __pow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__pow__(modified_other, *args)

    def __lshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__lshift__(modified_other)

    def __rshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rshift__(modified_other)

    def __and__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__and__(modified_other)

    def __xor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__xor__(modified_other)

    def __or__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__or__(modified_other)

    def __rmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rmul__(modified_other)

    def __rmatmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rmatmul__(modified_other)

    def __rtruediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rtruediv__(modified_other)

    def __rfloordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rfloordiv__(modified_other)

    def __rpow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rpow__(modified_other, *args)

    def __rlshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rlshift__(modified_other)

    def __rrshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rrshift__(modified_other)

    def __rand__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rand__(modified_other)

    def __rxor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rxor__(modified_other)

    def __ror__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__ror__(modified_other)

    def __neg__(self):
        return self._coerce_to_number().__neg__()

    def __pos__(self):
        return self._coerce_to_number()

    def __abs__(self):
        return self._coerce_to_number().__abs__()

    def __round__(self, *args):
        return self._coerce_to_number().__round__(*args)

    def __floor__(self):
        return self._coerce_to_number().__floor__()

    def __ceil__(self):
        return self._coerce_to_number().__ceil__()

    def __bytes__(self):
        return self.encode('utf-8')


class Record(tuple):
    def __new__(cls, args, recordstr):
        return super().__new__(cls, tuple(Field(f) for f in args))

    def __init__(self, args, recordstr):
        self.str = recordstr


class Header(Record):
    pass


class ImplicitVarsDict(dict):
    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if isinstance(value, LazyItem):
            return value()
        return value


class LazyItem:
    def __init__(self, func, cache=True):
        self.func = func
        self._val = None
        self._cache_state = 'no_cached_value' if cache else 'dont_cache'

    def __call__(self, *arg, **kwargs):
        if self._cache_state == 'cached':
            return self._val
        value = self.func(*arg, **kwargs)
        if self._cache_state != 'dont_cache':
            self._val = value
            self._cache_state = 'cached'
        return value


def _importpandas():
    global pd
    import pandas as pd
    return pd


def _importnumpy():
    import numpy as np
    return np


def _add_globals(dictionary, modules):
    for module_name in modules:
        module = importlib.import_module(module_name)
        dictionary[module_name] = module
    dictionary['re'] = re
    dictionary['pd'] = LazyItem(_importpandas)
    dictionary['np'] = LazyItem(_importnumpy)


def split_last_statement(tokens, prog):
    def _line_pos_to_pos(linepos):
        line, pos = linepos
        offset = 0
        for i in range(1, line):
            offset = prog.index('\n', offset) + 1
        return pos + offset

    started = False
    for tok in reversed(tokens):
        if tok.exact_type in (token.SEMI, token.NEWLINE):
            if started:
                return _line_pos_to_pos(tok.start), _line_pos_to_pos(tok.end)
        elif tok.type not in (token.ENDMARKER, token.COMMENT, token.NL):
            started = True
    return 0, 0


def parse_prog(prog):
    prog_io = io.StringIO(prog)
    tokens = list(tokenize.generate_tokens(prog_io.readline))
    split_start, split_end = split_last_statement(tokens, prog)
    prog_stmts = prog[:split_start]
    prog_expr = prog[split_end:]
    try:
        exec_statements = ast.parse(prog_stmts, mode='exec')
    except SyntaxError as e:
        raise RuntimeError(textwrap.dedent(
            f'''\
            Invalid syntax:
              {prog_stmts}
              {" "*(e.offset-1)}^'''))
    try:
        # Try to parse as generator expression (the common case)
        eval_expr = ast.parse(f'({prog_expr})', mode='eval')
    except SyntaxError as e:
        # Try to parse as <expr> if <condition>
        try:
            eval_expr = ast.parse(f'{prog_expr} else None', mode='eval')
        except SyntaxError:
            try:
                ast.parse(f'{prog_expr}', mode='exec')
            except SyntaxError:
                raise RuntimeError(textwrap.dedent(
                    f'''\
                    Invalid syntax:
                      {prog_expr}
                      {" "*(e.offset-2)}^'''))
            else:
                raise RuntimeError(textwrap.dedent(f'''\
                    Cannot evaluate value from statement:
                      {prog_expr}'''))
    debug(ast.dump(eval_expr))
    return exec_statements, eval_expr


class Scope:
    def __init__(self):
        self.scope = 'undecided'

    def set_scope(self, scope):
        if self.scope == scope:
            return False
        if self.scope != 'undecided':
            raise RuntimeError(
                'Cannot access both record scoped and table scoped variables')
        self.scope = scope
        return True


class NoMoreRecords(StopIteration):
    pass


class RecordScoped(LazyItem):
    def __init__(self, scope, generator):
        self._iter = iter(generator)
        self._scope = scope

    def __call__(self, *args, **kwargs):
        if self._scope.set_scope('record'):
            self.next()
        return self._val

    def next(self):
        '''
        Advances to the next record in the generator. This is done manually so
        that the value can be accessed multiple times within the same
        iteration.
        '''
        try:
            self._val = next(self._iter)
        except StopIteration as e:
            raise NoMoreRecords() from e


class TableScoped(LazyItem):
    def __init__(self, scope, *args):
        self._scope = scope
        super().__init__(*args)

    def __call__(self, *args, **kwargs):
        self._scope.set_scope('table')
        return super().__call__(*args, **kwargs)


class UserError(RuntimeError):

    def formatted_tb(self):
        return traceback.format_exception(
            self.__cause__,
            self.__cause__,
            self.__cause__.__traceback__.tb_next)

    def __str__(self):
        return ''.join(self.formatted_tb()).rstrip('\n')


def pol(prog, input_file=None, *,
        field_separator=None,
        record_separator='\n',
        input_format='awk',
        output_format='awk',
        modules=()):
    exec_statements, eval_expr = parse_prog(prog)
    debug('eval', ast.dump(exec_statements), ast.dump(eval_expr))
    exec_compiled = compile(exec_statements,
                            filename='pol_user_prog.py', mode='exec')
    eval_compiled = compile(eval_expr,
                            filename='pol_user_prog.py', mode='eval')
    record_seq = LazySequence(
        gen_records(
            input_file,
            input_format=input_format,
            record_separator=record_separator,
            field_separator=field_separator))

    scope = Scope()

    def record_scoped(generator):
        return RecordScoped(scope, generator)

    def table_scoped(func):
        return TableScoped(scope, func)

    def get_dataframe():
        pd = _importpandas()
        firstrow = record_seq[0]
        if isinstance(firstrow, Header):
            df = pd.DataFrame(
                record_seq[1:],
                columns=[f.str for f in firstrow])
        else:
            df = pd.DataFrame(record_seq)
        df = df.apply(pd.to_numeric, errors='ignore')
        return df

    record_var = record_scoped(record_seq)
    global_dict = ImplicitVarsDict({
        'lines': table_scoped(lambda: LazySequence(r.str for r in record_seq)),
        'records': table_scoped(lambda: record_seq),
        'filename': input_file,
        'file': table_scoped(lambda: get_contents(input_file)),
        'contents': table_scoped(lambda: get_contents(input_file)),
        'df': table_scoped(get_dataframe),
        'record': record_var,
        'line': LazyItem(lambda: record_var().str, cache=False),
        'fields': record_var,
    })
    _add_globals(global_dict, modules)
    try:
        exec(exec_compiled, global_dict)
        result = eval(eval_compiled, global_dict)
    except NoMoreRecords:
        pass
    except Exception as e:
        raise UserError() from e
    else:
        if scope.scope == 'record':
            _result = result

            def _gen_result():
                yield _result
                while True:
                    try:
                        record_var.next()
                    except NoMoreRecords:
                        break
                    exec(exec_compiled, global_dict)
                    yield eval(eval_compiled, global_dict)

            result = _gen_result()
        print_result(result, output_format=output_format)
