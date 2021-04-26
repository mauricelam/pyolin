#!/usr/bin/env python3

import argparse
import ast
import collections
import collections.abc
import functools
import importlib
import itertools
import re
import sys
import tokenize
try:
    import pandas as pd
except ImportError:
    pd = None


from contextlib import contextmanager
from hashbang import command, Argument


SCOPES = ('df', 'file', 'contents', 'records', 'lines', 'record', 'line',
          'fields')


def get_loaded_names(node):
    return {
        child.id for child in ast.walk(node)
        if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
    }


def get_scope(node):
    loaded_names = get_loaded_names(node)
    for name in SCOPES:
        if name in loaded_names:
            return name


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


def gen_records(input_file):
    with get_io(input_file) as f:
        for line in f:
            yield Record.fromstring(line.rstrip())


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
    print(*args, file=sys.stderr)


def print_result(result, scope):
    """
    Result is a sequence of strings or sequence. Sequences will be formatted to
    tab-separated.
    """
    def format_record(record):
        if isinstance(record, str):
            return record  # String is a sequence too. Handle it first
        elif isinstance(record, bytes):
            return record.decode('utf-8')
        elif isinstance(record, collections.abc.Iterable):
            return ' '.join(format_record(i) for i in record)
        else:
            return str(record)

    if pd and isinstance(result, pd.DataFrame):
        print(result)
    elif isinstance(result, (str, Record, tuple, bytes)):
        # Special cases for iterables that are treated as a single record
        print(format_record(result))
    elif isinstance(result, collections.abc.Iterable):
        formatted_generator = (
            format_record(line) for line in result if line is not None)
        for line in formatted_generator:
            print(line, flush=True)
    else:
        print(format_record(result))


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


def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer


class Record(tuple):
    def __new__(cls, args, line):
        return super().__new__(cls, tuple(args))

    def __init__(self, args, line):
        self.line = line

    @staticmethod
    def fromstring(line):
        return Record([Field(f) for f in re.split(r'[ \t]+', line)], line=line)


class LazyDict(dict):
    @memoize
    def __getitem__(self, key):
        return dict.__getitem__(self, key)()

    def __setitem__(self, key, value):
        return dict.__setitem__(self, key, lambda: value)


def _add_globals(dictionary, modules):
    for module_name in modules:
        module = importlib.import_module(module_name)
        dictionary[module_name] = module
    dictionary['re'] = re
    if pd:
        dictionary['pd'] = pd


@command(
    Argument('modules', aliases='m', append=True),
    formatter_class=argparse.RawDescriptionHelpFormatter)
def pol(prog, input_file=None, *, modules=()):
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
            Additionally, `record.line` gives the original string of the given
            line before processing.
        line – Alias for `record.line`.

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
    try:
        # Try to parse as generator expression (the common case)
        prog_ast = ast.parse(f'({prog})', mode='eval')
    except SyntaxError:
        # Try to parse as <expr> if <condition>
        prog_ast = ast.parse(f'{prog} else None', mode='eval')
    debug(ast.dump(prog_ast))
    compiled = compile(prog_ast, filename='pol_user_prog.py', mode='eval')
    scope = get_scope(prog_ast)
    if scope in ('record', 'line', 'fields'):
        def _gen_result():
            for record in gen_records(input_file):
                global_dict = {
                    'record': record,
                    'line': record.line,
                    'fields': record,
                    'filename': input_file,
                }
                _add_globals(global_dict, modules)
                yield eval(compiled, global_dict)
        result = _gen_result()
    else:
        def get_dataframe():
            if pd:
                df = pd.DataFrame(gen_records(input_file))
                df = df.apply(pd.to_numeric, errors='ignore')
                return df
            else:
                raise ModuleNotFoundError('Module pandas cannot be found')
        global_dict = LazyDict({
            'lines':
                lambda: LazySequence(r.line for r in gen_records(input_file)),
            'records': lambda: LazySequence(gen_records(input_file)),
            'filename': lambda: input_file,
            'file': lambda: get_contents(input_file),
            'contents': lambda: get_contents(input_file),
            'df': get_dataframe,
        })
        _add_globals(global_dict, modules)
        result = eval(compiled, global_dict)
    print_result(result, scope)


if __name__ == '__main__':
    pol.execute()
