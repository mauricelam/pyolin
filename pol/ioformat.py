import collections
import csv
from math import floor
import io
import itertools
from itertools import zip_longest
import json
import os
import re
import shutil
import sys
import textwrap


from .record import Record, Header, HasHeader
from .util import debug, _UNDEFINED_


def _gen_split(stream, delimiter):
    '''
    Read the stream "line by line", where line is defined by the delimiter.

    list(_gen_split(stream, delimiter)) is similar to stream.read().split(delimiter)
    '''
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
    binary = False

    def __init__(self, record_separator, field_separator):
        self.record_separator = record_separator or self.default_rs
        self.field_separator = field_separator or self.default_fs

    def records(self, stream):
        raise NotImplemented()


class AwkParser(AbstractParser):
    def records(self, stream):
        for recordstr in _gen_split(stream, self.record_separator):
            yield Record(*self.parserecord(recordstr), recordstr=recordstr)

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
        stream1, stream2, stream3 = itertools.tee(_gen_split(stream, self.record_separator), 3)
        sniff_sample = '\n'.join(line for line in itertools.islice(stream3, 0, 5))
        sniffer = CustomSniffer()
        dialect = sniffer.sniff(sniff_sample, delimiters=self.field_separator)
        csv_reader = csv.reader(stream2, sniffer.dialect)
        header = None
        for lineno, line in enumerate(stream1):
            if sniffer.update_dialect(line):
                csv_reader = csv.reader(
                    stream2, sniffer.dialect, delimiter=self.field_separator)
            fields = next(csv_reader)
            if lineno == 0 and sniffer.has_header(sniff_sample):
                header = Header(*fields, recordstr=line)
                yield header
            else:
                yield Record(*fields, recordstr=line, header=header)


class CsvDialectParser(AbstractParser):
    default_fs = r','

    def __init__(self, *args, dialect, **kwargs):
        super().__init__(*args, **kwargs)
        self.dialect = dialect

    def records(self, stream):
        # TODO: Sniff header?
        stream1, stream2 = itertools.tee(_gen_split(stream, self.record_separator))
        csv_reader = csv.reader(stream2, self.dialect, delimiter=self.field_separator)
        for line, fields in zip(stream1, csv_reader):
            yield Record(*fields, recordstr=line)


class JsonParser:
    binary = False

    def records(self, stream):
        records = json.load(stream)
        debug('records', records)
        header = None
        for i, r in enumerate(records):
            if not i:
                header = Header(*r.keys(), recordstr='')
                yield header
            yield Record(*r.values(), recordstr=json.dumps(r), header=header)


class BinaryParser:
    binary = True

    def records(self, stream):
        raise AttributeError('Record based attributes are not supported in binary input mode')


PARSERS = {
    'awk': AwkParser,
    'csv': CsvParser,
    'csv_excel': lambda rs, fs: CsvDialectParser(rs, fs, dialect=csv.excel),
    'csv_unix': lambda rs, fs: CsvDialectParser(rs, fs, dialect=csv.unix_dialect),
    'tsv': lambda rs, fs: CsvParser(rs, '\t'),
    'json': lambda rs, fs: JsonParser(),
    'binary': lambda rs, fs: BinaryParser(),
}


def create_parser(input_format, record_separator, field_separator):
    try:
        return PARSERS[input_format](record_separator, field_separator)
    except KeyError as e:
        raise ValueError(f'Unknown input format {input_format}') from e


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

    def _generate_header(self, num_columns):
        if num_columns == 1:
            return ['value']
        else:
            return [str(i) for i in range(num_columns)]

    def print_result(self, result, *, header=None):
        header = header or HasHeader.get(result)
        if 'pandas' in sys.modules:
            # Re-import it only if it is already imported before. If not the result can't be a
            # dataframe.
            import pandas as pd
        else:
            pd = None
        if pd and isinstance(result, pd.DataFrame):
            header = header or [str(i) for i in result.columns]
            result = (self.format_record(row) for i, row in result.iterrows())
        elif isinstance(result, collections.abc.Iterable):
            if not isinstance(result, (str, Record, tuple, bytes)):
                result = (self.format_record(r)
                          for r in result if r is not _UNDEFINED_)
                result, result_tee = itertools.tee(result)
                num_columns = len(list(next(result_tee, [])))
                header = header or self._generate_header(num_columns)
            else:
                result = (self.format_record(result),)
                num_columns = len(result[0])
                header = header or self._generate_header(num_columns)
        else:
            header = header or ['value']
            result = (self.format_record(result),)

        for line in self.format_table(result, header):
            print(line, flush=True, end='')


class AutoPrinter(Printer):
    def print_result(self, result, *, header=None):
        printer_type = 'awk'
        if isinstance(result, collections.abc.Iterable):
            if not isinstance(result, (str, Record, tuple, bytes)):
                printer_type = 'markdown'
        self._printer = PRINTERS[printer_type]()
        super().print_result(result, header=header)

    def format_table(self, table, header):
        return self._printer.format_table(table, header)


class AwkPrinter(Printer):
    def format_table(self, table, header):
        for record in table:
            yield ' '.join(record) + '\n'


class CsvPrinter(Printer):
    def __init__(self, *, header=False, delimiter=',', dialect=csv.excel):
        self.header = header
        self.delimiter = delimiter
        self.dialect = dialect

    def format_table(self, table, header):
        output = io.StringIO()
        self.writer = csv.writer(output, self.dialect, delimiter=self.delimiter)
        if self.header:
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


class MarkdownRowFormat:
    def __init__(self, widths):
        self._width_formats = ['{:%d}' % w for w in widths]
        self._row_template = '| ' + ' | '.join(self._width_formats) + ' |'
        self._cont_row_template = ': ' + ' : '.join(self._width_formats) + ' :'
        self._wrappers = [
            textwrap.TextWrapper(
                width=w, expand_tabs=False, replace_whitespace=False,
                drop_whitespace=False)
            for w in widths
        ]

    def format(self, cells):
        cell_lines = [wrapper.wrap(cell) if wrapper else [cell]
                      for wrapper, cell in zip_longest(self._wrappers, cells)]
        line_cells = zip_longest(*cell_lines, fillvalue='')
        result = ''
        for i, line_cell in enumerate(line_cells):
            # If there are extra columns that are not found in the header, also print them out.
            # While that's not valid markdown, it's better than silently discarding the values.
            extra_length = len(line_cell) - len(self._width_formats)
            if not i:
                template = self._row_template + ''.join(' {} |' for _ in range(extra_length))
                result += template.format(*line_cell) + '\n'
            else:
                template = self._cont_row_template + ''.join(' {} :' for _ in range(extra_length))
                result += self._cont_row_template.format(*line_cell) + '\n'
        return result


class MarkdownPrinter(Printer):
    def _allocate_width(self, header, table):
        if sys.stdout.isatty():
            available_width, _ = shutil.get_terminal_size((100, 24))
        else:
            available_width = int(os.getenv('POL_TABLE_WIDTH', 100))
        # Subtract number of characters used by markdown
        available_width -= 2 + 3 * (len(header) - 1) + 2
        remaining_space = available_width
        record_lens = zip(*[[len(c) for c in record] for record in table])
        lens = {i: max(len(h), *c_lens)
                for i, (h, c_lens) in enumerate(zip(header, record_lens))}
        widths = [0] * len(header)
        while lens:
            to_del = []
            for i, l in lens.items():
                if l < remaining_space / len(lens):
                    widths[i] = l
                    to_del.append(i)
            for i in to_del:
                del lens[i]
            if not to_del:
                divided = floor(remaining_space / len(lens))
                remainder = remaining_space % len(lens)
                for i in lens:
                    widths[i] = divided + 1 if i < remainder else divided
                break

            remaining_space = available_width - sum(widths)
            if remaining_space <= 0:
                break
        return [max(w, 1) for w in widths]

    def format_table(self, table, header):
        table1, table2 = itertools.tee(table)
        widths = self._allocate_width(header, itertools.islice(table2, 10))
        row_format = MarkdownRowFormat(widths)
        if not header:
            return
        yield row_format.format(header)
        yield '| ' + ' | '.join('-' * w for w in widths) + ' |\n'
        for record in table1:
            yield row_format.format(record)


class JsonPrinter(Printer):
    def format_table(self, table, header):
        def maybe_to_numeric(val):
            try:
                return int(val)
            except ValueError:
                try:
                    return float(val)
                except ValueError:
                    return val

        yield '[\n'
        for i, record in enumerate(table):
            if i:
                yield ',\n'
            yield json.dumps(
                {h: maybe_to_numeric(f) for h, f in zip(header, record)})
        yield '\n]\n'


class ReprPrinter(Printer):
    def print_result(self, result, *, header=None):
        print(repr(result))


class StrPrinter(Printer):
    def print_result(self, result, *, header=None):
        print(result)


PRINTERS = {
    'auto': AutoPrinter,
    'awk': AwkPrinter,
    'unix': AwkPrinter,
    'csv': CsvPrinter,
    'tsv': lambda: CsvPrinter(delimiter='\t'),
    'markdown': MarkdownPrinter,
    'table': MarkdownPrinter,
    'json': JsonPrinter,
    'repr': ReprPrinter,
    'str': StrPrinter,
}
