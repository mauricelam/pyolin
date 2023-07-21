import abc
import collections
import collections.abc
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
from typing import Any, Callable, Generator, Iterable, Optional, Sequence, Type, Union


from .record import Record, Header, HasHeader
from .util import _UNDEFINED_, clean_close_stdout_and_stderr, debug, peek_iter
from .field import Field
from . import header_detector

__all__ = [
    "AbstractParser",
    "AutoParser",
    "AwkParser",
    "CsvParser",
    "JsonParser",
    "AutoPrinter",
    "create_parser",
    "PARSERS",
    "Printer",
    "AutoPrinter",
    "AwkPrinter",
    "CsvPrinter",
    "MarkdownPrinter",
    "JsonPrinter",
    "ReprPrinter",
    "StrPrinter",
    "BinaryPrinter",
    "new_printer",
    "PRINTERS",
]


class _LimitReached(Exception):
    """Limit reached without seeing a delimiter"""

    def __init__(self, read_bytes: bytes):
        self.read_bytes = read_bytes


def _gen_split(
    stream: io.BytesIO, delimiter: str, *, limit: Optional[int] = None
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
            raise _LimitReached(bytes(chunk))
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
    def __init__(self, record_separator: str, field_separator: str | None):
        self.has_header: bool | None = None
        self.record_separator = record_separator
        self.field_separator = field_separator

    def records(self, stream: io.BytesIO):
        result = self.gen_records(stream)
        has_header = self.has_header
        if self.has_header is None:
            # Try to automatically detect whether there is a header
            result, result2 = itertools.tee(result)
            preview = itertools.islice((r for r, l in result2), 0, 10)
            has_header = header_detector.has_header(preview)
        if has_header:
            header = None
            for i, (record, line) in enumerate(result):
                if not i:
                    header = Header(*record, recordstr=line)
                    yield header
                else:
                    yield Record(*record, recordstr=line, header=header)
        else:
            yield from (Record(*record, recordstr=line) for record, line in result)

    @abc.abstractmethod
    def gen_records(self, stream: io.BytesIO):
        """
        Yields records in the format of (tuple, str)
        """
        raise NotImplementedError()


class AutoParser(AbstractParser):
    """
    A parser that tries to automatically detect the input data format.

    Supports JSON, field separated text (awk style), CSV, and TSV.
    """

    def __init__(self, record_separator: str, field_separator: str | None):
        super().__init__(record_separator, field_separator)

    def gen_records(self, stream: io.BytesIO):
        try:
            gen_lines = _gen_split(stream, self.record_separator, limit=4000)
            sample, gen_lines = peek_iter(gen_lines, 5)
            sniffer = CustomSniffer()
            sniff_result = None
            try:
                sample_str = self.record_separator.join(
                    b.decode("utf-8") for b in sample
                )
                sniff_result = sniffer.sniff(
                    sample_str, delimiters=self.field_separator
                )
            except (csv.Error, UnicodeDecodeError) as exc:
                debug(exc)
            if sniff_result and sniff_result.delimiter in (",", "\t"):
                self.field_separator = self.field_separator or sniff_result.delimiter
                yield from CsvParser(
                    record_separator=self.record_separator,
                    field_separator=self.field_separator,
                    dialect=sniff_result,
                ).gen_records_from_lines(gen_lines, sniffer)
            else:
                # TODO: This can be JSON too
                yield from AwkParser(
                    self.record_separator, self.field_separator
                ).gen_records_from_lines(gen_lines)
        except _LimitReached as limit_reached:
            # Check is JSON
            read_bytes = limit_reached.read_bytes
            if read_bytes.startswith(b"{") or read_bytes.startswith(b"["):
                try:
                    records = json.loads(read_bytes + stream.read())
                    self.has_header = True
                    for i, r in enumerate(records):
                        if not i:
                            yield r.keys(), ""
                        yield r.values(), json.dumps(r)
                except:
                    raise RuntimeError("Unable to detect input format") from None
        except UnicodeDecodeError:
            raise AttributeError(
                "`record`-based attributes are not supported for binary inputs"
            ) from None


class AwkParser(AbstractParser):
    def __init__(self, record_separator: str, field_separator: str | None):
        super().__init__(record_separator, field_separator or r"[ \t]+")

    def gen_records(self, stream: io.BytesIO):
        assert self.field_separator
        gen_lines = _gen_split(stream, self.record_separator)
        return self.gen_records_from_lines(gen_lines)

    def gen_records_from_lines(self, gen_lines: Iterable[bytes]):
        assert self.field_separator
        for record_bytes in gen_lines:
            record_str = record_bytes.decode("utf-8")
            yield re.split(self.field_separator, record_str), record_str


class CustomSniffer(csv.Sniffer):
    def __init__(self, force_dialect: csv.Dialect | None = None):
        super().__init__()
        self._force_dialect = force_dialect
        self.dialect: csv.Dialect | Type[csv.Dialect] | None = None
        self.dialect_doublequote_decided = False

    def sniff(
        self, sample: str, delimiters: str | None = None
    ) -> csv.Dialect | type[csv.Dialect]:
        if self._force_dialect is not None:
            return self._force_dialect
        if self.dialect is not None:
            return self.dialect
        self.dialect = super().sniff(sample, delimiters=delimiters)
        self.dialect.doublequote = True
        return self.dialect

    def update_dialect(self, line):
        if self._force_dialect is not None:
            return False
        if self.dialect_doublequote_decided:
            return False
        assert self.dialect
        if re.search(r'[^\\]""', line):
            self.dialect.doublequote = True
            return False
        elif '\\"' in line:
            self.dialect.doublequote = False
            self.dialect.escapechar = "\\"
            return True


class CsvReader:
    def __init__(self, dialect: Union[str, csv.Dialect, Type[csv.Dialect]]):
        self._dialect = dialect
        self._current_line = None
        self._csv_reader = csv.reader(self, dialect)

    @property
    def dialect(self) -> Union[str, csv.Dialect, Type[csv.Dialect]]:
        return self._dialect

    @dialect.setter
    def dialect(self, dialect: Union[str, csv.Dialect, Type[csv.Dialect]]):
        self._dialect = dialect
        self._csv_reader = csv.reader(self, dialect)

    def __iter__(self):
        return self

    def __next__(self) -> str:
        assert self._current_line is not None
        return self._current_line

    def read(self, line):
        self._current_line = line
        return next(self._csv_reader)


class CsvParser(AbstractParser):
    def __init__(
        self,
        record_separator: str,
        field_separator: str | None,
        dialect: csv.Dialect | Type[csv.Dialect] | None = None,
    ):
        super().__init__(record_separator, field_separator or ",")
        self.dialect = dialect

    def gen_records(self, stream: io.BytesIO):
        gen_lines = _gen_split(stream, self.record_separator)
        sniffer = None
        if self.dialect is None:
            preview, gen_lines = peek_iter(gen_lines, 5)
            sniff_sample = self.record_separator.join(
                b.decode("utf-8") for b in preview
            )
            sniffer = CustomSniffer(force_dialect=self.dialect)
            self.dialect = sniffer.sniff(sniff_sample, delimiters=self.field_separator)
        return self.gen_records_from_lines(gen_lines, sniffer)

    def gen_records_from_lines(
        self, gen_lines: Iterable[bytes], sniffer: CustomSniffer | None
    ) -> Generator[tuple[Any, str], None, None]:
        assert self.dialect
        csv_reader = CsvReader(self.dialect)
        for line in gen_lines:
            line_str = line.decode("utf-8")
            if sniffer and sniffer.update_dialect(line_str):
                csv_reader.dialect = sniffer.dialect  # type: ignore
            fields = csv_reader.read(line_str)
            yield fields, line_str


class JsonParser(AbstractParser):
    def __init__(self, record_separator: str, field_separator: str | None):
        super().__init__(record_separator, field_separator)
        self.has_header = True

    def gen_records(self, stream: io.BytesIO):
        records = json.load(stream)
        for i, r in enumerate(records):
            if not i:
                yield r.keys(), ""
            yield r.values(), json.dumps(r)


PARSERS = {
    "auto": AutoParser,
    "awk": AwkParser,
    "csv": CsvParser,
    "csv_excel": lambda rs, fs: CsvParser(rs, fs, dialect=csv.excel),
    "csv_unix": lambda rs, fs: CsvParser(rs, fs, dialect=csv.unix_dialect),
    "tsv": lambda rs, fs: CsvParser(rs, "\t"),
    "json": JsonParser,
}


def create_parser(
    input_format, record_separator, field_separator
) -> Callable[[], AbstractParser]:
    try:
        return PARSERS[input_format](record_separator, field_separator)
    except KeyError as exc:
        raise ValueError(f"Unknown input format {input_format}") from exc


class Printer(abc.ABC):
    @abc.abstractmethod
    def format_table(self, table, header):
        raise NotImplementedError()

    def format_value(self, value):
        if isinstance(value, str):
            return value  # String is a sequence too. Handle it first
        elif isinstance(value, bytes):
            return value.decode("utf-8", "backslashreplace")
        elif isinstance(value, float):
            return "{:.6g}".format(value)
        else:
            return str(value)

    def format_record(self, record):
        if isinstance(record, (str, bytes)):
            return [self.format_value(record)]
        elif isinstance(record, collections.abc.Iterable):
            return [self.format_value(i) for i in record]
        else:
            return [self.format_value(record)]

    def _generate_header(self, first_column):
        header = []
        for i, c in enumerate(first_column):
            h = None
            if isinstance(c, Field):
                h = c.header
            if not h:
                if len(first_column) == 1:
                    h = "value"
                else:
                    h = str(i)
            header.append(h)
        return header

    def print_result(self, result, *, header=None):
        try:
            for line in self.gen_result(result, header=header):
                print(line, flush=True, end="")
        except BrokenPipeError:
            clean_close_stdout_and_stderr()
            sys.exit(141)

    def gen_result(self, result, *, header=None):
        if result is _UNDEFINED_:
            return
        header = header or HasHeader.get(result)
        if "pandas" in sys.modules:
            # Re-import it only if it is already imported before. If not the result can't be a
            # dataframe.
            import pandas as pd
        else:
            pd = None
        if pd and isinstance(result, pd.DataFrame):
            header = header or [str(i) for i in result.columns]
            result = (self.format_record(row) for _, row in result.iterrows())
        elif isinstance(result, collections.abc.Iterable):
            if not isinstance(result, (str, Record, tuple, bytes)):
                result = (self.format_record(r) for r in result if r is not _UNDEFINED_)
                result, result_tee = itertools.tee(result)
                first_column = list(next(result_tee, []))
                header = header or self._generate_header(first_column)
            else:
                result = (self.format_record(result),)
                header = header or self._generate_header(result[0])
        else:
            header = header or ["value"]
            result = (self.format_record(result),)

        yield from self.format_table(result, header)


class AutoPrinter(Printer):
    _printer: Printer

    def gen_result(self, result, *, header=None):
        printer_type = "awk"
        if isinstance(result, collections.abc.Iterable):
            if not isinstance(result, (str, Record, tuple, bytes)):
                printer_type = "markdown"
        self._printer = new_printer(printer_type)
        yield from super().gen_result(result, header=header)

    def format_table(self, table, header):
        return self._printer.format_table(table, header)


class AwkPrinter(Printer):
    def __init__(self):
        self.record_separator = "\n"
        self.field_separator = " "

    def format_table(self, table, header):
        for record in table:
            yield self.field_separator.join(record) + self.record_separator


class CsvPrinter(Printer):
    def __init__(self, *, print_header=False, delimiter=",", dialect=csv.excel):
        self.print_header = print_header
        self.delimiter = delimiter
        self.dialect = dialect

    def format_table(self, table, header):
        output = io.StringIO()
        try:
            self.writer = csv.writer(output, self.dialect, delimiter=self.delimiter)
        except csv.Error as e:
            if "unknown dialect" in str(e):
                raise RuntimeError(f'Unknown dialect "{self.dialect}"') from e
            raise RuntimeError(e) from e
        if self.print_header:
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
        self._width_formats = ["{:%d}" % w for w in widths]
        self._row_template = "| " + " | ".join(self._width_formats) + " |"
        self._cont_row_template = ": " + " : ".join(self._width_formats) + " :"
        self._wrappers = [
            textwrap.TextWrapper(
                width=w,
                expand_tabs=False,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            for w in widths
        ]

    def format(self, cells: Sequence[Any]) -> str:
        cell_lines = [
            wrapper.wrap(str(cell)) if wrapper else [cell]
            for wrapper, cell in zip_longest(self._wrappers, cells)
        ]
        line_cells = zip_longest(*cell_lines, fillvalue="")
        result = ""
        for i, line_cell in enumerate(line_cells):
            # If there are extra columns that are not found in the header, also print them out.
            # While that's not valid markdown, it's better than silently discarding the values.
            extra_length = len(line_cell) - len(self._width_formats)
            if not i:
                template = self._row_template + "".join(
                    " {} |" for _ in range(extra_length)
                )
                result += template.format(*line_cell) + "\n"
            else:
                template = self._cont_row_template + "".join(
                    " {} :" for _ in range(extra_length)
                )
                result += self._cont_row_template.format(*line_cell) + "\n"
        return result


class MarkdownPrinter(Printer):
    def _allocate_width(self, header: Sequence[str], table):
        if sys.stdout.isatty():
            available_width, _ = shutil.get_terminal_size((100, 24))
        else:
            available_width = int(os.getenv("PYOLIN_TABLE_WIDTH", "100"))
        # Subtract number of characters used by markdown
        available_width -= 2 + 3 * (len(header) - 1) + 2
        remaining_space = available_width
        record_lens = zip(*[[len(c) for c in record] for record in table])
        lens = {
            i: max(len(h), *c_lens)
            for i, (h, c_lens) in enumerate(zip(header, record_lens))
        }
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

    def format_table(self, table, header: Optional[Sequence[str]]):
        table1, table2 = itertools.tee(table)
        widths = self._allocate_width(header, itertools.islice(table2, 10))
        row_format = MarkdownRowFormat(widths)
        if not header:
            return
        yield row_format.format(header)
        yield "| " + " | ".join("-" * w for w in widths) + " |\n"
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

        yield "[\n"
        for i, record in enumerate(table):
            if i:
                yield ",\n"
            yield json.dumps({h: maybe_to_numeric(f) for h, f in zip(header, record)})
        yield "\n]\n"


class ReprPrinter(Printer):
    def gen_result(self, result, *, header=None):
        yield repr(result) + "\n"

    def format_table(self, table, header):
        raise NotImplementedError()


class StrPrinter(Printer):
    def gen_result(self, result, *, header=None):
        result = str(result)
        if result:
            yield result + "\n"

    def format_table(self, table, header):
        raise NotImplementedError()


class BinaryPrinter(Printer):
    def gen_result(self, result, *, header=None):
        if isinstance(result, str):
            yield bytes(result, "utf-8")
        else:
            yield bytes(result)

    def print_result(self, result, *, header=None):
        try:
            for line in self.gen_result(result, header=header):
                sys.stdout.buffer.write(line)
        except BrokenPipeError:
            clean_close_stdout_and_stderr()
            sys.exit(141)

    def format_table(self, table, header):
        raise NotImplementedError()


PRINTERS = {
    "auto": AutoPrinter,
    "awk": AwkPrinter,
    "unix": AwkPrinter,
    "csv": CsvPrinter,
    "tsv": lambda: CsvPrinter(delimiter="\t"),
    "markdown": MarkdownPrinter,
    "table": MarkdownPrinter,
    "json": JsonPrinter,
    "repr": ReprPrinter,
    "str": StrPrinter,
    "binary": BinaryPrinter,
}


def new_printer(output_format: str) -> Printer:
    return PRINTERS[output_format]()
