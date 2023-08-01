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
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)


from .json_encoder import CustomJsonEncoder
from .record import Record, Header, HasHeader
from .util import (
    _UNDEFINED_,
    clean_close_stdout_and_stderr,
    debug,
    is_list_like,
    peek_iter,
    tee_if_iterable,
)
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
    def __init__(self, record_separator: str, field_separator: Optional[str]):
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
    A parser that automatically detects the input data format.

    Supports JSON, field separated text (awk style), CSV, and TSV.
    """

    def gen_records(self, stream: io.BytesIO) -> Generator[tuple[Any, str], None, None]:
        # Note: This method returns a generator, instead of yielding by itself so that the parsing
        # logic can run eagerly and set `self.has_header` before it is used.
        try:
            gen_lines = _gen_split(stream, self.record_separator, limit=4000)
            sample, gen_lines = peek_iter(gen_lines, 5)
            csv_parser = CsvParser(self.record_separator, self.field_separator)
            csv_sniffer = csv_parser.sniff_heuristic(sample)
            if csv_sniffer:
                # Update field_separator to the detected delimiter
                self.field_separator = csv_parser.dialect.delimiter
                return csv_parser.gen_records_from_lines(gen_lines, csv_sniffer)
            else:
                json_parser = JsonParser(self.record_separator, self.field_separator)
                gen_lines, gen_lines_for_json = itertools.tee(gen_lines)
                sample, gen_lines_for_json = peek_iter(gen_lines_for_json, 1)
                first_char: bytes = next(iter(sample), b"")[:1]
                if first_char in (b"{", b"["):
                    try:
                        json_object = json.loads(
                            self.record_separator.encode("utf-8").join(
                                gen_lines_for_json
                            )
                        )
                        self.has_header = True
                        return json_parser.gen_records_from_json(json_object)
                    except json.JSONDecodeError:
                        pass
                return AwkParser(
                    self.record_separator, self.field_separator
                ).gen_records_from_lines(gen_lines)
        except _LimitReached:
            raise RuntimeError(
                "Unable to detect input format. Try specifying the input type with --input_format"
            ) from None
        except UnicodeDecodeError:
            raise AttributeError(
                "`record`-based attributes are not supported for binary inputs"
            ) from None


class AwkParser(AbstractParser):
    def __init__(
        self,
        record_separator: str,
        # For AwkParser, the field separator is a regex string.
        field_separator: Optional[str],
    ):
        super().__init__(record_separator, field_separator or r"[ \t]+")

    def gen_records(self, stream: io.BytesIO):
        assert self.field_separator
        gen_lines = _gen_split(stream, self.record_separator)
        return self.gen_records_from_lines(gen_lines)

    def gen_records_from_lines(self, gen_lines: Iterable[bytes]):
        assert self.field_separator
        try:
            for record_bytes in gen_lines:
                record_str = record_bytes.decode("utf-8")
                yield re.split(self.field_separator, record_str), record_str
        except UnicodeDecodeError:
            raise AttributeError(
                "`record`-based attributes are not supported for binary inputs"
            ) from None


class CustomSniffer(csv.Sniffer):
    def __init__(self):
        super().__init__()
        self._force_dialect: Optional[csv.Dialect] = None
        self.dialect: Union[csv.Dialect, Type[csv.Dialect], None] = None
        self.dialect_doublequote_decided = False

    def sniff(
        self, sample: str, delimiters: Optional[str] = None
    ) -> Union[csv.Dialect, type[csv.Dialect]]:
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
    COMMON_DELIMITERS = ",\t;"

    def __init__(
        self,
        record_separator: str,
        # For CsvParser, field_separator is a str where each character is a possible delimiter.
        field_separator: Optional[str],
        dialect: Union[csv.Dialect, Type[csv.Dialect], None] = None,
    ):
        super().__init__(record_separator, field_separator or self.COMMON_DELIMITERS)
        self.dialect = dialect

    def sniff_heuristic(self, sample: Iterable[bytes]) -> Optional[CustomSniffer]:
        """Sniffs the given sample, and returns a sniffer if the input looks
        like a CSV, or returns None otherwise.

        Compared to `_sniff`, this tries harder to guess whether the input is a
        CSV or not, whereas `_sniff` assumes the input is CSV and tries to guess
        the type."""
        assert not self.dialect
        try:
            sniffer = self._sniff(sample)
            if self.dialect and self.dialect.delimiter in self.field_separator:
                return sniffer
        except (csv.Error, UnicodeDecodeError) as exc:
            debug(exc)
        return None

    def _sniff(self, sample: Iterable[bytes]) -> Optional[CustomSniffer]:
        """Sniffs the given sample, and returns a sniffer of the best guess if
        the sniffer can determine."""
        sniffer = CustomSniffer()
        sample_str = self.record_separator.join(b.decode("utf-8") for b in sample)
        self.dialect = sniffer.sniff(sample_str, delimiters=self.field_separator)
        return sniffer

    def gen_records(self, stream: io.BytesIO):
        gen_lines = _gen_split(stream, self.record_separator)
        sniffer = None
        if self.dialect is None:
            preview, gen_lines = peek_iter(gen_lines, 5)
            sniffer = self._sniff(preview)
        return self.gen_records_from_lines(gen_lines, sniffer)

    def gen_records_from_lines(
        self, gen_lines: Iterable[bytes], sniffer: Optional[CustomSniffer]
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
    def __init__(self, record_separator: str, field_separator: Optional[str]):
        super().__init__(record_separator, field_separator)
        self.has_header = True

    def gen_records_from_json(self, json_object: Any):
        for i, record in enumerate(json_object):
            if not isinstance(record, dict):
                raise TypeError("Input is not an array of objects")
            if not i:
                yield record.keys(), ""
            yield record.values(), json.dumps(record)

    def gen_records(self, stream: io.BytesIO):
        json_object = json.load(stream)
        return self.gen_records_from_json(json_object)


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
    """A printer that defines how to turn the result of the pyolin program into output to stdout."""

    def format_value(self, value):
        """Formats a "single value", as opposed to compound values like lists or iterables."""
        if isinstance(value, str):
            return value  # String is a sequence too. Handle it first
        elif isinstance(value, bytes):
            return value.decode("utf-8", "backslashreplace")
        elif isinstance(value, float):
            return f"{value:.6g}"
        else:
            return str(value)

    def format_record(self, record):
        if isinstance(record, (str, bytes)):
            return [self.format_value(record)]
        elif isinstance(record, collections.abc.Iterable):
            if isinstance(record, dict):
                record = record.items()
            return [self.format_value(i) for i in record]
        else:
            return [self.format_value(record)]

    def _generate_header(self, first_column) -> Sequence[str]:
        header = []
        has_real_header = False
        for i, column in enumerate(first_column):
            header_item = None
            if isinstance(column, Field):
                if column.header:
                    header_item = column.header
                    has_real_header = True
            if not header_item:
                if len(first_column) == 1:
                    header_item = "value"
                else:
                    header_item = str(i)
            header.append(header_item)
        return header if has_real_header else _SynthesizedHeader(header)

    def print_result(self, result, *, header=None):
        try:
            for line in self.gen_result(result, header=header):
                print(line, flush=True, end="")
        except BrokenPipeError:
            clean_close_stdout_and_stderr()
            sys.exit(141)

    def to_table(
        self, result: Any, *, header: Optional[Sequence[str]] = None
    ) -> tuple[Sequence[str], Iterable[Any]]:
        header = header or HasHeader.get(result)
        if "pandas" in sys.modules:
            # Re-import it only if it is already imported before. If not the result can't be a
            # dataframe.
            import pandas as pd
        else:
            pd = None
        if pd and isinstance(result, pd.DataFrame):
            header = header or _SynthesizedHeader([str(i) for i in result.columns])
            result = (self.format_record(row) for _, row in result.iterrows())
            return (header, result)
        elif isinstance(result, collections.abc.Iterable):
            if isinstance(result, dict):
                result = result.items()
            if isinstance(result, (str, Record, tuple, bytes)):
                result = (self.format_record(result),)
                header = header or self._generate_header(result[0])
                return (header, result)
            result = (self.format_record(r) for r in result if r is not _UNDEFINED_)
            result, result_tee = itertools.tee(result)
            first_column = list(next(result_tee, []))
            header = header or self._generate_header(first_column)
            return (header, result)
        else:
            header = header or _SynthesizedHeader(["value"])
            result = (self.format_record(result),)
            return (header, result)

    @abc.abstractmethod
    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        raise NotImplementedError()


I = TypeVar("I")


class _SynthesizedHeader(list[I]):
    """A header that is synthesized (e.g. numbered 0, 1, 2), not from actual imput data"""


class AutoPrinter(Printer):
    _printer: Printer

    def _infer_suitable_printer(self, result: Any) -> str:
        if isinstance(result, dict):
            return "json"
        if "pandas" in sys.modules:
            import pandas as pd

            if isinstance(result, pd.DataFrame):
                return "markdown"
        if isinstance(result, collections.abc.Iterable) and not isinstance(
            result, (str, Record, tuple, bytes)
        ):
            first_row = next(iter(result), None)
            if isinstance(first_row, collections.abc.Sequence):
                if all(not is_list_like(cell) for cell in first_row):
                    return "markdown"
                else:
                    return "json"
            else:
                return "markdown"
        return "awk"

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        tee_result, result = tee_if_iterable(result)
        printer_str = self._infer_suitable_printer(tee_result)
        self._printer = new_printer(printer_str)
        yield from self._printer.gen_result(result, header=header)


class AwkPrinter(Printer):
    def __init__(self):
        self.record_separator = "\n"
        self.field_separator = " "

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        header, table = self.to_table(result, header=header)
        if not isinstance(header, _SynthesizedHeader):
            yield self.field_separator.join(header) + self.record_separator
        for record in table:
            yield self.field_separator.join(record) + self.record_separator


class CsvPrinter(Printer):
    def __init__(self, *, print_header=False, delimiter=",", dialect=csv.excel):
        self.print_header = print_header
        self.delimiter = delimiter
        self.dialect = dialect
        self.writer = None

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        header, table_result = self.to_table(result, header=header)
        output = io.StringIO()
        try:
            self.writer = csv.writer(output, self.dialect, delimiter=self.delimiter)
        except csv.Error as exc:
            if "unknown dialect" in str(exc):
                raise RuntimeError(f'Unknown dialect "{self.dialect}"') from exc
            raise RuntimeError(exc) from exc
        if self.print_header:
            self.writer.writerow(header)
            yield self._pop_value(output)
        for record in table_result:
            self.writer.writerow(record)
            yield self._pop_value(output)

    def _pop_value(self, stringio):
        value = stringio.getvalue()
        stringio.seek(0)
        stringio.truncate(0)
        return value


class _MarkdownRowFormat:
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
            for i, length in lens.items():
                if length < remaining_space / len(lens):
                    widths[i] = length
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

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        header, table_result = self.to_table(result, header=header)
        table1, table2 = itertools.tee(table_result)
        widths = self._allocate_width(header, itertools.islice(table2, 10))
        row_format = _MarkdownRowFormat(widths)
        if not header:
            return
        yield row_format.format(header)
        yield "| " + " | ".join("-" * w for w in widths) + " |\n"
        for record in table1:
            yield row_format.format(record)


class JsonPrinter(Printer):
    """Prints the results out in JSON format.

    If the result is table-like:
        if input has header row: prints it out as array of objects.
        else: prints it out in a 2D-array.
    else:
        Regular json.dumps()"""

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        if is_list_like(result):
            (peek_row,), result = peek_iter(result, 1)
            if is_list_like(peek_row):
                if not is_list_like(next(iter(peek_row), None)):
                    header, table = self.to_table(result, header=header)
                    if isinstance(header, _SynthesizedHeader):
                        yield from CustomJsonEncoder(indent=4).iterencode(table)
                        yield "\n"
                        return
                    else:
                        yield "[\n"
                        for i, record in enumerate(table):
                            if i:
                                yield ",\n"
                            yield "    "
                            yield from CustomJsonEncoder().iterencode(
                                dict(zip(header, record))
                            )
                        yield "\n]\n"
                        return
            if header:
                yield from CustomJsonEncoder(indent=4).iterencode(
                    dict(zip(header, result))
                )
                yield "\n"
                return
        yield from CustomJsonEncoder(indent=4).iterencode(result)
        yield "\n"


class ReprPrinter(Printer):
    def gen_result(self, result, *, header=None):
        yield repr(result) + "\n"


class StrPrinter(Printer):
    def gen_result(self, result, *, header=None):
        result = str(result)
        if result:
            yield result + "\n"


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
