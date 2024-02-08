import csv
import io
import re
from typing import Any, Generator, Iterable, List, Optional, Type, Union
import typing
from pyolin.core import PluginContext
from pyolin.ioformat import AbstractParser, Printer, PrinterConfig, gen_split
from pyolin.record import Record
from pyolin.util import _UNDEFINED_, debug, peek_iter


class CustomSniffer(csv.Sniffer):
    """A CSV sniffer that detects which CSV dialect and delimiters to use."""

    def __init__(self):
        super().__init__()
        self._force_dialect: Optional[csv.Dialect] = None
        self.dialect: Union[csv.Dialect, Type[csv.Dialect], None] = None
        self.dialect_doublequote_decided = False

    def sniff(
        self, sample: str, delimiters: Optional[str] = None
    ) -> Union[csv.Dialect, Type[csv.Dialect]]:
        if self._force_dialect is not None:
            return self._force_dialect
        if self.dialect is not None:
            return self.dialect
        self.dialect = super().sniff(sample, delimiters=delimiters)
        self.dialect.doublequote = True
        return self.dialect

    def update_dialect(self, line: str) -> bool:
        """Sniffs the given line and updates the dialect accordingly."""
        if self._force_dialect is not None:
            return False
        if self.dialect_doublequote_decided:
            return False
        assert self.dialect
        if re.search(r'[^\\]""', line):
            self.dialect.doublequote = True
            return False
        if '\\"' in line:
            self.dialect.doublequote = False
            self.dialect.escapechar = "\\"
            return True
        return False


class CsvReader:
    """A CSV reader that can dynamically update which dialect it is using after
    construction."""

    def __init__(self, dialect: Union[csv.Dialect, Type[csv.Dialect]]):
        self._dialect = dialect
        self._current_line = None
        self._csv_reader = csv.reader(self, dialect)

    @property
    def dialect(self) -> Union[csv.Dialect, Type[csv.Dialect]]:
        """The dialect of the CSV."""
        return self._dialect

    @dialect.setter
    def dialect(self, dialect: Union[csv.Dialect, Type[csv.Dialect]]):
        self._dialect = dialect
        self._csv_reader = csv.reader(self, dialect)

    def __iter__(self):
        return self

    def __next__(self) -> str:
        assert self._current_line is not None
        return self._current_line

    def read(self, line) -> List[str]:
        """Reads a line with the CSV reader, returning the list of fields."""
        self._current_line = line
        return next(self._csv_reader)


class CsvParser(AbstractParser):
    """A parser for CSV format."""

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

    def gen_records(self, stream: typing.BinaryIO) -> Generator[Record, None, None]:
        gen_lines = gen_split(stream, self.record_separator)
        sniffer = None
        if self.dialect is None:
            preview, gen_lines = peek_iter(gen_lines, 5)
            sniffer = self._sniff(preview)
        return self.gen_records_from_lines(gen_lines, sniffer)

    def gen_records_from_lines(
        self, gen_lines: Iterable[bytes], sniffer: Optional[CustomSniffer]
    ) -> Generator[Record, None, None]:
        """Generates the records from a given iterable of lines."""
        assert self.dialect
        csv_reader = CsvReader(self.dialect)
        for line in gen_lines:
            line_str = line.decode("utf-8")
            if sniffer and sniffer.update_dialect(line_str):
                csv_reader.dialect = sniffer.dialect  # type: ignore
            fields = csv_reader.read(line_str)
            yield Record(*fields, source=line)


class CsvPrinter(Printer):
    """A printer that prints out the results in CSV format."""

    def __init__(self, *, print_header=False, delimiter=",", dialect=csv.excel):
        self.print_header = print_header
        self.delimiter = delimiter
        self.dialect = dialect
        self.writer = None

    def gen_result(
        self, result: Any, config: PrinterConfig
    ) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        header, table_result = self.to_table(result, header=config.header)
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


def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_parsers(
        csv=CsvParser,
        csv_excel=lambda rs, fs: CsvParser(rs, fs, dialect=csv.excel),
        csv_unix=lambda rs, fs: CsvParser(rs, fs, dialect=csv.unix_dialect),
        tsv=lambda rs, fs: CsvParser(rs, "\t"),
    )
    ctx.export_printers(
        csv=CsvPrinter,
        tsv=lambda: CsvPrinter(delimiter="\t"),
    )
