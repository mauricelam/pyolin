import itertools
import json
import typing
from pyolin.ioformat import (
    AbstractParser,
    LimitReached,
    CsvParser,
    TxtParser,
    UnexpectedDataFormat,
    gen_split,
)
from pyolin.core import PluginContext
from pyolin.plugins.json import JsonParser
from pyolin.record import Record
from pyolin.util import peek_iter


class AutoParser(AbstractParser):
    """
    A parser that automatically detects the input data format.

    Supports JSON, field separated text (awk style), CSV, and TSV.
    """

    def gen_records(
        self, stream: typing.BinaryIO
    ) -> typing.Generator[Record, None, None]:
        # Note: This method returns a generator, instead of yielding by itself so that the parsing
        # logic can run eagerly and set `self.has_header` before it is used.
        try:
            gen_lines = gen_split(stream, self.record_separator, limit=4000)
            sample, gen_lines = peek_iter(gen_lines, 5)
            csv_parser = CsvParser(self.record_separator, self.field_separator)
            csv_sniffer = csv_parser.sniff_heuristic(sample)
            if csv_sniffer:
                # Update field_separator to the detected delimiter
                assert csv_parser.dialect
                self.field_separator = csv_parser.dialect.delimiter
                yield from csv_parser.gen_records_from_lines(gen_lines, csv_sniffer)
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
                        yield from json_parser.gen_records_from_json(json_object)
                        return
                    except (json.JSONDecodeError, UnexpectedDataFormat):
                        pass
                yield from TxtParser(
                    self.record_separator, self.field_separator
                ).gen_records_from_lines(gen_lines)
        except LimitReached:
            raise RuntimeError(
                "Unable to detect input format. Try specifying the input type with --input_format"
            ) from None
        except UnicodeDecodeError:
            raise AttributeError(
                "`record`-based attributes are not supported for binary inputs"
            ) from None


def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_parsers(auto=AutoParser)
