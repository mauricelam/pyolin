import re
import typing
from typing import Generator, Iterable, Optional
from pyolin.ioformat import (
    AbstractParser,
    gen_split,
)
from pyolin.core import PluginContext
from pyolin.record import Record


class TxtParser(AbstractParser):
    """A parser in the AWK style, which can be thought of as whitespace
    separated values. It splits the records by the `record_separator`, which is
    the newline charater by default, and splits the record into fields using the
    `field_separator`, which is a regex pattern that defaults to `[ \\t]+`."""

    def __init__(
        self,
        record_separator: str,
        # For TxtParser, the field separator is a regex string.
        field_separator: Optional[str],
    ):
        super().__init__(record_separator, field_separator or r"[ \t]+")

    def gen_records(self, stream: typing.BinaryIO) -> Generator[Record, None, None]:
        assert self.field_separator
        gen_lines = gen_split(stream, self.record_separator)
        return self.gen_records_from_lines(gen_lines)

    def gen_records_from_lines(
        self, gen_lines: Iterable[bytes]
    ) -> Generator[Record, None, None]:
        """Generates a record from the given iterable of lines."""
        assert self.field_separator
        try:
            for record_bytes in gen_lines:
                if record_bytes:
                    yield Record(
                        *re.split(self.field_separator, record_bytes.decode("utf-8")),
                        source=record_bytes,
                    )
                else:
                    yield Record(*(), source=record_bytes)
        except UnicodeDecodeError:
            raise AttributeError(
                "`record`-based attributes are not supported for binary inputs"
            ) from None


def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_parsers(txt=TxtParser, awk=TxtParser)
