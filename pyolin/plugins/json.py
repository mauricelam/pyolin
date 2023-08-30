import collections
import collections.abc
import json
from typing import Any, Generator, Iterable, Optional
import typing

from pyolin.ioformat import (
    AbstractParser,
    SynthesizedHeader,
    Printer,
    UnexpectedDataFormat,
    export_parsers,
    export_printers,
    gen_split,
)
from pyolin.record import Record
from pyolin.util import _UNDEFINED_, is_list_like, peek_iter


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
                if all(
                    not is_list_like(field) and not isinstance(field, dict)
                    for field in peek_row
                ):
                    header, table = self.to_table(result, header=header)
                    if isinstance(header, SynthesizedHeader):
                        yield from _CustomJsonEncoder(indent=2).iterencode(table)
                        yield "\n"
                        return
                    else:
                        yield "[\n"
                        for i, record in enumerate(table):
                            if i:
                                yield ",\n"
                            yield "    "
                            yield from _CustomJsonEncoder().iterencode(
                                dict(zip(header, record))
                            )
                        yield "\n]\n"
                        return
            if header:
                yield from _CustomJsonEncoder(indent=2).iterencode(
                    dict(zip(header, result))
                )
                yield "\n"
                return
        yield from _CustomJsonEncoder(indent=2).iterencode(result)
        yield "\n"


class JsonlPrinter(Printer):
    """Prints the results out in JSON-lines format.

    For each item in the result, if the item is table-like:
        if input has header row: prints it out as array of objects.
        else: prints it out in a 2D-array.
    else:
        Regular json.dumps()"""

    def gen_result(self, result: Any, *, header=None) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        encoder = _CustomJsonEncoder()
        gen_json = None
        if is_list_like(result):
            (peek_row,), result = peek_iter(result, 1)
            if is_list_like(peek_row):
                if all(not is_list_like(field) for field in peek_row):
                    header, table = self.to_table(result, header=header)
                    if isinstance(header, SynthesizedHeader):
                        gen_json = table
                    else:
                        gen_json = (dict(zip(header, record)) for record in table)
            if not gen_json:
                gen_json = result
            for line in gen_json:
                yield from encoder.iterencode(line)
                yield "\n"
        else:
            raise RuntimeError("Cannot print non-list-like output to as JSONL")


class _CustomJsonEncoder(json.JSONEncoder):
    """Custom JSON encoder that accepts more types (at the cost of less type
    safety)"""

    def iterencode(self, o: Any, _one_shot=False):
        yield from super().iterencode(_WrappedValue(o), _one_shot=_one_shot)

    def default(self, o: Any):
        if isinstance(o, _WrappedValue):
            return o.unwrap()
        if isinstance(o, collections.abc.Iterable):
            return list(o)
        try:
            return json.JSONEncoder.default(self, o)
        except TypeError:
            return repr(o)


class _WrappedValue:
    """
    A value wrapper to allow us to add custom JSON encoding logic in `JSONEncoder.default()`.
    """

    def __init__(self, value: Any):
        self.value = value

    def unwrap(self) -> Any:
        if isinstance(self.value, dict):
            return {
                k: _WrappedValue(v)
                for k, v in self.value.items()
                if v is not _UNDEFINED_
            }
        elif isinstance(self.value, (list, tuple)):
            return [_WrappedValue(v) for v in self.value if v is not _UNDEFINED_]
        elif isinstance(self.value, (str, bytes)):
            try:
                self.value = int(self.value)
            except ValueError:
                try:
                    self.value = float(self.value)
                except ValueError:
                    pass
            return self.value
        elif isinstance(self.value, collections.abc.Iterable):
            return [_WrappedValue(v) for v in self.value if v is not _UNDEFINED_]
        else:
            return self.value


class JsonParser(AbstractParser):
    """A parser that parses table-like JSON objects. A JSON object is table-like
    if it is an array of JSON objects, where each object is in the format `{
    "column_name": value }."""

    def __init__(self, record_separator: str, field_separator: Optional[str]):
        super().__init__(record_separator, field_separator)
        self.has_header = True

    def gen_records_from_json(
        self, json_object: Iterable[Any]
    ) -> Generator[Record, None, None]:
        """Generates the records from a given iterable of JSON objects."""
        for i, record in enumerate(json_object):
            if not isinstance(record, dict):
                raise UnexpectedDataFormat("Input is not an array of objects")
            if not i:
                yield Record(*record.keys(), source=b"")
            yield Record(*record.values(), source=json.dumps(record).encode("utf-8"))

    def gen_records(self, stream: typing.BinaryIO):
        lines = gen_split(stream, self.record_separator)
        try:
            yield from self.gen_records_from_json(json.loads(line) for line in lines)
        except json.JSONDecodeError:
            stream.seek(0)
            json_object = json.load(stream)
            yield from self.gen_records_from_json(json_object)


def register():
    export_printers(json=JsonPrinter, jsonl=JsonlPrinter)
    export_parsers(json=JsonParser)
