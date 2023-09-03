import collections
import collections.abc
from io import TextIOWrapper
import json
from typing import Any, Callable, ContextManager, Generator, Iterable, Optional, Sequence, Union
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
from pyolin.core import PluginRegistration, PyolinConfig
from pyolin.record import Record
from pyolin.util import (
    _UNDEFINED_,
    Item,
    LazyItem,
    NoMoreRecords,
    ReplayIter,
    is_list_like,
    peek_iter,
)


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


JsonValue = Union[str, int, float, dict, list]


class JsonFinder:
    """A class that can take repeated inputs of string and accumulates the string value until a
    complete JSON value is read, and then returns it. This is used for "streaming" type parsing for
    a file that contains multiple concatenated JSON values, like the JSON-lines format.
    """

    def __init__(self):
        self._accumulated = ""
        self._token_stack = []

    def _peek_stack(self) -> Optional[str]:
        return self._token_stack[-1] if self._token_stack else None

    def add_input(self, s: str) -> Sequence[JsonValue]:
        parsed_values = []
        skip_next = False
        for i, c in enumerate(s):
            self._accumulated += c
            if skip_next:
                skip_next = False
                continue
            if c in ("{", "[") and self._peek_stack() != '"':
                self._token_stack.append(c)
            elif c == "}" and self._peek_stack() != '"':
                assert self._token_stack.pop() == "{"
            elif c == "]" and self._peek_stack() != '"':
                assert self._token_stack.pop() == "["
            elif c == '"':
                if self._peek_stack() == '"':
                    self._token_stack.pop()
                else:
                    self._token_stack.append('"')
            elif c == "\\":
                skip_next = True
            if not self._token_stack:
                if self._accumulated.strip("\n\r\t "):
                    parsed_values.append(json.loads(self._accumulated))
                self._accumulated = ""
        return parsed_values

    def is_exhausted(self) -> bool:
        return self._accumulated == ""


def register(
    plugin_reg: PluginRegistration,
    input_stream: Callable[[], ContextManager[typing.BinaryIO]],
    config: PyolinConfig,
):
    export_printers(json=JsonPrinter, jsonl=JsonlPrinter)
    export_parsers(json=JsonParser)

    def json_seq():
        with input_stream() as io_stream:
            text_stream = TextIOWrapper(io_stream)
            finder = JsonFinder()
            while line := text_stream.readline(1024):
                yield from finder.add_input(line)

    iter_json_seq = None

    def access_json():
        nonlocal iter_json_seq
        iter_json_seq = iter_json_seq or ReplayIter(json_seq())
        if config._scope_iterator or iter_json_seq.has_multiple_items():
            # Special case: Don't set scope if there is only one item, so that
            # the output of a program using `jsonobj` will not present itself as
            # a sequence.
            config.set_scope(iter_json_seq, "json")
        try:
            return iter_json_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords

    plugin_reg.register_global(
        "jsonobj",
        Item(access_json),
    )
    plugin_reg.register_global(
        "jsonobjs",
        LazyItem(
            json_seq,
            on_accessed=lambda: config.set_scope(None, "file"),
        ),
    )
