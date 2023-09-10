from dataclasses import dataclass
from typing import Any, Callable, ContextManager, Iterator, Optional, Protocol, Union
import typing
from pyolin.ioformat import (
    PARSERS,
    PRINTERS,
    AbstractParser,
    Printer,
    PrinterConfig,
    create_parser,
    new_printer,
)
from pyolin.record import Header
from pyolin.util import Item


class PluginContext:
    def __init__(self):
        self._globals = {}

    def register_globals(self, **global_vars: Union[Item, Any]):
        self._globals.update(global_vars)

    def export_printers(self, **printers: Callable[[], Printer]):
        """
        Export a printer type for pyolin programs to use. This function is
        intended for plugins to call to register additional printers.
        """
        PRINTERS.update(printers)

    def export_parsers(self, **parsers: Callable[[str, Optional[str]], AbstractParser]):
        """
        Export a parser type for pyolin programs to use. This function is
        intended for plugins to call to register additional parsers.

        Keyword arguments `parsers` should contain a factory function to create
        new parsers, where the parameters are `(record_separator,
        field_separator)`.
        """
        PARSERS.update(parsers)


class PyolinPlugin(Protocol):
    @staticmethod
    def register(
        ctx: PluginContext,
        input_stream: Callable[[], ContextManager[typing.BinaryIO]],
        config: 'PyolinConfig',
    ):
        ...


@dataclass
class ScopeIterator:
    """An optional iterator that a pyolin program can set (typically via
    accessing a provided variable), so that the program will continue executing
    multiple times until the iterator is exhausted."""

    iterator: Optional[Iterator[Any]]
    # A name for the scope, for comparison and to display in error messages.
    name: str


@dataclass
class PyolinConfig:
    """Configuration of Pyolin, available to the Pyolin program as `cfg`."""

    _printer: Union[str, Printer]
    _record_separator: str
    _field_separator: Optional[str]
    _input_format: str
    _parser: Optional[AbstractParser] = None
    _parser_frozen: bool = False
    header: Optional[Header] = None

    # Printer config
    _scope_iterator: Optional[ScopeIterator] = None
    suggested_printer: Optional[str] = None

    def printer_config(self) -> PrinterConfig:
        return PrinterConfig(self.header, self.suggested_printer)

    @property
    def printer(self) -> Printer:
        if isinstance(self._printer, str):
            try:
                self._printer = new_printer(self._printer)
            except KeyError:
                raise ValueError(
                    f'Unrecognized output format "{self._printer}"'
                ) from None
        return self._printer

    @printer.setter
    def printer(self, printer: Printer):
        self._printer = printer

    @property
    def parser(self) -> AbstractParser:
        """The parser for parsing input files, if `records` or `record` is used."""
        if not self._parser:
            self._parser = self.new_parser(self._input_format)
        return self._parser

    @parser.setter
    def parser(self, value):
        if self._parser_frozen:
            raise RuntimeError("Parsing already started, cannot set parser")
        if isinstance(value, str):
            self._parser = self.new_parser(value)
        elif isinstance(value, AbstractParser):
            self._parser = value
        else:
            raise TypeError(
                f"Expect `parser` to be an `AbstractParser`. Found `{value.__class__}` instead"
            )

    def new_parser(
        self,
        parser_format: str,
        *,
        record_separator: Optional[str] = None,
        field_separator: Optional[str] = None,
    ) -> AbstractParser:
        """Create a new parser based on the given format and the current configuration."""
        return create_parser(
            parser_format,
            record_separator or self._record_separator,
            field_separator or self._field_separator,
        )

    def _freeze_parser(self) -> AbstractParser:
        self._parser_frozen = True
        return self.parser

    def set_scope(self, scope_iterator: Optional[Iterator[Any]], name: str):
        """Set the scope of the pyolin program execution.

        The scope can only be set once per pyolin program. When set, pyolin will
        execute the given program in a loop until the iterator is exhausted.
        Therefore, the pyolin program and/or the registerer of this scope should
        ensure that the iterator is advanced on every invocation."""
        if self._scope_iterator is not None and self._scope_iterator.name != name:
            raise RuntimeError(
                f"Cannot change scope from "
                f'"{self._scope_iterator.name}" to "{name}"'
            )
        self._scope_iterator = ScopeIterator(scope_iterator, name)
