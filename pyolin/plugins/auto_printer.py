import collections
import collections.abc
import sys
from typing import Any, Generator, Optional
from pyolin.core import PluginContext
from pyolin.ioformat import Printer, PrinterConfig, new_printer
from pyolin.record import Record
from pyolin.util import is_list_like, tee_if_iterable


class AutoPrinter(Printer):
    """A printer that automatically decides which format to print the results
    in."""

    _printer: Printer

    def _infer_suitable_printer(
        self, result: Any, suggested_printer: Optional[str]
    ) -> str:
        if isinstance(result, dict):
            return "json"
        if "pandas" in sys.modules:
            if isinstance(result, sys.modules["pandas"].DataFrame):
                return "markdown"
        if suggested_printer is not None:
            return suggested_printer
        if isinstance(result, collections.abc.Iterable) and not isinstance(
            result, (str, Record, tuple, bytes)
        ):
            first_row = next(iter(result), None)
            if isinstance(first_row, (dict, collections.abc.Sequence)):
                if all(not is_list_like(cell) for cell in first_row):
                    return "markdown"
                else:
                    return "json"
            else:
                return "markdown"
        return "txt"

    def gen_result(
        self, result: Any, config: PrinterConfig
    ) -> Generator[str, None, None]:
        tee_result, result = tee_if_iterable(result)
        printer_str = self._infer_suitable_printer(tee_result, config.suggested_printer)
        self._printer = new_printer(printer_str)
        yield from self._printer.gen_result(result, config=config)

    
def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_printers(auto=AutoPrinter)