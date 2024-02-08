"""Plugin module for printing in python built-in formats, like repr, str,
and binary."""

import sys
from typing import Any
from pyolin.core import PluginContext
from pyolin.ioformat import Printer, PrinterConfig
from pyolin.util import clean_close_stdout_and_stderr


class ReprPrinter(Printer):
    """Prints the result out using Python's `repr()` function."""

    def gen_result(self, result: Any, config: PrinterConfig):
        yield repr(result) + "\n"


class StrPrinter(Printer):
    """Prints the result out using Python's `str()` function."""

    def gen_result(self, result: Any, config: PrinterConfig):
        result = str(result)
        if result:
            yield result + "\n"


class BinaryPrinter(Printer):
    """Writes the result as binary out to stdout. This is typically used when
    redirecting the output to a file or to another program."""

    def gen_result(self, result: Any, config: PrinterConfig):
        if isinstance(result, str):
            yield bytes(result, "utf-8")
        else:
            yield bytes(result)

    def print_result(self, result: Any, config: PrinterConfig):
        try:
            for line in self.gen_result(result, config):
                sys.stdout.buffer.write(line)
        except BrokenPipeError:
            clean_close_stdout_and_stderr()
            sys.exit(141)


def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_printers(
        repr=ReprPrinter,
        str=StrPrinter,
        binary=BinaryPrinter,
    )
