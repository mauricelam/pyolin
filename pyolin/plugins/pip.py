"""
This plugin provides the `pip` global variable, which allows to easily install
or uninstall additional packages in the virtual env pyolin is running in.

e.g.
    pyolin pip install yaml
"""

import sys
from typing import Any, Callable, ContextManager, Generator
import typing
from pyolin.core import PluginContext, PyolinConfig
import subprocess
from pyolin.ioformat import Printer, PrinterConfig

from pyolin.util import Item, clean_close_stdout_and_stderr


PIP = type("PIP", (object,), {})()


class PipCustomPrinter(Printer):
    def print_result(self, result: Any, config: PrinterConfig):
        if result is not PIP:
            raise RuntimeError(
                "`pip` variable should be returned unmodified in the pyolin the program.\n"
                "e.g. `pyolin pip install yaml`"
            )
        try:
            subprocess.run(["pip", *sys.argv[1:]], check=True)
        except BrokenPipeError:
            clean_close_stdout_and_stderr()
            sys.exit(141)

    def gen_result(
        self, result: Any, config: PrinterConfig
    ) -> Generator[str, None, None]:
        raise NotImplementedError()


def register(
    ctx: PluginContext,
    input_stream: Callable[[], ContextManager[typing.BinaryIO]],
    config: PyolinConfig,
):
    def _pip_var():
        config.printer = PipCustomPrinter()
        return PIP

    ctx.register_globals(
        pip=Item(_pip_var),
    )
