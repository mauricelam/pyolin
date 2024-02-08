"""
See `PyolinPlugin` and `PluginContext` in the `core` module for details on how
to write a plugin.
"""
from typing import List

from pyolin.core import PyolinPlugin
from . import (
    auto_parser,
    json,
    lines,
    argv,
    pip,
    csv_parser,
    txt_format,
    py_printer,
    markdown_printer,
    auto_printer,
)

PLUGINS: List[PyolinPlugin] = [
    json,
    lines,
    argv,
    pip,
    auto_parser,
    csv_parser,
    txt_format,
    py_printer,
    markdown_printer,
    auto_printer,
]
