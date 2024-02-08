import itertools
from itertools import zip_longest
from math import floor
import os
import shutil
import sys
import textwrap
from typing import Any, Generator, Sequence
from pyolin.core import PluginContext
from pyolin.ioformat import Printer, PrinterConfig
from pyolin.util import _UNDEFINED_


class _MarkdownRowFormat:
    def __init__(self, widths):
        self._width_formats = [f"{{:{w}}}" for w in widths]
        self._row_template = (
            "| " + " | ".join(self._width_formats) + " |" if widths else "|"
        )
        self._cont_row_template = (
            ": " + " : ".join(self._width_formats) + " :" if widths else ":"
        )
        self._wrappers = [
            textwrap.TextWrapper(
                width=w,
                expand_tabs=False,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            for w in widths
        ]

    def format(self, cells: Sequence[Any]) -> str:
        """Formats the given list of cells in Markdown."""
        cell_lines = [
            wrapper.wrap(str(cell)) if wrapper else [cell]
            for wrapper, cell in zip_longest(self._wrappers, cells)
        ]
        line_cells = zip_longest(*cell_lines, fillvalue="")
        result = ""
        for i, line_cell in enumerate(line_cells):
            # If there are extra columns that are not found in the header, also print them out.
            # While that's not valid markdown, it's better than silently discarding the values.
            extra_length = len(line_cell) - len(self._width_formats)
            if not i:
                template = self._row_template + "".join(
                    " {} |" for _ in range(extra_length)
                )
                result += template.format(*line_cell) + "\n"
            else:
                template = self._cont_row_template + "".join(
                    " {} :" for _ in range(extra_length)
                )
                result += self._cont_row_template.format(*line_cell) + "\n"
        return result


class MarkdownPrinter(Printer):
    """Prints the result in the markdown table format. Note that if the input
    data does not conform to a table-like structure (e.g. have different number
    of fields in different rows), the output may not be valid markdown."""

    def _allocate_width(self, header: Sequence[str], table):
        if sys.stdout.isatty():
            available_width, _ = shutil.get_terminal_size((100, 24))
        else:
            available_width = int(os.getenv("PYOLIN_TABLE_WIDTH", "100"))
        # Subtract number of characters used by markdown
        available_width -= 2 + 3 * (len(header) - 1) + 2
        remaining_space = available_width
        record_lens = zip(*[[len(c) for c in record] for record in table])
        lens = {
            i: max(len(h), *c_lens)
            for i, (h, c_lens) in enumerate(zip(header, record_lens))
        }
        widths = [0] * len(header)
        while lens:
            to_del = []
            for i, length in lens.items():
                if length < remaining_space / len(lens):
                    widths[i] = length
                    to_del.append(i)
            for i in to_del:
                del lens[i]
            if not to_del:
                divided = floor(remaining_space / len(lens))
                remainder = remaining_space % len(lens)
                for i in lens:
                    widths[i] = divided + 1 if i < remainder else divided
                break

            remaining_space = available_width - sum(widths)
            if remaining_space <= 0:
                break
        return [max(w, 1) for w in widths]

    def gen_result(
        self, result: Any, config: PrinterConfig
    ) -> Generator[str, None, None]:
        if result is _UNDEFINED_:
            return
        header, table_result = self.to_table(result, header=config.header)
        table1, table2, table3 = itertools.tee(table_result, 3)
        widths = self._allocate_width(header, itertools.islice(table2, 10))
        row_format = _MarkdownRowFormat(widths)
        if not header and not any(True for _ in table3):
            return  # Empty result, skip printing
        if header:
            # Edge case: don't print out an empty header
            yield row_format.format(header)
            yield "| " + " | ".join("-" * w for w in widths) + " |\n"
        for record in table1:
            yield row_format.format(record)

def register(ctx: PluginContext, _input_stream, _config):
    ctx.export_printers(
        md=MarkdownPrinter,
        markdown=MarkdownPrinter,
        table=MarkdownPrinter,
    )
