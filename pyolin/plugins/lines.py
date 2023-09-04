"""
Plugin that provides the `lines` and `line` variables, which splits the input bytes by the newline
character.
"""

from typing import Callable, ContextManager
import typing
from pyolin.core import PluginRegistration, PyolinConfig
from pyolin.ioformat import gen_split
from pyolin.util import Item, CachedItem, NoMoreRecords, ReplayIter, StreamingSequence


def register(
    plugin_reg: PluginRegistration,
    input_stream: Callable[[], ContextManager[typing.BinaryIO]],
    config: PyolinConfig,
):
    def gen_lines(input_stream: Callable[[], ContextManager[typing.BinaryIO]]):
        with input_stream() as io_stream:
            for bytearr in gen_split(io_stream, "\n"):
                yield bytearr.decode("utf-8")

    iter_line_seq = ReplayIter(gen_lines(input_stream))

    def _line_var():
        config.set_scope(iter_line_seq, "line")
        try:
            return iter_line_seq.current_or_first_value()
        except StopIteration:
            raise NoMoreRecords
    plugin_reg.register_global("line", Item(_line_var))

    def _lines_var():
        config.set_scope(None, "file")
        return StreamingSequence(gen_lines(input_stream))
    plugin_reg.register_global(
        "lines",
        CachedItem(_lines_var),
    )
