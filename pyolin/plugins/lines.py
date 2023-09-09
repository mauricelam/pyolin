"""
Plugin that provides the `lines` and `line` variables, which splits the input bytes by the newline
character.
"""

from typing import Callable, ContextManager
import typing
from pyolin.core import PluginContext, PyolinConfig
from pyolin.ioformat import gen_split
from pyolin.util import Item, CachedItem, NoMoreRecords, ReplayIter, StreamingSequence


def register(
    ctx: PluginContext,
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

    def _lines_var():
        config.set_scope(None, "file")
        return StreamingSequence(gen_lines(input_stream))

    ctx.register_globals(
        line=Item(_line_var),
        lines=CachedItem(_lines_var),
    )
