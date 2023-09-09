import sys
from typing import Callable, ContextManager
import typing
from pyolin.core import PluginContext, PyolinConfig
from pyolin.field import DeferredType
from pyolin.util import Item


def register(
    ctx: PluginContext,
    input_stream: Callable[[], ContextManager[typing.BinaryIO]],
    config: PyolinConfig,
):
    def _argv_var():
        return [DeferredType(arg) for arg in sys.argv]

    ctx.register_globals(
        argv=Item(_argv_var),
    )
