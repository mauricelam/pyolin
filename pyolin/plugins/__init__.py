"""
Each plugin should have a function with the following signature:

    def register(
        ctx: PluginContext,
        input_stream: Callable[[], ContextManager[typing.BinaryIO]],
        config: PyolinConfig,
    )
"""
from . import auto_parser, json, lines, argv

PLUGINS = [auto_parser, json, lines, argv]
