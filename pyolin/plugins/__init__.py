"""
Each plugin should have a function with the following signature:

    def register(
        plugin_reg: PluginRegistration,
        input_stream: Callable[[], ContextManager[typing.BinaryIO]],
        config: PyolinConfig,
    )
"""
from . import auto_parser, json, lines

PLUGINS = [auto_parser, json, lines]
