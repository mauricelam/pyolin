"""Utilities for testing."""

import contextlib
from dataclasses import dataclass
import io
import os
import signal
import subprocess
import sys
import textwrap
from typing import Optional, Union

import pytest

from pyolin import pyolin


@dataclass
class File:
    """Represent a test input file, relative to the directory containing this
    source file."""

    filename: str

    def path(self):
        return os.path.join(os.path.dirname(__file__), self.filename)


def _process_input(input_: Union[File, str, bytes]) -> Union[str, io.BytesIO]:
    if isinstance(input_, bytes):
        return io.BytesIO(input_)
    elif isinstance(input_, File):
        return input_.path()
    elif isinstance(input_, str):
        return io.BytesIO(textwrap.dedent(input_.lstrip("\n")).encode("utf-8"))
    else:
        return input_


class TextIO(io.TextIOWrapper):
    """A wrapper over a text IO that allows getting the raw bytes out."""

    def __init__(self, **kwargs):
        self._io = io.BytesIO()
        super().__init__(self._io, write_through=True, **kwargs)

    def getvalue(self):
        """Gets the text value"""
        return self._io.getvalue().decode("utf-8")

    def getbytes(self):
        """Gets the bytes value"""
        return self._io.getvalue()

    def __eq__(self, o):
        if isinstance(o, str):
            return self.getvalue().__eq__(textwrap.dedent(o.lstrip("\n")))
        if isinstance(o, bytes):
            return self.getbytes().__eq__(o)
        return self.getvalue().__eq__(o)

    def __str__(self):
        return self.getvalue()


@pytest.fixture
def input_file_nba():
    return File("data_nba.txt")


@pytest.fixture(name="pyolin")
def pyolin_prog():
    """A pytest fixture to allow getting the "pyolin" function parameter for
    testing."""

    def run_cli(
        prog,
        *,
        input_: Union[str, bytes, File] = File("data_nba.txt"),
        extra_args=(),
        **kwargs,
    ):
        with run_capturing_output(errmsg=f"Prog: {prog}") as output:
            # pylint:disable=protected-access
            pyolin._command_line(
                prog, *extra_args, input_=_process_input(input_), **kwargs
            )
            # pylint:enable=protected-access
            return output

    @contextlib.contextmanager
    def pyolin_popen(prog, *, extra_args=(), text=True, **kwargs):
        with subprocess.Popen(
            [sys.executable, "-m", "pyolin", prog] + extra_args,
            stdin=kwargs.get("stdin", subprocess.PIPE),
            stdout=kwargs.get("stdout", subprocess.PIPE),
            stderr=kwargs.get("stderr", subprocess.PIPE),
            universal_newlines=text,
            **kwargs,
        ) as proc:
            yield proc

    run_cli.popen = pyolin_popen

    return run_cli


class ErrorWithStderr(Exception):
    """Error with stderr captured. See `run_capturing_output`."""

    def __init__(self, stderr, *, errmsg=None):
        self.stderr = stderr
        self.errmsg = errmsg

    def __str__(self):
        result = ["", self.errmsg]
        if self.stderr:
            result += [
                "",
                "===== STDERR =====",
                str(self.stderr),
                "=================="
            ]
        if self.__cause__:  # pylint:disable=using-constant-test
            result += [
                "",
                "===== ERROR =====",
                str(self.__cause__),
                "=================",
            ]
        return "\n".join(result)


class timeout:  # pylint:disable=invalid-name
    """Waits for the given number of seconds or raise TimeoutError.

    Usage:

    with timeout(10):
        # do potentially long-running stuff
    """

    def __init__(self, seconds, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def _handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, _type, _value, _traceback):
        signal.alarm(0)


@contextlib.contextmanager
def run_capturing_output(*, errmsg: Optional[str] = None):
    out = TextIO()
    err = TextIO()
    with contextlib.redirect_stdout(out):
        with contextlib.redirect_stderr(err):
            try:
                yield out
            except BaseException as exc:
                raise ErrorWithStderr(err.getvalue(), errmsg=errmsg) from exc


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, TextIO) or isinstance(right, TextIO) and op == "==":
        if str(left) != str(right):
            return [
                "",
                "=== Actual ===",
                *str(left).split("\n"),
                "===",
                "=== Expected ===",
                *str(right).split("\n"),
                "===",
            ]
