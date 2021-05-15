import contextlib
import io
import signal
from unittest import mock


class ErrorWithStderr(Exception):

    def __init__(self, stderr, *, errmsg=None):
        self.stderr = stderr
        self.errmsg = errmsg

    def __str__(self):
        result = ['', self.errmsg]
        if self.stderr:
            result += [
                '',
                '===== STDERR =====',
                str(self.stderr),
                '=================='
            ]
        if self.__cause__:
            result += [
                '',
                '===== ERROR =====',
                str(self.__cause__),
                '================='
            ]
        return '\n'.join(result)


class timeout:
    def __init__(self, seconds, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


class TextIO(io.TextIOWrapper):
    def __init__(self, **kwargs):
        self._io = io.BytesIO()
        super().__init__(self._io, write_through=True, **kwargs)

    def getvalue(self):
        return self._io.getvalue().decode('utf-8')

    def getbytes(self):
        return self._io.getvalue()


@contextlib.contextmanager
def run_capturing_output(*, errmsg=None):
    out = TextIO()
    err = TextIO()
    with contextlib.redirect_stdout(out):
        with contextlib.redirect_stderr(err):
            try:
                yield out
            except BaseException as e:
                raise ErrorWithStderr(err.getvalue(), errmsg=errmsg) from e
