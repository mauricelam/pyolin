# pylint: disable=missing-function-docstring
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=too-many-lines

import contextlib
import os
import traceback
from typing import Any, Callable, Sequence, Union
from pprint import pformat
import subprocess
import sys
import textwrap
import unittest
from unittest import mock

from pyolin.parser import UserError
from pyolin import pyolin
from pyolin.util import _UNDEFINED_

from .utils import ErrorWithStderr, run_capturing_output, timeout


def _test_file(file):
    return os.path.join(os.path.dirname(__file__), file)


def run_cli(prog, *, input_file="data_nba.txt", extra_args=(), **kwargs):
    with run_capturing_output(errmsg=f"Prog: {prog}") as output:
        # pylint:disable=protected-access
        pyolin._command_line(prog, *extra_args, input_=_test_file(input_file), **kwargs)
        # pylint:enable=protected-access
        return output


def run_pyolin(prog, *, input_file="data_nba.txt", **kwargs):
    return pyolin.run(prog, input_=_test_file(input_file), **kwargs)


@contextlib.contextmanager
def pyolin_popen(prog, extra_args=(), universal_newlines=True, **kwargs):
    with subprocess.Popen(
        [sys.executable, "-m", "pyolin", prog] + extra_args,
        stdin=kwargs.get("stdin", subprocess.PIPE),
        stdout=kwargs.get("stdout", subprocess.PIPE),
        stderr=kwargs.get("stderr", subprocess.PIPE),
        universal_newlines=universal_newlines,
        **kwargs,
    ) as proc:
        yield proc


class PyolinTest(unittest.TestCase):
    maxDiff = None
    longMessage = True

    def _myassert(self, actual, expected, prog, data):
        if isinstance(expected, str):
            expected = textwrap.dedent(expected)
            self.assertEqual(
                actual,
                expected,
                msg=f"""\
Prog: pyolin \'{prog}\' {_test_file(data)}
Expected:
{textwrap.indent(expected, '    ')}
---

Actual:
{textwrap.indent(actual, '    ')}
---
""",
            )
        else:
            self.assertEqual(
                actual,
                expected,
                msg=f"""\
Prog: pyolin \'{prog}\' {_test_file(data)}
Expected:
    {expected!r}
---

Actual:
    {actual!r}
---
""",
            )

    def assert_run_pyolin(
        self,
        prog: Union[str, Callable[[], Any]],
        expected: Any,
        *,
        input_file: str = "data_nba.txt",
        **kwargs,
    ):
        actual = run_pyolin(prog, input_file=input_file, **kwargs)
        self.assertEqual(actual, expected)

    def assert_pyolin(
        self,
        prog: Union[str, Callable[[], Any]],
        expected: Union[str, bytes],
        *,
        input_file: str = "data_nba.txt",
        extra_args: Sequence[str] = (),
        **kwargs,
    ):
        actual = run_cli(prog, input_file=input_file, extra_args=extra_args, **kwargs)
        if isinstance(expected, str):
            self._myassert(actual.getvalue(), expected, prog, input_file)
        else:
            self._myassert(actual.getbytes(), expected, prog, input_file)

    def test_lines(self):
        self.assert_pyolin(
            "line for line in lines",
            """\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            """,
        )

    def test_line(self):
        self.assert_pyolin(
            "line",
            """\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            """,
        )

    def test_fields(self):
        self.assert_pyolin(
            "fields",
            """\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            """,
        )

    def test_awk_output_format(self):
        self.assert_pyolin(
            "fields",
            """\
            Bucks Milwaukee 60 22 0.732
            Raptors Toronto 58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston 49 33 0.598
            Pacers Indiana 48 34 0.585
            """,
            output_format="awk",
        )

    def test_awk_output_format_field_separator(self):
        self.assert_pyolin(
            'cfg.printer.field_separator = ","; fields',
            """\
            Bucks,Milwaukee,60,22,0.732
            Raptors,Toronto,58,24,0.707
            76ers,Philadelphia,51,31,0.622
            Celtics,Boston,49,33,0.598
            Pacers,Indiana,48,34,0.585
            """,
            output_format="awk",
        )

    def test_awk_output_format_record_separator(self):
        self.assert_pyolin(
            'cfg.printer.record_separator = ";\\n"; fields',
            """\
            Bucks Milwaukee 60 22 0.732;
            Raptors Toronto 58 24 0.707;
            76ers Philadelphia 51 31 0.622;
            Celtics Boston 49 33 0.598;
            Pacers Indiana 48 34 0.585;
            """,
            output_format="awk",
        )

    def test_reorder_fields(self):
        self.assert_pyolin(
            "fields[1], fields[0]",
            """\
            | 0            | 1       |
            | ------------ | ------- |
            | Milwaukee    | Bucks   |
            | Toronto      | Raptors |
            | Philadelphia | 76ers   |
            | Boston       | Celtics |
            | Indiana      | Pacers  |
            """,
        )

    def test_conditional(self):
        self.assert_pyolin(
            'record for record in records if record[1] == "Boston"',
            """\
            | 0       | 1      | 2  | 3  | 4     |
            | ------- | ------ | -- | -- | ----- |
            | Celtics | Boston | 49 | 33 | 0.598 |
            """,
        )

    def test_number_conversion(self):
        self.assert_pyolin(
            "record.str for record in records if record[2] > 50",
            """\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            """,
        )

    def test_expression_record(self):
        self.assert_pyolin(
            "len(records)",
            """\
            5
            """,
        )

    def test_if_expression(self):
        self.assert_pyolin(
            "fields[0] if fields[3] > 30",
            """\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            """,
        )

    def test_ternary_explicit(self):
        self.assert_pyolin(
            'r[1] if len(r[1]) > 8 else "Name too short" for r in records',
            """\
            | value          |
            | -------------- |
            | Milwaukee      |
            | Name too short |
            | Philadelphia   |
            | Name too short |
            | Name too short |
            """,
        )

    def test_ternary_implicit(self):
        self.assert_pyolin(
            'fields[1] if fields[2] > 50 else "Score too low"',
            """\
            | value         |
            | ------------- |
            | Milwaukee     |
            | Toronto       |
            | Philadelphia  |
            | Score too low |
            | Score too low |
            """,
        )

    def test_count_condition(self):
        self.assert_pyolin(
            "len([r for r in records if r[2] > 50])",
            """\
            3
            """,
        )

    def test_enumerate(self):
        self.assert_pyolin(
            "(i, line) for i, line in enumerate(lines)",
            """\
            | 0 | 1                              |
            | - | ------------------------------ |
            | 0 | Bucks Milwaukee    60 22 0.732 |
            | 1 | Raptors Toronto    58 24 0.707 |
            | 2 | 76ers Philadelphia 51 31 0.622 |
            | 3 | Celtics Boston     49 33 0.598 |
            | 4 | Pacers Indiana     48 34 0.585 |
            """,
        )

    def test_skip_none(self):
        self.assert_pyolin(
            "[None, 1, 2, 3]",
            """\
            | value |
            | ----- |
            | None  |
            | 1     |
            | 2     |
            | 3     |
            """,
        )

    def test_singleton_none(self):
        """
        Just a singleton None, not in a sequence, should be printed (maybe?)
        """
        self.assert_pyolin(
            "None",
            """\
            None
            """,
        )

    def test_regex(self):
        self.assert_pyolin(
            r'fields if re.match(r"^\d.*", fields[0])',
            """\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            """,
        )

    def test_addition(self):
        self.assert_pyolin(
            "fields[2] + 100",
            """\
            | value |
            | ----- |
            | 160   |
            | 158   |
            | 151   |
            | 149   |
            | 148   |
            """,
        )

    def test_radd(self):
        self.assert_pyolin(
            "100 + fields[2]",
            """\
            | value |
            | ----- |
            | 160   |
            | 158   |
            | 151   |
            | 149   |
            | 148   |
            """,
        )

    def test_field_addition(self):
        self.assert_pyolin(
            "fields[2] + fields[3]",
            """\
            | value |
            | ----- |
            | 82    |
            | 82    |
            | 82    |
            | 82    |
            | 82    |
            """,
        )

    def test_field_concat(self):
        self.assert_pyolin(
            "fields[2] + fields[0]",
            """\
            | value     |
            | --------- |
            | 60Bucks   |
            | 58Raptors |
            | 5176ers   |
            | 49Celtics |
            | 48Pacers  |
            """,
        )

    def test_field_concat_reversed(self):
        self.assert_pyolin(
            "fields[0] + fields[2]",
            """\
            | value     |
            | --------- |
            | Bucks60   |
            | Raptors58 |
            | 76ers51   |
            | Celtics49 |
            | Pacers48  |
            """,
        )

    def test_string_concat(self):
        self.assert_pyolin(
            'fields[0] + "++"',
            """\
            | value     |
            | --------- |
            | Bucks++   |
            | Raptors++ |
            | 76ers++   |
            | Celtics++ |
            | Pacers++  |
            """,
        )

    def test_lt(self):
        self.assert_pyolin(
            "fields[0] if fields[2] < 51",
            """\
            | value   |
            | ------- |
            | Celtics |
            | Pacers  |
            """,
        )

    def test_le(self):
        self.assert_pyolin(
            "fields[0] if fields[2] <= 51",
            """\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            """,
        )

    def test_subtraction(self):
        self.assert_pyolin(
            "fields[2] - 50",
            """\
            | value |
            | ----- |
            | 10    |
            | 8     |
            | 1     |
            | -1    |
            | -2    |
            """,
        )

    def test_rsub(self):
        self.assert_pyolin(
            "50 - fields[2]",
            """\
            | value |
            | ----- |
            | -10   |
            | -8    |
            | -1    |
            | 1     |
            | 2     |
            """,
        )

    def test_left_shift(self):
        self.assert_pyolin(
            "fields[2] << 2",
            """\
            | value |
            | ----- |
            | 240   |
            | 232   |
            | 204   |
            | 196   |
            | 192   |
            """,
        )

    def test_neg(self):
        self.assert_pyolin(
            "(-fields[2])",
            """\
            | value |
            | ----- |
            | -60   |
            | -58   |
            | -51   |
            | -49   |
            | -48   |
            """,
        )

    def test_round(self):
        self.assert_pyolin(
            "round(fields[2], -2)",
            """\
            | value |
            | ----- |
            | 100   |
            | 100   |
            | 100   |
            | 0     |
            | 0     |
            """,
        )

    def test_skip_first_line(self):
        self.assert_pyolin(
            "l for l in lines[1:]",
            """\
            | value                          |
            | ------------------------------ |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            """,
        )

    def test_and(self):
        self.assert_pyolin(
            "record if fields[2] > 50 and fields[3] > 30",
            """\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            """,
        )

    def test_add_header(self):
        self.assert_pyolin(
            'cfg.header = ("Team", "City", "Win", "Loss", "Winrate"); records',
            """\
            | Team    | City         | Win | Loss | Winrate |
            | ------- | ------------ | --- | ---- | ------- |
            | Bucks   | Milwaukee    | 60  | 22   | 0.732   |
            | Raptors | Toronto      | 58  | 24   | 0.707   |
            | 76ers   | Philadelphia | 51  | 31   | 0.622   |
            | Celtics | Boston       | 49  | 33   | 0.598   |
            | Pacers  | Indiana      | 48  | 34   | 0.585   |
            """,
        )

    def test_count_dots(self):
        self.assert_pyolin(
            'sum(line.count("0") for line in lines)',
            """\
            7
            """,
        )

    def test_max_score(self):
        self.assert_pyolin(
            "max(r[2] for r in records)",
            """\
            60
            """,
        )

    def test_contents(self):
        self.assert_pyolin(
            "len(contents)",
            """\
            154
            """,
        )

    def test_empty_list(self):
        self.assert_pyolin(
            "[]",
            """\
            """,
        )

    def test_streaming_stdin(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; line",
            extra_args=["--input_format=awk", "--output_format=awk"],
        ) as proc:
            assert proc.stdin and proc.stdout
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Raptors Toronto    58 24 0.707\n"
                )
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Celtics Boston     49 33 0.598\n"
                )

    def test_closed_stdout(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; line",
            extra_args=["--input_format=awk", "--output_format=awk"],
        ) as proc:
            assert proc.stdin and proc.stdout and proc.stderr
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Raptors Toronto    58 24 0.707\n"
                )
            # Command line tools like `head` will close the pipe when it is done getting the data
            # it needs. Make sure this doesn't crash
            proc.stdout.close()
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.close()
            # proc.stdin.flush()
            errmsg = proc.stderr.read()
            self.assertEqual(errmsg, "", errmsg)

    def test_streaming_stdin_binary(self):
        with pyolin_popen(
            "file[:2]",
            extra_args=["--output_format=binary"],
            universal_newlines=False,
        ) as proc:
            stdout, _ = proc.communicate(b"\x30\x62\x43\x00")  # type: ignore
            self.assertEqual(stdout, b"\x30\x62")

    def test_streaming_slice(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; records[:2]",
            extra_args=["--input_format=awk", "--output_format=awk"],
        ) as proc:
            assert proc.stdin and proc.stdout
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Raptors Toronto 58 24 0.707\n"
                )
                self.assertEqual(proc.stdout.readline(), "Celtics Boston 49 33 0.598\n")
            proc.stdin.write("Write more stuff...\n")

    def test_streaming_index(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; records[1].str",
            extra_args=["--input_format=awk", "--output_format=awk"],
        ) as proc:
            assert proc.stdin and proc.stdout
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Celtics Boston     49 33 0.598\n"
                )
            proc.stdin.write("Write more stuff...\n")

    def test_streaming_index_with_auto_parser(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; records[1].str",
            extra_args=["--output_format=awk"],
        ) as proc:
            assert proc.stdin and proc.stdout
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.write("Warriors GS        49 33 0.598\n")
            proc.stdin.write("Lakers LA          49 33 0.598\n")
            proc.stdin.write("Bucks Milwaukee    49 33 0.598\n")
            proc.stdin.write("76ers Philly       49 33 0.598\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Celtics Boston     49 33 0.598\n"
                )
            proc.stdin.write("Write more stuff...\n")

    def test_records_index(self):
        self.assert_pyolin(
            "records[1]",
            """\
            Raptors Toronto 58 24 0.707
            """,
        )

    def test_destructuring(self):
        self.assert_pyolin(
            "city for team, city, _, _, _ in records",
            """\
            | value        |
            | ------------ |
            | Milwaukee    |
            | Toronto      |
            | Philadelphia |
            | Boston       |
            | Indiana      |
            """,
        )

    def test_percentage(self):
        self.assert_pyolin(
            "(r[0], round(r[3] / sum(r[3] for r in records), 2)) for r in records",
            """\
            | 0       | 1    |
            | ------- | ---- |
            | Bucks   | 0.15 |
            | Raptors | 0.17 |
            | 76ers   | 0.22 |
            | Celtics | 0.23 |
            | Pacers  | 0.24 |
            """,
        )

    def test_singleton_tuple(self):
        """
        Tuples are treated as fields in a single record, whereas other iterable
        types are treated as multiple records.
        """
        self.assert_pyolin(
            "sum(r[3] for r in records), max(r[3] for r in records)",
            """\
            144 34
            """,
        )

    def test_module_import(self):
        self.assert_pyolin(
            'record[0] if fnmatch.fnmatch(record[0], "*.txt")',
            """\
            | value                  |
            | ---------------------- |
            | dir/file.txt           |
            | dir/file1.txt          |
            | dir/fileb.txt          |
            | dir/subdir/subfile.txt |
            """,
            input_file="data_files.txt",
        )

    def test_record_variables(self):
        self.assert_pyolin(
            "type(record).__name__, type(line).__name__, type(fields).__name__",
            """\
            | 0      | 1   | 2      |
            | ------ | --- | ------ |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            """,
        )

    def test_file_variables(self):
        self.assert_pyolin(
            "type(lines).__name__, type(records).__name__, "
            "type(file).__name__, type(contents).__name__",
            """\
            StreamingSequence RecordSequence str str
            """,
        )

    def test_boolean(self):
        self.assert_pyolin(
            "record if record[1].bool",
            """\
            | 0          | 1    | 2  | 3    |
            | ---------- | ---- | -- | ---- |
            | dir        | True | 30 | 40.0 |
            | dir/subdir | True | 12 | 42.0 |
            """,
            input_file="data_files.txt",
        )

    def test_awk_header_detection(self):
        self.assert_pyolin(
            "record if record[1].bool",
            """\
            | Path       | IsDir | Size | Score |
            | ---------- | ----- | ---- | ----- |
            | dir        | True  | 30   | 40.0  |
            | dir/subdir | True  | 12   | 42.0  |
            """,
            input_file="data_files_with_header.txt",
        )

    def test_awk_output_with_header(self):
        self.assert_pyolin(
            "record if record[1].bool",
            """\
            Path IsDir Size Score
            dir True 30 40.0
            dir/subdir True 12 42.0
            """,
            input_file="data_files_with_header.txt",
            output_format="awk",
        )

    def test_filename(self):
        self.assert_pyolin(
            "filename",
            f"""\
            {os.path.dirname(__file__)}/data_files.txt
            """,
            input_file="data_files.txt",
        )

    def test_bytes(self):
        self.assert_pyolin(
            'b"hello"',
            """\
            hello
            """,
        )

    def test_reversed(self):
        self.assert_pyolin(
            "reversed(lines)",
            """\
            | value                          |
            | ------------------------------ |
            | Pacers Indiana     48 34 0.585 |
            | Celtics Boston     49 33 0.598 |
            | 76ers Philadelphia 51 31 0.622 |
            | Raptors Toronto    58 24 0.707 |
            | Bucks Milwaukee    60 22 0.732 |
            """,
        )

    def test_in_operator(self):
        self.assert_pyolin(
            '"Raptors Toronto    58 24 0.707" in lines',
            """\
            True
            """,
        )

    def test_base64(self):
        self.assert_pyolin(
            "base64.b64encode(fields[0].bytes)",
            """\
            | value        |
            | ------------ |
            | QnVja3M=     |
            | UmFwdG9ycw== |
            | NzZlcnM=     |
            | Q2VsdGljcw== |
            | UGFjZXJz     |
            """,
        )

    def test_url_quote(self):
        self.assert_pyolin(
            "urllib.parse.quote(line)",
            """\
            | value                                          |
            | ---------------------------------------------- |
            | Bucks%20Milwaukee%20%20%20%2060%2022%200.732   |
            | Raptors%20Toronto%20%20%20%2058%2024%200.707   |
            | 76ers%20Philadelphia%2051%2031%200.622         |
            | Celtics%20Boston%20%20%20%20%2049%2033%200.598 |
            | Pacers%20Indiana%20%20%20%20%2048%2034%200.585 |
            """,
        )

    def test_fields_equal(self):
        self.assert_pyolin(
            "fields[2], fields[3], fields[2] == fields[3]",
            """\
            | 0  | 1    | 2     |
            | -- | ---- | ----- |
            | 30 | 40.0 | False |
            | 40 | 32.0 | False |
            | 23 | 56.0 | False |
            | 15 | 85.0 | False |
            | 31 | 31.0 | True  |
            | 44 | 16.0 | False |
            | 12 | 42.0 | False |
            | 11 | 53.0 | False |
            """,
            input_file="data_files.txt",
        )

    def test_fields_comparison(self):
        self.assert_pyolin(
            "fields[2], fields[3], fields[2] >= fields[3]",
            """\
            | 0  | 1    | 2     |
            | -- | ---- | ----- |
            | 30 | 40.0 | False |
            | 40 | 32.0 | True  |
            | 23 | 56.0 | False |
            | 15 | 85.0 | False |
            | 31 | 31.0 | True  |
            | 44 | 16.0 | True  |
            | 12 | 42.0 | False |
            | 11 | 53.0 | False |
            """,
            input_file="data_files.txt",
        )

    def test_multiplication(self):
        self.assert_pyolin(
            "fields[3] * 10",
            """\
            | value |
            | ----- |
            | 220   |
            | 240   |
            | 310   |
            | 330   |
            | 340   |
            """,
        )

    def test_fields_multiplication(self):
        self.assert_pyolin(
            "fields[3] * fields[2]",
            """\
            | value |
            | ----- |
            | 1320  |
            | 1392  |
            | 1581  |
            | 1617  |
            | 1632  |
            """,
        )

    def test_string_multiplication(self):
        self.assert_pyolin(
            "fields[0] * 2",
            """\
            | value          |
            | -------------- |
            | BucksBucks     |
            | RaptorsRaptors |
            | 76ers76ers     |
            | CelticsCeltics |
            | PacersPacers   |
            """,
        )

    def test_pandas_dataframe(self):
        self.assert_pyolin(
            "df",
            """\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            """,
        )

    def test_pandas_dtypes(self):
        self.assert_pyolin(
            "df.dtypes",
            """\
            | value   |
            | ------- |
            | object  |
            | object  |
            | int64   |
            | int64   |
            | float64 |
            """,
        )

    def test_panda_numeric_operations(self):
        self.assert_pyolin(
            "df[2] * 2",
            """\
            | value |
            | ----- |
            | 120   |
            | 116   |
            | 102   |
            | 98    |
            | 96    |
            """,
        )

    def test_numpy_numeric_operations(self):
        self.assert_pyolin(
            "np.power(df[2], 2)",
            """\
            | value |
            | ----- |
            | 3600  |
            | 3364  |
            | 2601  |
            | 2401  |
            | 2304  |
            """,
        )

    def test_field_separator(self):
        self.assert_pyolin(
            "record",
            """\
            | 0         | 1          | 2           | 3    | 4    | 5     | 6    | 7    | 8  |
            | --------- | ---------- | ----------- | ---- | ---- | ----- | ---- | ---- | -- |
            | Alfalfa   | Aloysius   | 123-45-6789 | 40.0 | 90.0 | 100.0 | 83.0 | 49.0 | D- |
            | Alfred    | University | 123-12-1234 | 41.0 | 97.0 | 96.0  | 97.0 | 48.0 | D+ |
            | Gerty     | Gramma     | 567-89-0123 | 41.0 | 80.0 | 60.0  | 40.0 | 44.0 | C  |
            | Android   | Electric   | 087-65-4321 | 42.0 | 23.0 | 36.0  | 45.0 | 47.0 | B- |
            | Franklin  | Benny      | 234-56-2890 | 50.0 | 1.0  | 90.0  | 80.0 | 90.0 | B- |
            | George    | Boy        | 345-67-3901 | 40.0 | 1.0  | 11.0  | -1.0 | 4.0  | B  |
            | Heffalump | Harvey     | 632-79-9439 | 30.0 | 1.0  | 20.0  | 30.0 | 40.0 | C  |
            """,
            input_file="data_grades_simple_csv.csv",
            field_separator=r",",
        )

    def test_field_separator_regex(self):
        self.assert_pyolin(
            "record",
            # pylint:disable=line-too-long
            """\
            | 0         | 1          | 2           | 3  | 4 | 5  | 6 | 7   | 8 | 9  | 10 | 11 | 12 | 13 |
            | --------- | ---------- | ----------- | -- | - | -- | - | --- | - | -- | -- | -- | -- | -- |
            | Alfalfa   | Aloysius   | 123-45-6789 | 40 | 0 | 90 | 0 | 100 | 0 | 83 | 0  | 49 | 0  | D- |
            | Alfred    | University | 123-12-1234 | 41 | 0 | 97 | 0 | 96  | 0 | 97 | 0  | 48 | 0  | D+ |
            | Gerty     | Gramma     | 567-89-0123 | 41 | 0 | 80 | 0 | 60  | 0 | 40 | 0  | 44 | 0  | C  |
            | Android   | Electric   | 087-65-4321 | 42 | 0 | 23 | 0 | 36  | 0 | 45 | 0  | 47 | 0  | B- |
            | Franklin  | Benny      | 234-56-2890 | 50 | 0 | 1  | 0 | 90  | 0 | 80 | 0  | 90 | 0  | B- |
            | George    | Boy        | 345-67-3901 | 40 | 0 | 1  | 0 | 11  | 0 | -1 | 0  | 4  | 0  | B  |
            | Heffalump | Harvey     | 632-79-9439 | 30 | 0 | 1  | 0 | 20  | 0 | 30 | 0  | 40 | 0  | C  |
            """,
            # pylint:enable=line-too-long
            input_file="data_grades_simple_csv.csv",
            field_separator=r"[\.,]",
            input_format="awk",
        )

    def test_record_separator(self):
        self.assert_pyolin(
            "record",
            """\
            | value     |
            | --------- |
            | JET       |
            | 20031201  |
            | 20001006  |
            | 53521     |
            | 1.000E+01 |
            | NBIC      |
            | HSELM     |
            | TRANS     |
            | 2.000E+00 |
            | 1.000E+00 |
            | 2         |
            | 1         |
            | 0         |
            | 0         |
            """,
            input_file="data_onerow.csv",
            record_separator=r",",
        )

    def test_record_separator_multiple_chars(self):
        self.assert_pyolin(
            "cfg.parser.has_header=False; record",
            """\
            | value    |
            | -------- |
            | JET      |
            | 0031201  |
            | 0001006  | 53521 | 1.000E+01 | NBIC | HSELM | TRANS |
            | .000E+00 | 1.000E+00 |
            |          | 1 | 0 | 0 |
            """,
            input_file="data_onerow.csv",
            record_separator=r",2",
        )

    def test_record_separator_regex(self):
        self.assert_pyolin(
            "record",
            """\
            | value    |
            | -------- |
            | JET      |
            | 20031201 |
            | 20001006 |
            | 53521    |
            | 1        |
            | 000E+01  |
            | NBIC     |
            | HSELM    |
            | TRANS    |
            | 2        |
            | 000E+00  |
            | 1        |
            | 000E+00  |
            | 2        |
            | 1        |
            | 0        |
            | 0        |
            """,
            input_file="data_onerow.csv",
            record_separator=r"[,.]",
        )

    def test_simple_csv(self):
        self.assert_pyolin(
            "df[[0, 1, 2]]",
            """\
            | 0         | 1          | 2           |
            | --------- | ---------- | ----------- |
            | Alfalfa   | Aloysius   | 123-45-6789 |
            | Alfred    | University | 123-12-1234 |
            | Gerty     | Gramma     | 567-89-0123 |
            | Android   | Electric   | 087-65-4321 |
            | Franklin  | Benny      | 234-56-2890 |
            | George    | Boy        | 345-67-3901 |
            | Heffalump | Harvey     | 632-79-9439 |
            """,
            input_file="data_grades_simple_csv.csv",
            input_format="csv",
        )

    def test_quoted_csv(self):
        self.assert_pyolin(
            "record[0]",
            """\
            | value            |
            | ---------------- |
            | 60 Minutes       |
            | 48 Hours Mystery |
            | 20/20            |
            | Nightline        |
            | Dateline Friday  |
            | Dateline Sunday  |
            """,
            input_file="data_news_decline.csv",
            input_format="csv",
        )

    def test_quoted_csv_str(self):
        self.assert_pyolin(
            "record.str",
            """\
            | value                             |
            | --------------------------------- |
            | "60 Minutes",       7.6, 7.4, 7.3 |
            | "48 Hours Mystery", 4.1, 3.9, 3.6 |
            | "20/20",            4.1, 3.7, 3.3 |
            | "Nightline",        2.7, 2.6, 2.7 |
            | "Dateline Friday",  4.1, 4.1, 3.9 |
            | "Dateline Sunday",  3.5, 3.2, 3.1 |
            """,
            input_file="data_news_decline.csv",
            input_format="csv",
        )

    def test_auto_csv(self):
        self.assert_pyolin(
            "df[[0,1,2]]",
            """\
            | 0                     | 1        | 2                                |
            | --------------------- | -------- | -------------------------------- |
            | John                  | Doe      | 120 jefferson st.                |
            | Jack                  | McGinnis | 220 hobo Av.                     |
            | John "Da Man"         | Repici   | 120 Jefferson St.                |
            | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
            |                       | Blankman |                                  |
            | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
            """,
            input_file="data_addresses.csv",
            input_format="csv",
        )

    def test_csv_excel(self):
        self.assert_pyolin(
            "df[[0,1,2]]",
            """\
            | 0                     | 1        | 2                                |
            | --------------------- | -------- | -------------------------------- |
            | John                  | Doe      | 120 jefferson st.                |
            | Jack                  | McGinnis | 220 hobo Av.                     |
            | John "Da Man"         | Repici   | 120 Jefferson St.                |
            | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
            |                       | Blankman |                                  |
            | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
            """,
            input_file="data_addresses.csv",
            input_format="csv_excel",
        )

    def test_csv_unix(self):
        self.assert_pyolin(
            '"|".join((record[0], record[1], record[2]))',
            """\
            | value                                          |
            | ---------------------------------------------- |
            | John|Doe|120 jefferson st.                     |
            | Jack|McGinnis|220 hobo Av.                     |
            | John "Da Man"|Repici|120 Jefferson St.         |
            | Stephen|Tyler|7452 Terrace "At the Plaza" road |
            | |Blankman|                                     |
            | Joan "the bone", Anne|Jet|9th, at Terrace plc  |
            """,
            input_file="data_addresses_unix.csv",
            input_format="csv",
        )

    def test_quoted_tsv(self):
        self.assert_pyolin(
            "record[0], record[2]",
            """\
            | 0                | 1   |
            | ---------------- | --- |
            | 60 Minutes       | 7.4 |
            | 48 Hours Mystery | 3.9 |
            | 20/20            | 3.7 |
            | Nightline        | 2.6 |
            | Dateline Friday  | 4.1 |
            | Dateline Sunday  | 3.2 |
            """,
            input_file="data_news_decline.tsv",
            input_format="csv",
            field_separator="\t",
        )

    def test_statement(self):
        self.assert_pyolin(
            "a = record[2]; b = 1; a + b",
            """\
            | value |
            | ----- |
            | 61    |
            | 59    |
            | 52    |
            | 50    |
            | 49    |
            """,
        )

    def test_statement_table(self):
        self.assert_pyolin(
            "a = len(records); b = 2; a * b",
            """\
            10
            """,
        )

    def test_syntax_error(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("a..x")
        self.assertEqual(
            textwrap.dedent(
                """\
                Invalid syntax:
                  a..x
                    ^"""
            ),
            str(context.exception.__cause__),
        )

    def test_syntax_error_in_statement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("a..x; a+1")
        self.assertEqual(
            textwrap.dedent(
                """\
                Invalid syntax:
                  a..x
                    ^"""
            ),
            str(context.exception.__cause__),
        )

    def test_header_detection(self):
        self.assert_pyolin(
            'df[["Last name", "SSN", "Final"]]',
            """\
            | Last name | SSN         | Final |
            | --------- | ----------- | ----- |
            | Alfalfa   | 123-45-6789 | 49    |
            | Alfred    | 123-12-1234 | 48    |
            | Gerty     | 567-89-0123 | 44    |
            | Android   | 087-65-4321 | 47    |
            | Franklin  | 234-56-2890 | 90    |
            | George    | 345-67-3901 | 4     |
            | Heffalump | 632-79-9439 | 40    |
            """,
            input_file="data_grades_with_header.csv",
            input_format="csv",
        )

    def test_force_has_header(self):
        self.assert_pyolin(
            "cfg.parser.has_header = True; (r[0], r[2], r[7]) for r in records",
            """\
            | Alfalfa   | 123-45-6789 | 49.0 |
            | --------- | ----------- | ---- |
            | Alfred    | 123-12-1234 | 48.0 |
            | Gerty     | 567-89-0123 | 44.0 |
            | Android   | 087-65-4321 | 47.0 |
            | Franklin  | 234-56-2890 | 90.0 |
            | George    | 345-67-3901 | 4.0  |
            | Heffalump | 632-79-9439 | 40.0 |
            """,
            input_file="data_grades_simple_csv.csv",
            input_format="csv",
        )

    def test_header_detection_csv_excel(self):
        self.assert_pyolin(
            'df[["Last Name", "Address"]]',
            """\
            | Last Name             | Address                          |
            | --------------------- | -------------------------------- |
            | John                  | 120 jefferson st.                |
            | Jack                  | 220 hobo Av.                     |
            | John "Da Man"         | 120 Jefferson St.                |
            | Stephen               | 7452 Terrace "At the Plaza" road |
            | Joan "the bone", Anne | 9th, at Terrace plc              |
            """,
            input_file="data_addresses_with_header.csv",
            input_format="csv_excel",
        )

    def test_print_dataframe_header(self):
        self.assert_pyolin(
            "list(df.columns.values)",
            """\
            | value      |
            | ---------- |
            | Last name  |
            | First name |
            | SSN        |
            | Test1      |
            | Test2      |
            | Test3      |
            | Test4      |
            | Final      |
            | Grade      |
            """,
            input_file="data_grades_with_header.csv",
            input_format="csv",
        )

    def test_assign_to_record(self):
        """
        Try to confuse the parser by writing to a field called record
        """
        self.assert_pyolin(
            "record=1; record+1",
            """\
            2
            """,
        )

    def test_access_record_and_table(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("a = record[0]; b = records; b")
        self.assertEqual(
            "Cannot access both record scoped and table scoped variables",
            str(context.exception.__cause__.__cause__),  # type: ignore
        )

    def test_access_table_and_record(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("a = records; b = record[0]; b")
        self.assertEqual(
            "Cannot access both record scoped and table scoped variables",
            str(context.exception.__cause__.__cause__),  # type: ignore
        )

    def test_empty_record_scoped(self):
        self.assert_pyolin("record[0]", "", input_file=os.devnull)

    def test_empty_table_scoped(self):
        self.assert_pyolin("record for record in records", "", input_file=os.devnull)

    def test_semicolon_in_string(self):
        self.assert_pyolin('"hello; world"', "hello; world\n")

    def test_stack_trace_cleaning(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("urllib.parse.quote(12345)")
        assert isinstance(context.exception.__cause__, UserError)
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(5, len(formatted), pformat(formatted))
        self.assertIn("Traceback (most recent call last)", formatted[0])
        self.assertIn("pyolin_user_prog.py", formatted[1])
        self.assertIn("return quote_from_bytes", formatted[2])
        self.assertIn('"quote_from_bytes() expected bytes"', formatted[3])
        self.assertIn("quote_from_bytes() expected bytes", formatted[4])

    def test_invalid_output_format(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("1+1", output_format="invalid")
        self.assertEqual(
            'Unrecognized output format "invalid"', str(context.exception.__cause__)
        )

    def test_csv_output_format(self):
        self.assert_pyolin(
            "records",
            """\
            Bucks,Milwaukee,60,22,0.732\r
            Raptors,Toronto,58,24,0.707\r
            76ers,Philadelphia,51,31,0.622\r
            Celtics,Boston,49,33,0.598\r
            Pacers,Indiana,48,34,0.585\r
            """,
            output_format="csv",
        )

    def test_csv_output_format_unix(self):
        self.assert_pyolin(
            "cfg.printer.dialect = csv.unix_dialect; records",
            """\
            "Bucks","Milwaukee","60","22","0.732"
            "Raptors","Toronto","58","24","0.707"
            "76ers","Philadelphia","51","31","0.622"
            "Celtics","Boston","49","33","0.598"
            "Pacers","Indiana","48","34","0.585"
            """,
            output_format="csv",
        )

    def test_csv_output_invalid_dialect(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('cfg.printer.dialect = "invalid"; records', output_format="csv")
        self.assertEqual('Unknown dialect "invalid"', str(context.exception.__cause__))

    def test_csv_output_format_delimiter(self):
        self.assert_pyolin(
            'cfg.printer.delimiter = "^"; records',
            """\
            Bucks^Milwaukee^60^22^0.732\r
            Raptors^Toronto^58^24^0.707\r
            76ers^Philadelphia^51^31^0.622\r
            Celtics^Boston^49^33^0.598\r
            Pacers^Indiana^48^34^0.585\r
            """,
            output_format="csv",
        )

    def test_csv_output_non_tuple(self):
        self.assert_pyolin(
            "record[2]",
            """\
            60\r
            58\r
            51\r
            49\r
            48\r
            """,
            output_format="csv",
        )

    def test_csv_output_quoting(self):
        self.assert_pyolin(
            "records",
            '''\
            John,Doe,120 jefferson st.,Riverside, NJ, 08075\r
            Jack,McGinnis,220 hobo Av.,Phila, PA,09119\r
            "John ""Da Man""",Repici,120 Jefferson St.,Riverside, NJ,08075\r
            Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234\r
            ,Blankman,,SomeTown, SD, 00298\r
            "Joan ""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123\r
            ''',
            input_format="csv",
            output_format="csv",
            input_file="data_addresses.csv",
        )

    def test_csv_output_with_header(self):
        self.assert_pyolin(
            'cfg.printer.print_header = True; df[["Last name", "SSN", "Final"]]',
            """\
            Last name,SSN,Final\r
            Alfalfa,123-45-6789,49\r
            Alfred,123-12-1234,48\r
            Gerty,567-89-0123,44\r
            Android,087-65-4321,47\r
            Franklin,234-56-2890,90\r
            George,345-67-3901,4\r
            Heffalump,632-79-9439,40\r
            """,
            input_file="data_grades_with_header.csv",
            input_format="csv",
            output_format="csv",
        )

    def test_csv_output_with_header_function(self):
        def func():
            # pylint: disable=undefined-variable
            cfg.printer.print_header = True  # type: ignore
            return df[["Last name", "SSN", "Final"]]  # type: ignore
            # pylint: enable=undefined-variable

        self.assert_pyolin(
            func,
            """\
            Last name,SSN,Final\r
            Alfalfa,123-45-6789,49\r
            Alfred,123-12-1234,48\r
            Gerty,567-89-0123,44\r
            Android,087-65-4321,47\r
            Franklin,234-56-2890,90\r
            George,345-67-3901,4\r
            Heffalump,632-79-9439,40\r
            """,
            input_file="data_grades_with_header.csv",
            input_format="csv",
            output_format="csv",
        )

    def test_streaming_stdin_csv(self):
        with pyolin_popen(
            "cfg.parser.has_header = False; record",
            ["--output_format", "csv", "--input_format", "awk"],
        ) as proc:
            assert proc.stdin and proc.stdout
            proc.stdin.write("Raptors Toronto    58 24 0.707\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(), "Raptors,Toronto,58,24,0.707\n"
                )
            proc.stdin.write("Celtics Boston     49 33 0.598\n")
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(proc.stdout.readline(), "Celtics,Boston,49,33,0.598\n")

    def test_numeric_header(self):
        self.assert_pyolin(
            "cfg.printer.print_header = True; record[0],record[2],record[7]",
            """\
            0,1,2\r
            Alfalfa,123-45-6789,49.0\r
            Alfred,123-12-1234,48.0\r
            Gerty,567-89-0123,44.0\r
            Android,087-65-4321,47.0\r
            Franklin,234-56-2890,90.0\r
            George,345-67-3901,4.0\r
            Heffalump,632-79-9439,40.0\r
            """,
            input_file="data_grades_simple_csv.csv",
            input_format="csv",
            output_format="csv",
        )

    def test_markdown_output(self):
        self.assert_pyolin(
            'df[["Last name", "SSN", "Final"]]',
            """\
            | Last name | SSN         | Final |
            | --------- | ----------- | ----- |
            | Alfalfa   | 123-45-6789 | 49    |
            | Alfred    | 123-12-1234 | 48    |
            | Gerty     | 567-89-0123 | 44    |
            | Android   | 087-65-4321 | 47    |
            | Franklin  | 234-56-2890 | 90    |
            | George    | 345-67-3901 | 4     |
            | Heffalump | 632-79-9439 | 40    |
            """,
            input_file="data_grades_with_header.csv",
            input_format="csv",
            output_format="markdown",
        )

    def test_tsv_output(self):
        self.assert_pyolin(
            "records",
            """\
            Bucks	Milwaukee	60	22	0.732\r
            Raptors	Toronto	58	24	0.707\r
            76ers	Philadelphia	51	31	0.622\r
            Celtics	Boston	49	33	0.598\r
            Pacers	Indiana	48	34	0.585\r
            """,
            output_format="tsv",
        )

    def test_multiline_input(self):
        self.assert_pyolin(
            textwrap.dedent(
                """\
            record = 1
            record + 1\
            """
            ),
            """\
            2
            """,
        )

    def test_multiline_mixed_input(self):
        self.assert_pyolin(
            textwrap.dedent(
                """\
                record = 1; record += 1
                record += 1; record + 1
                """
            ),
            """\
            4
            """,
        )

    def test_last_statement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("1+1;pass")
        self.assertEqual(
            textwrap.dedent(
                """\
                Cannot evaluate value from statement:
                  pass"""
            ),
            str(context.exception.__cause__),
        )

    def test_multiline_python_program(self):
        self.assert_pyolin(
            textwrap.dedent(
                """\
                result = []
                for i in range(5):
                    result.append(range(i + 1))
                result
                """
            ),
            """\
            0
            0 1
            0 1 2
            0 1 2 3
            0 1 2 3 4
            """,
            output_format="awk",
        )

    def test_markdown_wrapping(self):
        with mock.patch.dict(os.environ, {"PYOLIN_TABLE_WIDTH": "80"}):
            self.assert_pyolin(
                'df[["marketplace", "review_body", "star_rating"]]',
                """\
                | marketplace | review_body                                      | star_rating |
                | ----------- | ------------------------------------------------ | ----------- |
                | US          | Absolutely love this watch! Get compliments      | 5           |
                :             : almost every time I wear it. Dainty.             :             :
                | US          | I love this watch it keeps time wonderfully.     | 5           |
                | US          | Scratches                                        | 2           |
                | US          | It works well on me. However, I found cheaper    | 5           |
                :             : prices in other places after making the purchase :             :
                | US          | Beautiful watch face.  The band looks nice all   | 4           |
                :             : around.  The links do make that squeaky cheapo   :             :
                :             : noise when you swing it back and forth on your   :             :
                :             : wrist which can be embarrassing in front of      :             :
                :             : watch enthusiasts.  However, to the naked eye    :             :
                :             : from afar, you can't tell the links are cheap or :             :
                :             :  folded because it is well polished and brushed  :             :
                :             : and the folds are pretty tight for the most      :             :
                :             : part.<br /><br />I love the new member of my     :             :
                :             : collection and it looks great.  I've had it for  :             :
                :             : about a week and so far it has kept good time    :             :
                :             : despite day 1 which is typical of a new          :             :
                :             : mechanical watch                                 :             :
                """,
                input_file="data_amazon_reviews.tsv",
                input_format="tsv",
                output_format="markdown",
            )

    def test_markdown_wrapping2(self):
        with mock.patch.dict(os.environ, {"PYOLIN_TABLE_WIDTH": "80"}):
            self.assert_pyolin(
                "records",
                """\
                | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11    | 12           | 13  | 14 |
                | - | - | - | - | - | - | - | - | - | - | -- | ----- | ------------ | --- | -- |
                | a | b | c | d | e | f | g | h | i | j | k  | lmnop | qrstuv123456 | wxy | z  |
                :   :   :   :   :   :   :   :   :   :   :    :       : 789123456789 :     :    :
                :   :   :   :   :   :   :   :   :   :   :    :       : 123456789    :     :    :
                """,
                input_file="data_formatting.txt",
                output_format="markdown",
            )

    def test_json_output(self):
        self.assert_pyolin(
            "records[0]",
            """\
            [
                "Bucks",
                "Milwaukee",
                60,
                22,
                0.732
            ]
            """,
            output_format="json",
        )

    def test_json_output_with_manual_header(self):
        self.assert_pyolin(
            "cfg.header = ['team', 'city', 'wins', 'loss', 'Win rate']; records[0]",
            """\
            {
                "team": "Bucks",
                "city": "Milwaukee",
                "wins": 60,
                "loss": 22,
                "Win rate": 0.732
            }
            """,
            output_format="json",
        )

    def test_json_output_with_header(self):
        self.assert_pyolin(
            "records",
            """\
            [
                {"Path": "dir", "IsDir": "True", "Size": 30, "Score": 40.0},
                {"Path": "dir/file.txt", "IsDir": "False", "Size": 40, "Score": 32.0},
                {"Path": "dir/file1.txt", "IsDir": "False", "Size": 23, "Score": 56.0},
                {"Path": "dir/file2.mp4", "IsDir": "False", "Size": 15, "Score": 85.0},
                {"Path": "dir/filea.png", "IsDir": "False", "Size": 31, "Score": 31.0},
                {"Path": "dir/fileb.txt", "IsDir": "False", "Size": 44, "Score": 16.0},
                {"Path": "dir/subdir", "IsDir": "True", "Size": 12, "Score": 42.0},
                {"Path": "dir/subdir/subfile.txt", "IsDir": "False", "Size": 11, "Score": 53.0}
            ]
            """,
            input_file="data_files_with_header.txt",
            output_format="json",
        )

    def test_json_input(self):
        self.assert_pyolin(
            "df",
            """\
            | color   | value |
            | ------- | ----- |
            | red     | #f00  |
            | green   | #0f0  |
            | blue    | #00f  |
            | cyan    | #0ff  |
            | magenta | #f0f  |
            | yellow  | #ff0  |
            | black   | #000  |
            """,
            input_file="data_colors.json",
            input_format="json",
            output_format="markdown",
        )

    def test_contains(self):
        self.assert_pyolin(
            '("green", "#0f0") in records',
            """\
            True
            """,
            input_file="data_colors.json",
            input_format="json",
        )

    def test_markdown_output_format(self):
        for prog, expected in [
            (
                "len(records)",
                """\
                | value |
                | ----- |
                | 7     |
                """,
            ),
            (
                "records[0]",
                """\
                | color | value |
                | ----- | ----- |
                | red   | #f00  |
                """,
            ),
            (
                "records",
                """\
                | color   | value |
                | ------- | ----- |
                | red     | #f00  |
                | green   | #0f0  |
                | blue    | #00f  |
                | cyan    | #0ff  |
                | magenta | #f0f  |
                | yellow  | #ff0  |
                | black   | #000  |
                """,
            ),
            (
                "lines",
                """\
                | value                                 |
                | ------------------------------------- |
                | {"color": "red", "value": "#f00"}     |
                | {"color": "green", "value": "#0f0"}   |
                | {"color": "blue", "value": "#00f"}    |
                | {"color": "cyan", "value": "#0ff"}    |
                | {"color": "magenta", "value": "#f0f"} |
                | {"color": "yellow", "value": "#ff0"}  |
                | {"color": "black", "value": "#000"}   |
                """,
            ),
        ]:
            with self.subTest(prog):
                self.assert_pyolin(
                    prog,
                    expected,
                    input_file="data_colors.json",
                    input_format="json",
                    output_format="markdown",
                )

    def test_markdown_non_uniform_column_count(self):
        self.assert_pyolin(
            "range(i) for i in range(1, 5)",
            """\
            | value |
            | ----- |
            | 0     |
            | 0     | 1 |
            | 0     | 1 | 2 |
            | 0     | 1 | 2 | 3 |
            """,
            output_format="markdown",
        )

    def test_repr_printer(self):
        self.assert_pyolin(
            "range(10)",
            """\
            range(0, 10)
            """,
            output_format="repr",
        )

    def test_repr_printer_table(self):
        self.assert_pyolin(
            "records",
            # pylint:disable=line-too-long
            """\
            [('Bucks', 'Milwaukee', '60', '22', '0.732'), ('Raptors', 'Toronto', '58', '24', '0.707'), ('76ers', 'Philadelphia', '51', '31', '0.622'), ('Celtics', 'Boston', '49', '33', '0.598'), ('Pacers', 'Indiana', '48', '34', '0.585')]
            """,
            # pylint:enable=line-too-long
            output_format="repr",
        )

    def test_repr_printer_records(self):
        self.assert_pyolin(
            '"aloha\u2011\u2011\u2011"',
            """\
            'aloha\u2011\u2011\u2011'
            """,
            output_format="repr",
        )

    def test_str_printer_records(self):
        self.assert_pyolin(
            '"aloha\u2011\u2011\u2011"',
            """\
            aloha\u2011\u2011\u2011
            """,
            output_format="str",
        )

    def test_str_printer_table(self):
        self.assert_pyolin(
            "records",
            (
                "[('Bucks', 'Milwaukee', '60', '22', '0.732'), "
                "('Raptors', 'Toronto', '58', '24', '0.707'), "
                "('76ers', 'Philadelphia', '51', '31', '0.622'), "
                "('Celtics', 'Boston', '49', '33', '0.598'), "
                "('Pacers', 'Indiana', '48', '34', '0.585')]\n"
            ),
            output_format="str",
        )

    def test_set_printer(self):
        self.assert_pyolin(
            'cfg.printer = new_printer("repr"); range(10)',
            """\
            range(0, 10)
            """,
        )

    def test_printer_none(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("cfg.printer = None; 123")
        self.assertEqual(
            'printer must be an instance of Printer. Found "None" instead',
            str(context.exception.__cause__),
        )

    def test_raise_stop_iteration(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("raise StopIteration(); None")
        assert isinstance(context.exception.__cause__, UserError)
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(3, len(formatted), pformat(formatted))
        self.assertIn("Traceback (most recent call last)", formatted[0])
        self.assertIn("pyolin_user_prog.py", formatted[1])
        self.assertIn("StopIteration", formatted[2])

    def test_binary_input_len(self):
        self.assert_pyolin(
            "len(file)",
            """\
            21
            """,
            input_file="data_pickle",
        )

    def test_binary_input_can_be_accessed(self):
        self.assert_pyolin(
            "type(file).__name__",
            """\
            bytes
            """,
            input_file="data_pickle",
        )

    def test_binary_input_pickle(self):
        self.assert_pyolin(
            "pickle.loads(file)",
            """\
            hello world
            """,
            input_file="data_pickle",
        )

    def test_binary_printer(self):
        self.assert_pyolin(
            'b"\\x30\\x62\\x43\\x00"',
            b"\x30\x62\x43\x00",
            output_format="binary",
        )

    def test_auto_parser(self):
        for input_file, expected_output in [
            ("data_nba.txt", ("Bucks", "Milwaukee", "60", "22", "0.732")),
            ("data_files.txt", ("dir", "True", "30", "40.0")),
            ("data_colors.json", ("red", "#f00")),
            (
                "data_addresses_with_header.csv",
                ("John", "Doe", "120 jefferson st.", "Riverside", " NJ", " 08075"),
            ),
            (
                "data_addresses_unix.csv",
                ("John", "Doe", "120 jefferson st.", "Riverside", " NJ", " 08075"),
            ),
            (
                "data_grades_with_header.csv",
                (
                    "Alfalfa",
                    "Aloysius",
                    "123-45-6789",
                    "40.0",
                    "90.0",
                    "100.0",
                    "83.0",
                    "49.0",
                    "D-",
                ),
            ),
            (
                "data_amazon_reviews.tsv",
                (
                    "US",
                    "3653882",
                    "R3O9SGZBVQBV76",
                    "B00FALQ1ZC",
                    "937001370",
                    (
                        'Invicta Women\'s 15150 "Angel" 18k Yellow Gold Ion-Plated Stainless Steel '
                        "and Brown Leather Watch"
                    ),
                    "Watches",
                    "5",
                    "0",
                    "0",
                    "N",
                    "Y",
                    "Five Stars",
                    (
                        "Absolutely love this watch! Get compliments almost every time I wear it. "
                        "Dainty."
                    ),
                    "2015-08-31",
                ),
            ),
        ]:
            with self.subTest(input_file):
                self.assert_pyolin(
                    "records[0]",
                    repr(expected_output) + "\n",
                    input_file=input_file,
                    output_format="repr",
                )

    def test_binary_input_access_records(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("records", input_file="data_pickle")

        def format_exception_only(exc):
            if sys.version_info >= (3, 10):
                return traceback.format_exception_only(
                    exc
                )  # pylint:disable=no-value-for-parameter
            else:
                return traceback.format_exception_only(type(exc), exc)  # type: ignore

        self.assertEqual(
            "`record`-based attributes are not supported for binary inputs",
            str(context.exception.__cause__),
            msg="".join(format_exception_only(context.exception.__cause__)),
        )

    def test_set_field_separator(self):
        self.assert_pyolin(
            'parser.field_separator = ","; record',
            """\
            | 0         | 1          | 2           | 3    | 4    | 5     | 6    | 7    | 8  |
            | --------- | ---------- | ----------- | ---- | ---- | ----- | ---- | ---- | -- |
            | Alfalfa   | Aloysius   | 123-45-6789 | 40.0 | 90.0 | 100.0 | 83.0 | 49.0 | D- |
            | Alfred    | University | 123-12-1234 | 41.0 | 97.0 | 96.0  | 97.0 | 48.0 | D+ |
            | Gerty     | Gramma     | 567-89-0123 | 41.0 | 80.0 | 60.0  | 40.0 | 44.0 | C  |
            | Android   | Electric   | 087-65-4321 | 42.0 | 23.0 | 36.0  | 45.0 | 47.0 | B- |
            | Franklin  | Benny      | 234-56-2890 | 50.0 | 1.0  | 90.0  | 80.0 | 90.0 | B- |
            | George    | Boy        | 345-67-3901 | 40.0 | 1.0  | 11.0  | -1.0 | 4.0  | B  |
            | Heffalump | Harvey     | 632-79-9439 | 30.0 | 1.0  | 20.0  | 30.0 | 40.0 | C  |
            """,
            input_file="data_grades_simple_csv.csv",
        )

    def test_set_record_separator(self):
        self.assert_pyolin(
            'cfg.parser.record_separator = ","; record',
            """\
            | value     |
            | --------- |
            | JET       |
            | 20031201  |
            | 20001006  |
            | 53521     |
            | 1.000E+01 |
            | NBIC      |
            | HSELM     |
            | TRANS     |
            | 2.000E+00 |
            | 1.000E+00 |
            | 2         |
            | 1         |
            | 0         |
            | 0         |
            """,
            input_file="data_onerow.csv",
        )

    def test_set_parser_json(self):
        self.assert_pyolin(
            'cfg.parser = new_parser("json"); df',
            """\
            | color   | value |
            | ------- | ----- |
            | red     | #f00  |
            | green   | #0f0  |
            | blue    | #00f  |
            | cyan    | #0ff  |
            | magenta | #f0f  |
            | yellow  | #ff0  |
            | black   | #000  |
            """,
            input_file="data_colors.json",
        )

    def test_set_parser_record(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("a = records[0]; cfg.parser = 123; cfg.header = (); 123")
        self.assertEqual(
            "Parsing already started, cannot set parser",
            str(context.exception.__cause__.__cause__),  # type: ignore
        )

    def test_records_if_undefined(self):
        self.assert_pyolin(
            "records if False",
            """\
            """,
        )

    def test_gen_records_if_undefined(self):
        self.assert_run_pyolin("records if False", _UNDEFINED_)

    def test_undefined(self):
        for output_format, expected in {
            "repr": "Undefined()\n",
            "str": "",
            "json": "",
            "awk": "",
            "auto": "",
            "binary": "",
            "csv": "",
        }.items():
            with self.subTest(output_format):
                self.assert_pyolin(
                    "_UNDEFINED_",
                    expected,
                    output_format=output_format,
                )

    def test_name_error(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("idontknowwhatisthis + 1")
        self.assertEqual(
            "name 'idontknowwhatisthis' is not defined",
            str(context.exception.__cause__.__cause__),  # type: ignore
        )

    def test_record_first(self):
        self.assert_pyolin(
            "global mysum\n"
            'if record.first: mysum = 0; cfg.header = ("sum", "value")\n'
            "mysum += record[2]\n"
            "mysum, record[2]",
            """\
            | sum | value |
            | --- | ----- |
            | 60  | 60    |
            | 118 | 58    |
            | 169 | 51    |
            | 218 | 49    |
            | 266 | 48    |
            """,
        )

    def test_record_num(self):
        self.assert_pyolin(
            "record.num",
            """\
            | value |
            | ----- |
            | 0     |
            | 1     |
            | 2     |
            | 3     |
            | 4     |
            """,
        )

    def test_trailing_newline(self):
        self.assert_pyolin(
            "records\n",
            """\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            """,
        )

    def test_execute_function(self):
        def get_records():
            # pylint: disable=undefined-variable
            return records  # type: ignore
            # pylint: enable=undefined-variable

        self.assert_run_pyolin(
            get_records,
            [
                ("Bucks", "Milwaukee", 60, 22, 0.732),
                ("Raptors", "Toronto", 58, 24, 0.707),
                ("76ers", "Philadelphia", 51, 31, 0.622),
                ("Celtics", "Boston", 49, 33, 0.598),
                ("Pacers", "Indiana", 48, 34, 0.585),
            ],
        )

    def test_execute_function_record_scoped(self):
        def get_records():
            # pylint: disable=undefined-variable
            return record[0]  # type: ignore
            # pylint: enable=undefined-variable

        self.assert_run_pyolin(
            get_records, ["Bucks", "Raptors", "76ers", "Celtics", "Pacers"]
        )

    def test_double_semi_colon(self):
        self.assert_pyolin(
            "record = 1; record += 1;; record += 1; record + 1",
            """\
            4
            """,
        )

    def test_if_record_first_double_semi_colon(self):
        """
        Double semi-colon is treated as a newline
        """
        self.assert_pyolin(
            # Alternative, record-scoped way to write
            #   sum = 0; ((sum, record[2]) for record in records)
            "global sum;; if record.first: sum = 0;; sum += record[2]; sum, record[2]",
            """\
            | 0   | 1  |
            | --- | -- |
            | 60  | 60 |
            | 118 | 58 |
            | 169 | 51 |
            | 218 | 49 |
            | 266 | 48 |
            """,
        )

    def test_undefined_is_false(self):
        self.assert_run_pyolin("bool(_UNDEFINED_)", False)

    def test_end_with_double_semi_colon(self):
        self.assert_pyolin(
            "record[2];;",
            """\
            | value |
            | ----- |
            | 60    |
            | 58    |
            | 51    |
            | 49    |
            | 48    |
            """,
        )

    def test_sys_argv(self):
        """
        sys.argv should be shifted, so sys.argv[1] should be the first one after the pyolin prog
        """
        self.assert_pyolin(
            "sys.argv",
            """\
            | value   |
            | ------- |
            | pyolin  |
            | testing |
            | 1       |
            | 2       |
            | 3       |
            """,
            extra_args=["testing", "1", "2", "3"],
        )

    def test_manual_load_json_record(self):
        for output_format, expected in {
            "awk": """\
                   color red
                   value #f00
                   """,
            "json": """\
                    {
                        "color": "red",
                        "value": "#f00"
                    }
                    """,
        }.items():
            with self.subTest(output_format):
                self.assert_pyolin(
                    "json.loads(file)[0]",
                    expected,
                    input_file="data_colors.json",
                    output_format=output_format,
                )

    def test_manual_load_json_output(self):
        for output_format, expected in {
            "awk": """\
                    ('color', 'red') ('value', '#f00')
                    ('color', 'green') ('value', '#0f0')
                    ('color', 'blue') ('value', '#00f')
                    ('color', 'cyan') ('value', '#0ff')
                    ('color', 'magenta') ('value', '#f0f')
                    ('color', 'yellow') ('value', '#ff0')
                    ('color', 'black') ('value', '#000')
                    """,
            "json": """\
                    [
                        {
                            "color": "red",
                            "value": "#f00"
                        },
                        {
                            "color": "green",
                            "value": "#0f0"
                        },
                        {
                            "color": "blue",
                            "value": "#00f"
                        },
                        {
                            "color": "cyan",
                            "value": "#0ff"
                        },
                        {
                            "color": "magenta",
                            "value": "#f0f"
                        },
                        {
                            "color": "yellow",
                            "value": "#ff0"
                        },
                        {
                            "color": "black",
                            "value": "#000"
                        }
                    ]
                    """,
        }.items():
            with self.subTest(output_format):
                self.assert_pyolin(
                    "json.loads(file)",
                    expected,
                    input_file="data_colors.json",
                    output_format=output_format,
                )

    def test_manual_load_csv(self):
        self.assert_pyolin(
            "csv.reader(io.StringIO(file))",
            # pylint:disable=line-too-long
            """\
            | 0                     | 1        | 2                                | 3           | 4   | 5      |
            | --------------------- | -------- | -------------------------------- | ----------- | --- | ------ |
            | John                  | Doe      | 120 jefferson st.                | Riverside   |  NJ |  08075 |
            | Jack                  | McGinnis | 220 hobo Av.                     | Phila       |  PA | 09119  |
            | John "Da Man"         | Repici   | 120 Jefferson St.                | Riverside   |  NJ | 08075  |
            | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road | SomeTown    | SD  |  91234 |
            |                       | Blankman |                                  | SomeTown    |  SD |  00298 |
            | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              | Desert City | CO  | 00123  |
            """,
            # pylint:enable=line-too-long
            input_file="data_addresses.csv",
        )

    def test_non_table_json(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli("records", input_file="data_json_example.json")
            self.assertEqual(
                "TypeError: Input is not an array of objects",
                str(context.exception.__cause__),
            )

    def test_jsonobj_string_output(self):
        self.assert_pyolin(
            "jsonobj['glossary']['title']",
            "example glossary\n",
            input_file="data_json_example.json",
        )

    def test_jsonobj_obj_output(self):
        self.assert_pyolin(
            "jsonobj['glossary']['GlossDiv']['GlossList']['GlossEntry']['GlossDef']",
            """\
            {
                "para": "A meta-markup language, used to create markup languages such as DocBook.",
                "GlossSeeAlso": [
                    "GML",
                    "XML"
                ]
            }
            """,
            input_file="data_json_example.json",
        )

    def test_3d_table(self):
        self.assert_pyolin(
            "[['foo', ['a', 'b']], ['bar', ['c', 'd']]]",
            """\
            [
                [
                    "foo",
                    "['a', 'b']"
                ],
                [
                    "bar",
                    "['c', 'd']"
                ]
            ]
            """,
            input_file="data_json_example.json",
        )

    def test_multiline_json_prog(self):
        self.assert_pyolin(
            textwrap.dedent(
                """\
            [
                ['foo', ['a', 'b']], ['bar', ['c', 'd']]
            ]"""
            ),
            """\
            [
                [
                    "foo",
                    "['a', 'b']"
                ],
                [
                    "bar",
                    "['c', 'd']"
                ]
            ]
            """,
            input_file="data_json_example.json",
        )

    def test_json_with_undefined(self):
        self.assert_pyolin(
            "[_UNDEFINED_, 'foo']",
            """\
            [
                "foo"
            ]
            """,
            input_file="data_json_example.json",
            output_format="json",
        )

    def test_records_negative_index(self):
        self.assert_pyolin(
            "records[-1]",
            """\
            Pacers Indiana 48 34 0.585
            """,
        )

    def test_records_negative_slice_start(self):
        self.assert_pyolin(
            "records[-1:]",
            """\
            | 0      | 1       | 2  | 3  | 4     |
            | ------ | ------- | -- | -- | ----- |
            | Pacers | Indiana | 48 | 34 | 0.585 |
            """,
        )

    def test_records_negative_slice_stop(self):
        self.assert_pyolin(
            "records[:-3]",
            """\
            | 0       | 1         | 2  | 3  | 4     |
            | ------- | --------- | -- | -- | ----- |
            | Bucks   | Milwaukee | 60 | 22 | 0.732 |
            | Raptors | Toronto   | 58 | 24 | 0.707 |
            """,
        )

    def test_records_negative_slice_step(self):
        self.assert_pyolin(
            "records[::-1]",
            """\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            """,
        )

    # TODOs:
    # Bash / Zsh autocomplete integration
