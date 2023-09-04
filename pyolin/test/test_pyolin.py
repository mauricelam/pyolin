# pylint: disable=missing-function-docstring
# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=too-many-lines
# pylint: disable=redefined-outer-name

import os
from pprint import pformat
import textwrap
from unittest import mock

import pytest
from pyolin import pyolin
from pyolin.parser import UserError
from pyolin.util import _UNDEFINED_

from .conftest import ErrorWithStderr, timeout, File


def test_lines(pyolin):
    _in = """\
        Bucks Milwaukee    60 22 0.732
        Raptors Toronto    58 24 0.707
        76ers Philadelphia 51 31 0.622
        Celtics Boston     49 33 0.598
        Pacers Indiana     48 34 0.585
        """
    assert (
        pyolin("line for line in lines", input_=_in)
        == """\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            """
    )


def test_line(pyolin):
    assert (
        pyolin("line")
        == """\
        | value                          |
        | ------------------------------ |
        | Bucks Milwaukee    60 22 0.732 |
        | Raptors Toronto    58 24 0.707 |
        | 76ers Philadelphia 51 31 0.622 |
        | Celtics Boston     49 33 0.598 |
        | Pacers Indiana     48 34 0.585 |
        """
    )


def test_fields(pyolin):
    assert (
        pyolin("fields")
        == """\
        | 0       | 1            | 2  | 3  | 4     |
        | ------- | ------------ | -- | -- | ----- |
        | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
        | Raptors | Toronto      | 58 | 24 | 0.707 |
        | 76ers   | Philadelphia | 51 | 31 | 0.622 |
        | Celtics | Boston       | 49 | 33 | 0.598 |
        | Pacers  | Indiana      | 48 | 34 | 0.585 |
        """
    )


def test_awk_output_format(pyolin):
    assert (
        pyolin("fields", output_format="awk")
        == """\
        Bucks Milwaukee 60 22 0.732
        Raptors Toronto 58 24 0.707
        76ers Philadelphia 51 31 0.622
        Celtics Boston 49 33 0.598
        Pacers Indiana 48 34 0.585
        """
    )


def test_awk_output_format_field_separator(pyolin):
    assert (
        pyolin(
            'cfg.printer.field_separator = ","; fields',
            output_format="awk",
        )
        == """\
        Bucks,Milwaukee,60,22,0.732
        Raptors,Toronto,58,24,0.707
        76ers,Philadelphia,51,31,0.622
        Celtics,Boston,49,33,0.598
        Pacers,Indiana,48,34,0.585
        """
    )


def test_awk_output_format_record_separator(pyolin):
    assert (
        pyolin(
            'cfg.printer.record_separator = ";\\n"; fields',
            output_format="awk",
        )
        == """\
        Bucks Milwaukee 60 22 0.732;
        Raptors Toronto 58 24 0.707;
        76ers Philadelphia 51 31 0.622;
        Celtics Boston 49 33 0.598;
        Pacers Indiana 48 34 0.585;
        """
    )


def test_reorder_fields(pyolin):
    assert (
        pyolin("fields[1], fields[0]")
        == """\
        | 0            | 1       |
        | ------------ | ------- |
        | Milwaukee    | Bucks   |
        | Toronto      | Raptors |
        | Philadelphia | 76ers   |
        | Boston       | Celtics |
        | Indiana      | Pacers  |
        """
    )


def test_conditional(pyolin):
    assert (
        pyolin('record for record in records if record[1] == "Boston"')
        == """\
        | 0       | 1      | 2  | 3  | 4     |
        | ------- | ------ | -- | -- | ----- |
        | Celtics | Boston | 49 | 33 | 0.598 |
        """
    )


def test_number_conversion(pyolin):
    assert (
        pyolin("record.source for record in records if record[2] > 50")
        == """\
        | value                          |
        | ------------------------------ |
        | Bucks Milwaukee    60 22 0.732 |
        | Raptors Toronto    58 24 0.707 |
        | 76ers Philadelphia 51 31 0.622 |
        """
    )


def test_expression_record(pyolin):
    assert (
        pyolin("len(records)")
        == """\
        5
        """
    )


def test_if_expression(pyolin):
    assert (
        pyolin("fields[0] if fields[3] > 30")
        == """\
        | value   |
        | ------- |
        | 76ers   |
        | Celtics |
        | Pacers  |
        """
    )


def test_ternary_explicit(pyolin):
    assert (
        pyolin('r[1] if len(r[1]) > 8 else "Name too short" for r in records')
        == """\
        | value          |
        | -------------- |
        | Milwaukee      |
        | Name too short |
        | Philadelphia   |
        | Name too short |
        | Name too short |
        """
    )


def test_ternary_implicit(pyolin):
    assert (
        pyolin('fields[1] if fields[2] > 50 else "Score too low"')
        == """\
        | value         |
        | ------------- |
        | Milwaukee     |
        | Toronto       |
        | Philadelphia  |
        | Score too low |
        | Score too low |
        """
    )


def test_count_condition(pyolin):
    assert (
        pyolin("len([r for r in records if r[2] > 50])")
        == """\
        3
        """
    )


def test_enumerate(pyolin):
    assert (
        pyolin("(i, line) for i, line in enumerate(lines)")
        == """\
        | 0 | 1                              |
        | - | ------------------------------ |
        | 0 | Bucks Milwaukee    60 22 0.732 |
        | 1 | Raptors Toronto    58 24 0.707 |
        | 2 | 76ers Philadelphia 51 31 0.622 |
        | 3 | Celtics Boston     49 33 0.598 |
        | 4 | Pacers Indiana     48 34 0.585 |
        """
    )


def test_skip_none(pyolin):
    assert (
        pyolin("[None, 1, 2, 3]")
        == """\
        | value |
        | ----- |
        | None  |
        | 1     |
        | 2     |
        | 3     |
        """
    )


def test_singleton_none(pyolin):
    """
    Just a singleton None, not in a sequence, should be printed (maybe?)
    """
    assert (
        pyolin("None")
        == """\
        None
        """
    )


def test_regex(pyolin):
    assert (
        pyolin(r'fields if re.match(r"^\d.*", fields[0])')
        == """\
        | 0     | 1            | 2  | 3  | 4     |
        | ----- | ------------ | -- | -- | ----- |
        | 76ers | Philadelphia | 51 | 31 | 0.622 |
        """
    )


def test_addition(pyolin):
    assert (
        pyolin("fields[2] + 100")
        == """\
        | value |
        | ----- |
        | 160   |
        | 158   |
        | 151   |
        | 149   |
        | 148   |
        """
    )


def test_radd(pyolin):
    assert (
        pyolin("100 + fields[2]")
        == """\
        | value |
        | ----- |
        | 160   |
        | 158   |
        | 151   |
        | 149   |
        | 148   |
        """
    )


def test_field_addition(pyolin):
    assert (
        pyolin("fields[2] + fields[3]")
        == """\
        | value |
        | ----- |
        | 82    |
        | 82    |
        | 82    |
        | 82    |
        | 82    |
        """
    )


def test_field_concat(pyolin):
    assert (
        pyolin("fields[2] + fields[0]")
        == """\
        | value     |
        | --------- |
        | 60Bucks   |
        | 58Raptors |
        | 5176ers   |
        | 49Celtics |
        | 48Pacers  |
        """
    )


def test_field_concat_reversed(pyolin):
    assert (
        pyolin("fields[0] + fields[2]")
        == """\
        | value     |
        | --------- |
        | Bucks60   |
        | Raptors58 |
        | 76ers51   |
        | Celtics49 |
        | Pacers48  |
        """
    )


def test_string_concat(pyolin):
    assert (
        pyolin('fields[0] + "++"')
        == """\
        | value     |
        | --------- |
        | Bucks++   |
        | Raptors++ |
        | 76ers++   |
        | Celtics++ |
        | Pacers++  |
        """
    )


def test_lt(pyolin):
    assert (
        pyolin("fields[0] if fields[2] < 51")
        == """\
        | value   |
        | ------- |
        | Celtics |
        | Pacers  |
        """
    )


def test_le(pyolin):
    assert (
        pyolin("fields[0] if fields[2] <= 51")
        == """\
        | value   |
        | ------- |
        | 76ers   |
        | Celtics |
        | Pacers  |
        """
    )


def test_subtraction(pyolin):
    assert (
        pyolin("fields[2] - 50")
        == """\
        | value |
        | ----- |
        | 10    |
        | 8     |
        | 1     |
        | -1    |
        | -2    |
        """
    )


def test_rsub(pyolin):
    assert (
        pyolin("50 - fields[2]")
        == """\
        | value |
        | ----- |
        | -10   |
        | -8    |
        | -1    |
        | 1     |
        | 2     |
        """
    )


def test_left_shift(pyolin):
    assert (
        pyolin("fields[2] << 2")
        == """\
        | value |
        | ----- |
        | 240   |
        | 232   |
        | 204   |
        | 196   |
        | 192   |
        """
    )


def test_neg(pyolin):
    assert (
        pyolin("(-fields[2])")
        == """\
        | value |
        | ----- |
        | -60   |
        | -58   |
        | -51   |
        | -49   |
        | -48   |
        """
    )


def test_round(pyolin):
    assert (
        pyolin("round(fields[2], -2)")
        == """\
        | value |
        | ----- |
        | 100   |
        | 100   |
        | 100   |
        | 0     |
        | 0     |
        """
    )


def test_skip_first_line(pyolin):
    assert (
        pyolin("l for l in lines[1:]")
        == """\
        | value                          |
        | ------------------------------ |
        | Raptors Toronto    58 24 0.707 |
        | 76ers Philadelphia 51 31 0.622 |
        | Celtics Boston     49 33 0.598 |
        | Pacers Indiana     48 34 0.585 |
        """
    )


def test_and(pyolin):
    assert (
        pyolin("record if fields[2] > 50 and fields[3] > 30")
        == """\
        | 0     | 1            | 2  | 3  | 4     |
        | ----- | ------------ | -- | -- | ----- |
        | 76ers | Philadelphia | 51 | 31 | 0.622 |
        """
    )


def test_add_header(pyolin):
    assert pyolin(
        'cfg.header = ("Team", "City", "Win", "Loss", "Winrate"); records'
    ) == (
        """\
        | Team    | City         | Win | Loss | Winrate |
        | ------- | ------------ | --- | ---- | ------- |
        | Bucks   | Milwaukee    | 60  | 22   | 0.732   |
        | Raptors | Toronto      | 58  | 24   | 0.707   |
        | 76ers   | Philadelphia | 51  | 31   | 0.622   |
        | Celtics | Boston       | 49  | 33   | 0.598   |
        | Pacers  | Indiana      | 48  | 34   | 0.585   |
        """
    )


def test_count_dots(pyolin):
    assert (
        pyolin('sum(line.count("0") for line in lines)')
        == """\
        7
        """
    )


def test_max_score(pyolin):
    assert (
        pyolin("max(r[2] for r in records)")
        == """\
        60
        """
    )


def test_contents(pyolin):
    assert (
        pyolin("len(contents)")
        == """\
        154
        """
    )


def test_empty_list(pyolin):
    assert pyolin("[]") == (
        """\
        """
    )


def test_markdown_empty_header(pyolin):
    assert pyolin("[(), (1,2,3)]", output_format="markdown") == (
        """\
        | 1 | 2 | 3 |
        """
    )


def test_streaming_stdin(pyolin):
    with pyolin.popen(
        "cfg.parser.has_header = False; line",
        extra_args=["--input_format=awk", "--output_format=awk"],
    ) as proc:
        assert proc.stdin and proc.stdout
        proc.stdin.write("Raptors Toronto    58 24 0.707\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Raptors Toronto    58 24 0.707\n"
        proc.stdin.write("Celtics Boston     49 33 0.598\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Celtics Boston     49 33 0.598\n"


def test_closed_stdout(pyolin):
    with pyolin.popen(
        "cfg.parser.has_header = False; line",
        extra_args=["--input_format=awk", "--output_format=awk"],
    ) as proc:
        assert proc.stdin and proc.stdout and proc.stderr
        proc.stdin.write("Raptors Toronto    58 24 0.707\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Raptors Toronto    58 24 0.707\n"
        # Command line tools like `head` will close the pipe when it is done
        # getting the data it needs. Make sure this doesn't crash
        proc.stdout.close()
        proc.stdin.write("Celtics Boston     49 33 0.598\n")
        proc.stdin.close()
        # proc.stdin.flush()
        errmsg = proc.stderr.read()
        assert errmsg == "", errmsg


def test_streaming_stdin_binary(pyolin):
    with pyolin.popen(
        "file[:2]",
        extra_args=["--output_format=binary"],
        text=False,
    ) as proc:
        stdout, _ = proc.communicate(b"\x30\x62\x43\x00")  # type: ignore
        assert stdout == b"\x30\x62"


def test_streaming_slice(pyolin):
    with pyolin.popen(
        "cfg.parser.has_header = False; records[:2]",
        extra_args=["--input_format=awk", "--output_format=awk"],
    ) as proc:
        assert proc.stdin and proc.stdout
        proc.stdin.write("Raptors Toronto    58 24 0.707\n")
        proc.stdin.write("Celtics Boston     49 33 0.598\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Raptors Toronto 58 24 0.707\n"
            assert proc.stdout.readline() == "Celtics Boston 49 33 0.598\n"
        proc.stdin.write("Write more stuff...\n")


def test_streaming_index(pyolin):
    with pyolin.popen(
        "cfg.parser.has_header = False; records[1].str",
        extra_args=["--input_format=awk", "--output_format=awk"],
    ) as proc:
        assert proc.stdin and proc.stdout
        proc.stdin.write("Raptors Toronto    58 24 0.707\n")
        proc.stdin.write("Celtics Boston     49 33 0.598\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Celtics Boston     49 33 0.598\n"
        proc.stdin.write("Write more stuff...\n")


def test_streaming_index_with_auto_parser(pyolin):
    with pyolin.popen(
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
            assert proc.stdout.readline() == "Celtics Boston     49 33 0.598\n"
        proc.stdin.write("Write more stuff...\n")


def test_records_index(pyolin):
    assert (
        pyolin("records[1]")
        == """\
        Raptors Toronto 58 24 0.707
        """
    )


def test_destructuring(pyolin):
    assert (
        pyolin("city for team, city, _, _, _ in records")
        == """\
        | value        |
        | ------------ |
        | Milwaukee    |
        | Toronto      |
        | Philadelphia |
        | Boston       |
        | Indiana      |
        """
    )


def test_percentage(pyolin):
    assert pyolin(
        "(r[0], round(r[3] / sum(r[3] for r in records), 2)) for r in records"
    ) == (
        """\
        | 0       | 1    |
        | ------- | ---- |
        | Bucks   | 0.15 |
        | Raptors | 0.17 |
        | 76ers   | 0.22 |
        | Celtics | 0.23 |
        | Pacers  | 0.24 |
        """
    )


def test_singleton_tuple(pyolin):
    """
    Tuples are treated as fields in a single record, whereas other iterable
    types are treated as multiple records.
    """
    assert (
        pyolin("sum(r[3] for r in records), max(r[3] for r in records)")
        == """\
        144 34
        """
    )


def test_module_import(pyolin):
    assert (
        pyolin(
            'record[0] if fnmatch.fnmatch(record[0], "*.txt")',
            input_=File("data_files.txt"),
        )
        == """\
        | value                  |
        | ---------------------- |
        | dir/file.txt           |
        | dir/file1.txt          |
        | dir/fileb.txt          |
        | dir/subdir/subfile.txt |
        """
    )


def test_record_variables(pyolin):
    assert pyolin(
        "type(record).__name__, type(record.source).__name__, type(fields).__name__"
    ) == (
        """\
        | 0      | 1            | 2      |
        | ------ | ------------ | ------ |
        | Record | DeferredType | Record |
        | Record | DeferredType | Record |
        | Record | DeferredType | Record |
        | Record | DeferredType | Record |
        | Record | DeferredType | Record |
        """
    )


def test_file_variables(pyolin):
    assert (
        pyolin(
            "type(lines).__name__, type(records).__name__, "
            "type(file).__name__, type(contents).__name__"
        )
        == """\
        StreamingSequence RecordSequence DeferredType DeferredType
        """
    )


def test_boolean(pyolin):
    assert (
        pyolin("record[0].bool, record[1].bool", input_=b"0 1")
        == """\
        | 0     | 1    |
        | ----- | ---- |
        | False | True |
        """
    )


def test_awk_header_detection(pyolin):
    assert pyolin(
        "record if record[1].bool", input_=File("data_files_with_header.txt")
    ) == (
        """\
        | Path       | IsDir | Size | Score |
        | ---------- | ----- | ---- | ----- |
        | dir        | True  | 30   | 40.0  |
        | dir/subdir | True  | 12   | 42.0  |
        """
    )


def test_awk_output_with_header(pyolin):
    assert (
        pyolin(
            "record if record[1].bool",
            input_=File("data_files_with_header.txt"),
            output_format="awk",
        )
        == """\
        Path IsDir Size Score
        dir True 30 40.0
        dir/subdir True 12 42.0
        """
    )


def test_filename(pyolin):
    assert (
        pyolin("filename", input_=File("data_files.txt"))
        == f"""\
        {os.path.dirname(__file__)}/data_files.txt
        """
    )


def test_bytes(pyolin):
    assert (
        pyolin('b"hello"')
        == """\
        hello
        """
    )


def test_reversed(pyolin):
    assert pyolin("reversed(lines)") == (
        """\
        | value                          |
        | ------------------------------ |
        | Pacers Indiana     48 34 0.585 |
        | Celtics Boston     49 33 0.598 |
        | 76ers Philadelphia 51 31 0.622 |
        | Raptors Toronto    58 24 0.707 |
        | Bucks Milwaukee    60 22 0.732 |
        """
    )


def test_in_operator(pyolin):
    assert pyolin('"Raptors Toronto    58 24 0.707" in lines') == (
        """\
        True
        """
    )


def test_url_quote(pyolin):
    assert pyolin("urllib.parse.quote(line)") == (
        """\
        | value                                          |
        | ---------------------------------------------- |
        | Bucks%20Milwaukee%20%20%20%2060%2022%200.732   |
        | Raptors%20Toronto%20%20%20%2058%2024%200.707   |
        | 76ers%20Philadelphia%2051%2031%200.622         |
        | Celtics%20Boston%20%20%20%20%2049%2033%200.598 |
        | Pacers%20Indiana%20%20%20%20%2048%2034%200.585 |
        """
    )


def test_fields_equal(pyolin):
    assert (
        pyolin(
            "fields[2], fields[3], fields[2] == fields[3]",
            input_=File("data_files.txt"),
        )
        == """\
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
        """
    )


def test_fields_comparison(pyolin):
    assert (
        pyolin(
            "fields[2], fields[3], fields[2] >= fields[3]",
            input_=File("data_files.txt"),
        )
        == """\
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
        """
    )


def test_multiplication(pyolin):
    assert (
        pyolin("fields[3] * 10")
        == """\
        | value |
        | ----- |
        | 220   |
        | 240   |
        | 310   |
        | 330   |
        | 340   |
        """
    )


def test_fields_multiplication(pyolin):
    assert (
        pyolin("fields[3] * fields[2]")
        == """\
        | value |
        | ----- |
        | 1320  |
        | 1392  |
        | 1581  |
        | 1617  |
        | 1632  |
        """
    )


def test_string_multiplication(pyolin):
    assert (
        pyolin("fields[0] * 2")
        == """\
        | value          |
        | -------------- |
        | BucksBucks     |
        | RaptorsRaptors |
        | 76ers76ers     |
        | CelticsCeltics |
        | PacersPacers   |
        """
    )


def test_pandas_dataframe(pyolin):
    assert (
        pyolin("df")
        == """\
        | 0       | 1            | 2  | 3  | 4     |
        | ------- | ------------ | -- | -- | ----- |
        | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
        | Raptors | Toronto      | 58 | 24 | 0.707 |
        | 76ers   | Philadelphia | 51 | 31 | 0.622 |
        | Celtics | Boston       | 49 | 33 | 0.598 |
        | Pacers  | Indiana      | 48 | 34 | 0.585 |
        """
    )


def test_pandas_dtypes(pyolin):
    assert (
        pyolin("df.dtypes")
        == """\
        | value   |
        | ------- |
        | object  |
        | object  |
        | int64   |
        | int64   |
        | float64 |
        """
    )


def test_panda_numeric_operations(pyolin):
    assert (
        pyolin("df[2] * 2")
        == """\
        | value |
        | ----- |
        | 120   |
        | 116   |
        | 102   |
        | 98    |
        | 96    |
        """
    )


def test_numpy_numeric_operations(pyolin):
    assert (
        pyolin("np.power(df[2], 2)")
        == """\
        | value |
        | ----- |
        | 3600  |
        | 3364  |
        | 2601  |
        | 2401  |
        | 2304  |
        """
    )


def test_field_separator(pyolin):
    assert pyolin(
        "record",
        input_=File("data_grades_simple_csv.csv"),
        field_separator=r",",
    ) == (
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
        """  # noqa: E501
    )


def test_field_separator_regex(pyolin):
    assert pyolin(
        "record",
        input_=File("data_grades_simple_csv.csv"),
        field_separator=r"[\.,]",
        input_format="awk",
    ) == (
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
        """  # noqa: E501
    )


def test_record_separator(pyolin):
    assert (
        pyolin(
            "record",
            input_=File("data_onerow.csv"),
            record_separator=r",",
        )
        == """\
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
        """
    )


def test_record_separator_multiple_chars(pyolin):
    assert (
        pyolin(
            "cfg.parser.has_header=False; record",
            input_=File("data_onerow.csv"),
            record_separator=r",2",
        )
        == """\
        | value    |
        | -------- |
        | JET      |
        | 0031201  |
        | 0001006  | 53521 | 1.000E+01 | NBIC | HSELM | TRANS |
        | .000E+00 | 1.000E+00 |
        |          | 1 | 0 | 0 |
        """
    )


def test_record_separator_regex(pyolin):
    assert (
        pyolin(
            "record",
            input_=File("data_onerow.csv"),
            record_separator=r"[,.]",
        )
        == """\
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
        """
    )


def test_simple_csv(pyolin):
    assert (
        pyolin(
            "df[[0, 1, 2]]",
            input_=File("data_grades_simple_csv.csv"),
            input_format="csv",
        )
        == """\
        | 0         | 1          | 2           |
        | --------- | ---------- | ----------- |
        | Alfalfa   | Aloysius   | 123-45-6789 |
        | Alfred    | University | 123-12-1234 |
        | Gerty     | Gramma     | 567-89-0123 |
        | Android   | Electric   | 087-65-4321 |
        | Franklin  | Benny      | 234-56-2890 |
        | George    | Boy        | 345-67-3901 |
        | Heffalump | Harvey     | 632-79-9439 |
        """
    )


def test_quoted_csv(pyolin):
    assert (
        pyolin(
            "record[0]",
            input_=File("data_news_decline.csv"),
            input_format="csv",
        )
        == """\
        | value            |
        | ---------------- |
        | 60 Minutes       |
        | 48 Hours Mystery |
        | 20/20            |
        | Nightline        |
        | Dateline Friday  |
        | Dateline Sunday  |
        """
    )


def test_empty_record(pyolin):
    in_ = """\
    something

    something
    """
    assert pyolin("bool(record)", input_=in_) == (
        """\
        | value |
        | ----- |
        | True  |
        | False |
        | True  |
        """
    )


def test_quoted_csv_str(pyolin):
    assert (
        pyolin(
            "record.str",
            input_=File("data_news_decline.csv"),
            input_format="csv",
        )
        == """\
        | value                             |
        | --------------------------------- |
        | "60 Minutes",       7.6, 7.4, 7.3 |
        | "48 Hours Mystery", 4.1, 3.9, 3.6 |
        | "20/20",            4.1, 3.7, 3.3 |
        | "Nightline",        2.7, 2.6, 2.7 |
        | "Dateline Friday",  4.1, 4.1, 3.9 |
        | "Dateline Sunday",  3.5, 3.2, 3.1 |
        """
    )


def test_auto_csv(pyolin):
    assert (
        pyolin(
            "df[[0,1,2]]",
            input_=File("data_addresses.csv"),
            input_format="csv",
        )
        == """\
        | 0                     | 1        | 2                                |
        | --------------------- | -------- | -------------------------------- |
        | John                  | Doe      | 120 jefferson st.                |
        | Jack                  | McGinnis | 220 hobo Av.                     |
        | John "Da Man"         | Repici   | 120 Jefferson St.                |
        | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
        |                       | Blankman |                                  |
        | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
        """
    )


def test_csv_excel(pyolin):
    assert (
        pyolin(
            "df[[0,1,2]]",
            input_=File("data_addresses.csv"),
            input_format="csv_excel",
        )
        == """\
        | 0                     | 1        | 2                                |
        | --------------------- | -------- | -------------------------------- |
        | John                  | Doe      | 120 jefferson st.                |
        | Jack                  | McGinnis | 220 hobo Av.                     |
        | John "Da Man"         | Repici   | 120 Jefferson St.                |
        | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
        |                       | Blankman |                                  |
        | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
        """
    )


def test_csv_unix(pyolin):
    assert (
        pyolin(
            '"|".join((record[0], record[1], record[2]))',
            input_=File("data_addresses_unix.csv"),
            input_format="csv",
        )
        == """\
        | value                                          |
        | ---------------------------------------------- |
        | John|Doe|120 jefferson st.                     |
        | Jack|McGinnis|220 hobo Av.                     |
        | John "Da Man"|Repici|120 Jefferson St.         |
        | Stephen|Tyler|7452 Terrace "At the Plaza" road |
        | |Blankman|                                     |
        | Joan "the bone", Anne|Jet|9th, at Terrace plc  |
        """
    )


def test_quoted_tsv(pyolin):
    assert pyolin(
        "record[0], record[2]",
        input_=File("data_news_decline.tsv"),
        input_format="csv",
        field_separator="\t",
    ) == (
        """\
        | 0                | 1   |
        | ---------------- | --- |
        | 60 Minutes       | 7.4 |
        | 48 Hours Mystery | 3.9 |
        | 20/20            | 3.7 |
        | Nightline        | 2.6 |
        | Dateline Friday  | 4.1 |
        | Dateline Sunday  | 3.2 |
        """
    )


def test_statement(pyolin):
    assert (
        pyolin("a = record[2]; b = 1; a + b")
        == """\
        | value |
        | ----- |
        | 61    |
        | 59    |
        | 52    |
        | 50    |
        | 49    |
        """
    )


def test_statement_table(pyolin):
    assert (
        pyolin("a = len(records); b = 2; a * b")
        == """\
        10
        """
    )


def test_input_too_long(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("records[0]", input_=b"t" * 4001)
    assert str(exc.value.__cause__.__cause__) == textwrap.dedent(  # type: ignore
        """\
        Unable to detect input format. Try specifying the input type with --input_format"""
    )


def test_input_long_but_broken_into_lines(pyolin):
    assert pyolin("records[0]", input_=b"t" * 2000 + b"\n" + b"x" * 3000) == (
        f"""\
        { "t" * 2000 }
        { "x" * 3000 }
        """
    )


def test_syntax_error(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("a..x")
    assert str(exc.value.__cause__) == textwrap.dedent(
        """\
        Invalid syntax:
          a..x
            ^"""
    )


def test_syntax_error_in_statement(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("a..x; a+1")
    assert str(exc.value.__cause__) == textwrap.dedent(
        """\
        Invalid syntax:
          a..x
            ^"""
    )


def test_header_detection(pyolin):
    assert (
        pyolin(
            'df[["Last name", "SSN", "Final"]]',
            input_=File("data_grades_with_header.csv"),
            input_format="csv",
        )
        == """\
        | Last name | SSN         | Final |
        | --------- | ----------- | ----- |
        | Alfalfa   | 123-45-6789 | 49    |
        | Alfred    | 123-12-1234 | 48    |
        | Gerty     | 567-89-0123 | 44    |
        | Android   | 087-65-4321 | 47    |
        | Franklin  | 234-56-2890 | 90    |
        | George    | 345-67-3901 | 4     |
        | Heffalump | 632-79-9439 | 40    |
        """
    )


def test_force_has_header(pyolin):
    assert pyolin(
        "cfg.parser.has_header = True; (r[0], r[2], r[7]) for r in records",
        input_=File("data_grades_simple_csv.csv"),
        input_format="csv",
    ) == (
        """\
        | Alfalfa   | 123-45-6789 | 49.0 |
        | --------- | ----------- | ---- |
        | Alfred    | 123-12-1234 | 48.0 |
        | Gerty     | 567-89-0123 | 44.0 |
        | Android   | 087-65-4321 | 47.0 |
        | Franklin  | 234-56-2890 | 90.0 |
        | George    | 345-67-3901 | 4.0  |
        | Heffalump | 632-79-9439 | 40.0 |
        """
    )


def test_header_detection_csv_excel(pyolin):
    assert (
        pyolin(
            'df[["Last Name", "Address"]]',
            input_=File("data_addresses_with_header.csv"),
            input_format="csv_excel",
        )
        == """\
        | Last Name             | Address                          |
        | --------------------- | -------------------------------- |
        | John                  | 120 jefferson st.                |
        | Jack                  | 220 hobo Av.                     |
        | John "Da Man"         | 120 Jefferson St.                |
        | Stephen               | 7452 Terrace "At the Plaza" road |
        | Joan "the bone", Anne | 9th, at Terrace plc              |
        """
    )


def test_print_dataframe_header(pyolin):
    assert (
        pyolin(
            "list(df.columns.values)",
            input_=File("data_grades_with_header.csv"),
            input_format="csv",
        )
        == """\
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
        """
    )


def test_assign_to_record(pyolin):
    """
    Try to confuse the parser by writing to a field called record
    """
    assert (
        pyolin("record=1; record+1")
        == """\
        2
        """
    )


def test_access_record_and_table(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("a = record[0]; b = records; b")
    assert 'Cannot change scope from "record" to "file"' == str(
        exc.value.__cause__.__cause__  # type: ignore
    )


def test_access_table_and_record(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("a = records; b = record[0]; b")
    assert 'Cannot change scope from "file" to "record"' in str(exc.value.__cause__)


def test_empty_record_scoped(pyolin):
    assert pyolin("record[0]", input_=File(os.devnull)) == ""


def test_empty_table_scoped(pyolin):
    assert pyolin("record for record in records", input_=File(os.devnull)) == ""


def test_semicolon_in_string(pyolin):
    assert pyolin('"hello; world"') == "hello; world\n"


def test_stack_trace_cleaning(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("urllib.parse.quote(12345)")
    assert isinstance(exc.value.__cause__, UserError)
    formatted = exc.value.__cause__.formatted_tb()
    assert 5 == len(formatted), pformat(formatted)
    assert "Traceback (most recent call last)" in formatted[0]
    assert "pyolin_user_prog.py" in formatted[1]
    assert "return quote_from_bytes" in formatted[2]
    assert '"quote_from_bytes() expected bytes"' in formatted[3]
    assert "quote_from_bytes() expected bytes" in formatted[4]


def test_invalid_output_format(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("1+1", output_format="invalid")
    assert 'Unrecognized output format "invalid"' == str(exc.value.__cause__)


def test_csv_output_format(pyolin):
    assert (
        pyolin(
            "records",
            output_format="csv",
        )
        == """\
        Bucks,Milwaukee,60,22,0.732\r
        Raptors,Toronto,58,24,0.707\r
        76ers,Philadelphia,51,31,0.622\r
        Celtics,Boston,49,33,0.598\r
        Pacers,Indiana,48,34,0.585\r
        """
    )


def test_csv_output_format_unix(pyolin):
    assert (
        pyolin(
            "cfg.printer.dialect = csv.unix_dialect; records",
            output_format="csv",
        )
        == """\
        "Bucks","Milwaukee","60","22","0.732"
        "Raptors","Toronto","58","24","0.707"
        "76ers","Philadelphia","51","31","0.622"
        "Celtics","Boston","49","33","0.598"
        "Pacers","Indiana","48","34","0.585"
        """
    )


def test_csv_output_invalid_dialect(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin('cfg.printer.dialect = "invalid"; records', output_format="csv")
    assert 'Unknown dialect "invalid"' == str(exc.value.__cause__)


def test_csv_output_format_delimiter(pyolin):
    assert (
        pyolin(
            'cfg.printer.delimiter = "^"; records',
            output_format="csv",
        )
        == """\
        Bucks^Milwaukee^60^22^0.732\r
        Raptors^Toronto^58^24^0.707\r
        76ers^Philadelphia^51^31^0.622\r
        Celtics^Boston^49^33^0.598\r
        Pacers^Indiana^48^34^0.585\r
        """
    )


def test_csv_output_non_tuple(pyolin):
    assert (
        pyolin(
            "record[2]",
            output_format="csv",
        )
        == """\
        60\r
        58\r
        51\r
        49\r
        48\r
        """
    )


def test_csv_output_quoting(pyolin):
    assert pyolin(
        "records",
        input_format="csv",
        output_format="csv",
        input_=File("data_addresses.csv"),
    ) == (
        '''\
        John,Doe,120 jefferson st.,Riverside, NJ, 08075\r
        Jack,McGinnis,220 hobo Av.,Phila, PA,09119\r
        "John ""Da Man""",Repici,120 Jefferson St.,Riverside, NJ,08075\r
        Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234\r
        ,Blankman,,SomeTown, SD, 00298\r
        "Joan ""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123\r
        '''
    )


def test_csv_output_with_header(pyolin):
    assert (
        pyolin(
            'cfg.printer.print_header = True; df[["Last name", "SSN", "Final"]]',
            input_=File("data_grades_with_header.csv"),
            input_format="csv",
            output_format="csv",
        )
        == """\
        Last name,SSN,Final\r
        Alfalfa,123-45-6789,49\r
        Alfred,123-12-1234,48\r
        Gerty,567-89-0123,44\r
        Android,087-65-4321,47\r
        Franklin,234-56-2890,90\r
        George,345-67-3901,4\r
        Heffalump,632-79-9439,40\r
        """
    )


def test_csv_output_with_header_function(pyolin):
    def func():
        cfg.printer.print_header = True  # type: ignore  # noqa: F821
        return df[["Last name", "SSN", "Final"]]  # type: ignore  # noqa: F821

    assert (
        pyolin(
            func,
            input_=File("data_grades_with_header.csv"),
            input_format="csv",
            output_format="csv",
        )
        == """\
        Last name,SSN,Final\r
        Alfalfa,123-45-6789,49\r
        Alfred,123-12-1234,48\r
        Gerty,567-89-0123,44\r
        Android,087-65-4321,47\r
        Franklin,234-56-2890,90\r
        George,345-67-3901,4\r
        Heffalump,632-79-9439,40\r
        """
    )


def test_streaming_stdin_csv(pyolin):
    with pyolin.popen(
        "cfg.parser.has_header = False; record",
        extra_args=["--output_format", "csv", "--input_format", "awk"],
    ) as proc:
        assert proc.stdin and proc.stdout
        proc.stdin.write("Raptors Toronto    58 24 0.707\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Raptors,Toronto,58,24,0.707\n"
        proc.stdin.write("Celtics Boston     49 33 0.598\n")
        proc.stdin.flush()
        with timeout(2):
            assert proc.stdout.readline() == "Celtics,Boston,49,33,0.598\n"


def test_numeric_header(pyolin):
    assert (
        pyolin(
            "cfg.printer.print_header = True; record[0],record[2],record[7]",
            input_=File("data_grades_simple_csv.csv"),
            input_format="csv",
            output_format="csv",
        )
        == """\
        0,1,2\r
        Alfalfa,123-45-6789,49.0\r
        Alfred,123-12-1234,48.0\r
        Gerty,567-89-0123,44.0\r
        Android,087-65-4321,47.0\r
        Franklin,234-56-2890,90.0\r
        George,345-67-3901,4.0\r
        Heffalump,632-79-9439,40.0\r
        """
    )


def test_markdown_output(pyolin):
    assert (
        pyolin(
            'df[["Last name", "SSN", "Final"]]',
            input_=File("data_grades_with_header.csv"),
            input_format="csv",
            output_format="markdown",
        )
        == """\
        | Last name | SSN         | Final |
        | --------- | ----------- | ----- |
        | Alfalfa   | 123-45-6789 | 49    |
        | Alfred    | 123-12-1234 | 48    |
        | Gerty     | 567-89-0123 | 44    |
        | Android   | 087-65-4321 | 47    |
        | Franklin  | 234-56-2890 | 90    |
        | George    | 345-67-3901 | 4     |
        | Heffalump | 632-79-9439 | 40    |
        """
    )


def test_tsv_output(pyolin):
    assert (
        pyolin(
            "records",
            output_format="tsv",
        )
        == """\
        Bucks	Milwaukee	60	22	0.732\r
        Raptors	Toronto	58	24	0.707\r
        76ers	Philadelphia	51	31	0.622\r
        Celtics	Boston	49	33	0.598\r
        Pacers	Indiana	48	34	0.585\r
        """
    )


def test_multiline_input(pyolin):
    assert (
        pyolin(
            textwrap.dedent(
                """\
        record = 1
        record + 1\
        """
            )
        )
        == """\
        2
        """
    )


def test_multiline_mixed_input(pyolin):
    assert (
        pyolin(
            textwrap.dedent(
                """\
            record = 1; record += 1
            record += 1; record + 1
            """
            )
        )
        == """\
        4
        """
    )


def test_last_statement(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("1+1;pass")
    assert (
        textwrap.dedent(
            """\
            Cannot evaluate value from statement:
              pass"""
        )
        == str(exc.value.__cause__)
    )


def test_multiline_python_program(pyolin):
    assert (
        pyolin(
            textwrap.dedent(
                """\
            result = []
            for i in range(5):
                result.append(range(i + 1))
            result
            """
            ),
            output_format="awk",
        )
        == """\
        0
        0 1
        0 1 2
        0 1 2 3
        0 1 2 3 4
        """
    )


def test_markdown_wrapping(pyolin):
    with mock.patch.dict(os.environ, {"PYOLIN_TABLE_WIDTH": "80"}):
        assert pyolin(
            'df[["marketplace", "review_body", "star_rating"]]',
            input_=File("data_amazon_reviews.tsv"),
            input_format="tsv",
            output_format="markdown",
        ) == (
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
            """
        )


def test_markdown_wrapping2(pyolin):
    with mock.patch.dict(os.environ, {"PYOLIN_TABLE_WIDTH": "80"}):
        assert pyolin(
            "records",
            input_=File("data_formatting.txt"),
            output_format="markdown",
        ) == (
            """\
            | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11    | 12           | 13  | 14 |
            | - | - | - | - | - | - | - | - | - | - | -- | ----- | ------------ | --- | -- |
            | a | b | c | d | e | f | g | h | i | j | k  | lmnop | qrstuv123456 | wxy | z  |
            :   :   :   :   :   :   :   :   :   :   :    :       : 789123456789 :     :    :
            :   :   :   :   :   :   :   :   :   :   :    :       : 123456789    :     :    :
            """
        )


def test_json_output(pyolin):
    assert (
        pyolin(
            "records[0]",
            output_format="json",
        )
        == """\
        [
          "Bucks",
          "Milwaukee",
          60,
          22,
          0.732
        ]
        """
    )


def test_jsonl_output(pyolin):
    assert (
        pyolin(
            "records",
            output_format="jsonl",
        )
        == """\
        ["Bucks", "Milwaukee", 60, 22, 0.732]
        ["Raptors", "Toronto", 58, 24, 0.707]
        ["76ers", "Philadelphia", 51, 31, 0.622]
        ["Celtics", "Boston", 49, 33, 0.598]
        ["Pacers", "Indiana", 48, 34, 0.585]
        """
    )


def test_json_output_with_manual_header(pyolin):
    assert (
        pyolin(
            "cfg.header = ['team', 'city', 'wins', 'loss', 'Win rate']; records[0]",
            output_format="json",
        )
        == """\
        {
          "team": "Bucks",
          "city": "Milwaukee",
          "wins": 60,
          "loss": 22,
          "Win rate": 0.732
        }
        """
    )


def test_jsonl_output_with_manual_header(pyolin):
    assert (
        pyolin(
            "cfg.header = ['team', 'city', 'wins', 'loss', 'Win rate']; records",
            output_format="jsonl",
        )
        == """\
        {"team": "Bucks", "city": "Milwaukee", "wins": 60, "loss": 22, "Win rate": 0.732}
        {"team": "Raptors", "city": "Toronto", "wins": 58, "loss": 24, "Win rate": 0.707}
        {"team": "76ers", "city": "Philadelphia", "wins": 51, "loss": 31, "Win rate": 0.622}
        {"team": "Celtics", "city": "Boston", "wins": 49, "loss": 33, "Win rate": 0.598}
        {"team": "Pacers", "city": "Indiana", "wins": 48, "loss": 34, "Win rate": 0.585}
        """
    )


def test_json_output_with_header(pyolin):
    assert (
        pyolin(
            "records",
            input_=File("data_files_with_header.txt"),
            output_format="json",
        )
        == """\
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
        """
    )


def test_json_input(pyolin):
    assert (
        pyolin(
            "df",
            input_=File("data_colors.json"),
            input_format="json",
            output_format="markdown",
        )
        == """\
        | color   | value |
        | ------- | ----- |
        | red     | #f00  |
        | green   | #0f0  |
        | blue    | #00f  |
        | cyan    | #0ff  |
        | magenta | #f0f  |
        | yellow  | #ff0  |
        | black   | #000  |
        """
    )


def test_jsonl_input(pyolin):
    """One JSON object per line"""
    in_ = """\
    { "color": "red", "value": "#f00" }
    { "color": "green", "value": "#0f0" }
    { "color": "blue", "value": "#00f" }
    """
    assert pyolin(
        "records",
        input_=in_,
        input_format="json",
        output_format="markdown",
    ) == (
        """\
        | color | value |
        | ----- | ----- |
        | red   | #f00  |
        | green | #0f0  |
        | blue  | #00f  |
        """
    )


def test_contains(pyolin):
    assert (
        pyolin(
            '("green", "#0f0") in records',
            input_=File("data_colors.json"),
            input_format="json",
        )
        == """\
        True
        """
    )


@pytest.mark.parametrize(
    "prog, expected",
    [
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
            "record.source",
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
    ],
)
def test_markdown_output_format(pyolin, prog, expected):
    assert (
        pyolin(
            prog,
            input_=File("data_colors.json"),
            input_format="json",
            output_format="markdown",
        )
        == expected
    )


def test_markdown_non_uniform_column_count(pyolin):
    assert (
        pyolin(
            "range(i) for i in range(1, 5)",
            output_format="markdown",
        )
        == """\
        | value |
        | ----- |
        | 0     |
        | 0     | 1 |
        | 0     | 1 | 2 |
        | 0     | 1 | 2 | 3 |
        """
    )


def test_repr_printer(pyolin):
    assert (
        pyolin(
            "range(10)",
            output_format="repr",
        )
        == """\
        range(0, 10)
        """
    )


def test_repr_printer_table(pyolin):
    assert pyolin("records", output_format="repr") == (
        """\
        [('Bucks', 'Milwaukee', '60', '22', '0.732'), ('Raptors', 'Toronto', '58', '24', '0.707'), ('76ers', 'Philadelphia', '51', '31', '0.622'), ('Celtics', 'Boston', '49', '33', '0.598'), ('Pacers', 'Indiana', '48', '34', '0.585')]
        """  # noqa: E501
    )


def test_repr_printer_records(pyolin):
    assert (
        pyolin(
            '"aloha\u2011\u2011\u2011"',
            output_format="repr",
        )
        == """\
        'aloha\u2011\u2011\u2011'
        """
    )


def test_str_printer_records(pyolin):
    assert (
        pyolin(
            '"aloha\u2011\u2011\u2011"',
            output_format="str",
        )
        == """\
        aloha\u2011\u2011\u2011
        """
    )


def test_str_printer_table(pyolin):
    assert pyolin(
        "records",
        output_format="str",
    ) == (
        "[('Bucks', 'Milwaukee', '60', '22', '0.732'), "
        "('Raptors', 'Toronto', '58', '24', '0.707'), "
        "('76ers', 'Philadelphia', '51', '31', '0.622'), "
        "('Celtics', 'Boston', '49', '33', '0.598'), "
        "('Pacers', 'Indiana', '48', '34', '0.585')]\n"
    )


def test_set_printer(pyolin):
    assert (
        pyolin('cfg.printer = new_printer("repr"); range(10)')
        == """\
        range(0, 10)
        """
    )


def test_printer_none(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("cfg.printer = None; 123")
    assert 'printer must be an instance of Printer. Found "None" instead' == str(
        exc.value.__cause__
    )


def test_raise_stop_iteration(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("raise StopIteration(); None")
    assert isinstance(exc.value.__cause__, UserError)
    formatted = exc.value.__cause__.formatted_tb()
    assert 3 == len(formatted), pformat(formatted)
    assert "Traceback (most recent call last)" in formatted[0]
    assert "pyolin_user_prog.py" in formatted[1]
    assert "StopIteration" in formatted[2]


def test_binary_input_len(pyolin):
    assert pyolin("len(file.bytes)", input_=File("data_pickle")) == "21\n"


def test_binary_input_len_non_unicode(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("len(file)", input_=b"\x00\xff\x03")
    assert "Cannot get length of str containing non-UTF8" in str(exc.value.__cause__)


def test_binary_input_len_bytes_non_unicode(pyolin):
    assert pyolin("len(file.bytes)", input_=b"\x00\xff\x03") == "3\n"


def test_binary_input_can_be_accessed(pyolin):
    assert (
        pyolin("type(file).__name__", input_=File("data_pickle"))
        == """\
        DeferredType
        """
    )


def test_binary_input_pickle(pyolin):
    assert (
        pyolin("pickle.loads(file.bytes)", input_=File("data_pickle"))
        == """\
        hello world
        """
    )


def test_binary_printer(pyolin):
    assert (
        pyolin(
            'b"\\x30\\x62\\x43\\x00"',
            output_format="binary",
        )
        == b"\x30\x62\x43\x00"
    )


@pytest.mark.parametrize(
    "input_file, expected_output",
    [
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
    ],
)
def test_auto_parser(pyolin, input_file, expected_output):
    assert (
        pyolin(
            "records[0]",
            input_=File(input_file),
            output_format="repr",
        )
        == repr(expected_output) + "\n"
    )


def test_binary_input_access_records(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("records", input_=File("data_pickle"))

    assert "`record`-based attributes are not supported for binary inputs" == str(
        exc.value.__cause__
    )


def test_set_field_separator(pyolin):
    assert (
        pyolin(
            'cfg.parser.field_separator = ","; record',
            input_=File("data_grades_simple_csv.csv"),
            input_format="tsv",  # Make sure we can change the field separator from "\t" to ","
        )
        == """\
        | 0         | 1          | 2           | 3    | 4    | 5     | 6    | 7    | 8  |
        | --------- | ---------- | ----------- | ---- | ---- | ----- | ---- | ---- | -- |
        | Alfalfa   | Aloysius   | 123-45-6789 | 40.0 | 90.0 | 100.0 | 83.0 | 49.0 | D- |
        | Alfred    | University | 123-12-1234 | 41.0 | 97.0 | 96.0  | 97.0 | 48.0 | D+ |
        | Gerty     | Gramma     | 567-89-0123 | 41.0 | 80.0 | 60.0  | 40.0 | 44.0 | C  |
        | Android   | Electric   | 087-65-4321 | 42.0 | 23.0 | 36.0  | 45.0 | 47.0 | B- |
        | Franklin  | Benny      | 234-56-2890 | 50.0 | 1.0  | 90.0  | 80.0 | 90.0 | B- |
        | George    | Boy        | 345-67-3901 | 40.0 | 1.0  | 11.0  | -1.0 | 4.0  | B  |
        | Heffalump | Harvey     | 632-79-9439 | 30.0 | 1.0  | 20.0  | 30.0 | 40.0 | C  |
        """
    )


def test_set_record_separator(pyolin):
    assert (
        pyolin(
            'cfg.parser.record_separator = ","; record',
            input_=File("data_onerow.csv"),
        )
        == """\
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
        """
    )


def test_set_parser_json(pyolin):
    assert (
        pyolin(
            'cfg.parser = new_parser("json"); df',
            input_=File("data_colors.json"),
        )
        == """\
        | color   | value |
        | ------- | ----- |
        | red     | #f00  |
        | green   | #0f0  |
        | blue    | #00f  |
        | cyan    | #0ff  |
        | magenta | #f0f  |
        | yellow  | #ff0  |
        | black   | #000  |
        """
    )


def test_set_parser_record(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("a = records[0]; cfg.parser = 123; cfg.header = (); 123")
    assert "Parsing already started, cannot set parser" in str(exc.value.__cause__)


def test_records_if_undefined(pyolin):
    assert (
        pyolin("records if False")
        == """\
        """
    )


def test_gen_records_if_undefined(input_file_nba):
    assert pyolin.run("records if False", input_=input_file_nba) == _UNDEFINED_


@pytest.mark.parametrize(
    "output_format, expected",
    [
        ("repr", "Undefined()\n"),
        ("str", ""),
        ("json", ""),
        ("jsonl", ""),
        ("awk", ""),
        ("auto", ""),
        ("binary", ""),
        ("csv", ""),
    ],
)
def test_undefined(pyolin, output_format, expected):
    assert (
        pyolin(
            "_UNDEFINED_",
            output_format=output_format,
        )
        == expected
    )


def test_name_error(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("idontknowwhatisthis + 1")
    assert "name 'idontknowwhatisthis' is not defined" in str(exc.value.__cause__)


def test_record_first(pyolin):
    assert (
        pyolin(
            "global mysum\n"
            'if record.first: mysum = 0; cfg.header = ("sum", "value")\n'
            "mysum += record[2]\n"
            "mysum, record[2]"
        )
        == """\
        | sum | value |
        | --- | ----- |
        | 60  | 60    |
        | 118 | 58    |
        | 169 | 51    |
        | 218 | 49    |
        | 266 | 48    |
        """
    )


def test_record_num(pyolin):
    assert (
        pyolin("record.num")
        == """\
        | value |
        | ----- |
        | 0     |
        | 1     |
        | 2     |
        | 3     |
        | 4     |
        """
    )


def test_trailing_newline(pyolin):
    assert (
        pyolin("records\n")
        == """\
        | 0       | 1            | 2  | 3  | 4     |
        | ------- | ------------ | -- | -- | ----- |
        | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
        | Raptors | Toronto      | 58 | 24 | 0.707 |
        | 76ers   | Philadelphia | 51 | 31 | 0.622 |
        | Celtics | Boston       | 49 | 33 | 0.598 |
        | Pacers  | Indiana      | 48 | 34 | 0.585 |
        """
    )


def test_execute_function(input_file_nba):
    def get_records():
        return records  # type: ignore  # noqa: F821

    assert pyolin.run(get_records, input_=input_file_nba.path()) == [
        ("Bucks", "Milwaukee", 60, 22, 0.732),
        ("Raptors", "Toronto", 58, 24, 0.707),
        ("76ers", "Philadelphia", 51, 31, 0.622),
        ("Celtics", "Boston", 49, 33, 0.598),
        ("Pacers", "Indiana", 48, 34, 0.585),
    ]


def test_execute_function_record_scoped(input_file_nba):
    def get_records():
        return record[0]  # type: ignore  # noqa: F821

    assert pyolin.run(get_records, input_=input_file_nba.path()) == [
        "Bucks",
        "Raptors",
        "76ers",
        "Celtics",
        "Pacers",
    ]


def test_double_semi_colon(pyolin):
    assert (
        pyolin("record = 1; record += 1;; record += 1; record + 1")
        == """\
        4
        """
    )


def test_if_record_first_double_semi_colon(pyolin):
    """
    Double semi-colon is treated as a newline
    """
    assert (
        pyolin(
            # Alternative, record-scoped way to write
            #   sum = 0; ((sum, record[2]) for record in records)
            "global sum;; if record.first: sum = 0;; sum += record[2]; sum, record[2]"
        )
        == """\
        | 0   | 1  |
        | --- | -- |
        | 60  | 60 |
        | 118 | 58 |
        | 169 | 51 |
        | 218 | 49 |
        | 266 | 48 |
        """
    )


def test_undefined_is_false():
    pyolin.run("bool(_UNDEFINED_)", False)


def test_end_with_double_semi_colon(pyolin):
    assert (
        pyolin("record[2];;")
        == """\
        | value |
        | ----- |
        | 60    |
        | 58    |
        | 51    |
        | 49    |
        | 48    |
        """
    )


def test_sys_argv(pyolin):
    """
    sys.argv should be shifted, so sys.argv[1] should be the first one after the pyolin prog
    """
    assert (
        pyolin(
            "sys.argv",
            extra_args=["testing", "1", "2", "3"],
        )
        == """\
        | value   |
        | ------- |
        | pyolin  |
        | testing |
        | 1       |
        | 2       |
        | 3       |
        """
    )


@pytest.mark.parametrize(
    "output_format, expected",
    [
        (
            "txt",
            """\
            color value
            red #f00
            """,
        ),
        (
            "json",
            """\
            {
              "color": "red",
              "value": "#f00"
            }
            """,
        ),
    ],
)
def test_manual_load_json_record(pyolin, output_format, expected):
    assert (
        pyolin(
            "json.loads(file)[0]",
            input_=File("data_colors.json"),
            output_format=output_format,
        )
        == expected
    )


@pytest.mark.parametrize(
    "output_format, expected",
    [
        (
            "txt",
            # There isn't really a correct format for arbitrary JSON when using txt output.
            # Currently each object has its key-value pair flattened into separate columns
            """\
            color value
            red #f00
            green #0f0
            blue #00f
            cyan #0ff
            magenta #f0f
            yellow #ff0
            black #000
            """,
        ),
        (
            "json",
            """\
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
        ),
        (
            "md",
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
    ],
)
def test_manual_load_json_output(pyolin, output_format, expected):
    assert (
        pyolin(
            "json.loads(file)",
            input_=File("data_colors.json"),
            output_format=output_format,
        )
        == expected
    )


def test_manual_load_csv(pyolin):
    assert pyolin(
        "csv.reader(io.StringIO(file))",
        input_=File("data_addresses.csv"),
    ) == (
        """\
        | 0                     | 1        | 2                                | 3           | 4   | 5      |
        | --------------------- | -------- | -------------------------------- | ----------- | --- | ------ |
        | John                  | Doe      | 120 jefferson st.                | Riverside   |  NJ |  08075 |
        | Jack                  | McGinnis | 220 hobo Av.                     | Phila       |  PA | 09119  |
        | John "Da Man"         | Repici   | 120 Jefferson St.                | Riverside   |  NJ | 08075  |
        | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road | SomeTown    | SD  |  91234 |
        |                       | Blankman |                                  | SomeTown    |  SD |  00298 |
        | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              | Desert City | CO  | 00123  |
        """  # noqa: E501
    )


def test_non_table_json(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("records", input_=File("data_json_example.json"), input_format="json")
        assert "TypeError: Input is not an array of objects" == str(exc.value.__cause__)


def test_jsonobj_string_output(pyolin):
    assert pyolin(
        "jsonobj['glossary']['title']",
        input_=File("data_json_example.json"),
        output_format="txt",
    ) == ("example glossary\n")


def test_jsonobjs_single_line(pyolin):
    in_ = """\
    {"a": 1, "b": 2}
    {"a": 2, "b": 3}
    {"a": 3, "b": 4}
    """
    assert pyolin(
        "jsonobjs",
        input_=in_,
        output_format="txt",
    ) == (
        """\
        a b
        1 2
        2 3
        3 4
        """
    )


def test_jsonobj_obj_output(pyolin):
    assert pyolin(
        "jsonobj['glossary']['GlossDiv']['GlossList']['GlossEntry']['GlossDef']",
        input_=File("data_json_example.json"),
        output_format="json",
    ) == (
        """\
        {
          "para": "A meta-markup language, used to create markup languages such as DocBook.",
          "GlossSeeAlso": [
            "GML",
            "XML"
          ]
        }
        """
    )


def test_3d_table(pyolin):
    assert (
        pyolin(
            "[['foo', ['a', 'b']], ['bar', ['c', 'd']]]",
            input_=File("data_json_example.json"),
        )
        == """\
        [
          [
            "foo",
            [
              "a",
              "b"
            ]
          ],
          [
            "bar",
            [
              "c",
              "d"
            ]
          ]
        ]
        """
    )


def test_multiline_json_prog(pyolin):
    assert (
        pyolin(
            textwrap.dedent(
                """\
                [
                    ['foo', ['a', 'b']], ['bar', ['c', 'd']]
                ]"""
            ),
            input_=File("data_json_example.json"),
        )
        == (
            """\
            [
              [
                "foo",
                [
                  "a",
                  "b"
                ]
              ],
              [
                "bar",
                [
                  "c",
                  "d"
                ]
              ]
            ]
            """
        )
    )


def test_json_with_undefined(pyolin):
    assert (
        pyolin(
            "[_UNDEFINED_, 'foo']",
            input_=File("data_json_example.json"),
            output_format="json",
        )
        == """\
        [
          "foo"
        ]
        """
    )


def test_records_negative_index(pyolin):
    assert (
        pyolin("records[-1]")
        == """\
        Pacers Indiana 48 34 0.585
        """
    )


def test_records_negative_slice_start(pyolin):
    assert (
        pyolin("records[-1:]")
        == """\
        | 0      | 1       | 2  | 3  | 4     |
        | ------ | ------- | -- | -- | ----- |
        | Pacers | Indiana | 48 | 34 | 0.585 |
        """
    )


def test_records_negative_slice_stop(pyolin):
    assert (
        pyolin("records[:-3]")
        == """\
        | 0       | 1         | 2  | 3  | 4     |
        | ------- | --------- | -- | -- | ----- |
        | Bucks   | Milwaukee | 60 | 22 | 0.732 |
        | Raptors | Toronto   | 58 | 24 | 0.707 |
        """
    )


def test_records_negative_slice_step(pyolin):
    assert (
        pyolin("records[::-1]")
        == """\
        | 0       | 1            | 2  | 3  | 4     |
        | ------- | ------------ | -- | -- | ----- |
        | Pacers  | Indiana      | 48 | 34 | 0.585 |
        | Celtics | Boston       | 49 | 33 | 0.598 |
        | 76ers   | Philadelphia | 51 | 31 | 0.622 |
        | Raptors | Toronto      | 58 | 24 | 0.707 |
        | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
        """
    )


# TODOs:
# Bash / Zsh autocomplete integration
# Multiline / interactive mode / ipython integration?
# Easier to define globals
# yield support
# ARGV that provides deferred typing
