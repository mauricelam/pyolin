import contextlib
import os
from pyolin.parser import UserError
from pyolin import pyolin
from pyolin.util import _UNDEFINED_
from pprint import pformat
import subprocess
import sys
import textwrap
import unittest
from unittest import mock

from .utils import ErrorWithStderr, run_capturing_output, timeout


def _test_file(file):
    return os.path.join(os.path.dirname(__file__), file)


def run_cli(prog, *, input='data_nba.txt', extra_args=(), **kwargs):
    with run_capturing_output(errmsg=f'Prog: {prog}') as output:
        pyolin._command_line(prog, *extra_args, input=_test_file(input), **kwargs)
        return output


def run_pyolin(prog, *, input='data_nba.txt', **kwargs):
    return pyolin.run(prog, input=_test_file(input), **kwargs)


@contextlib.contextmanager
def pyolinPopen(prog, extra_args=(), universal_newlines=True, **kwargs):
    with subprocess.Popen(
        [sys.executable, '.', prog] + extra_args,
        stdin=kwargs.get('stdin', subprocess.PIPE),
        stdout=kwargs.get('stdout', subprocess.PIPE),
        stderr=kwargs.get('stderr', subprocess.PIPE),
        universal_newlines=universal_newlines,
        **kwargs
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
                msg=f'''\
Prog: pyolin \'{prog}\' {_test_file(data)}
Expected:
{textwrap.indent(expected, '    ')}
---

Actual:
{textwrap.indent(actual, '    ')}
---
''')
        else:
            self.assertEqual(
                actual,
                expected,
                msg=f'''\
Prog: pyolin \'{prog}\' {_test_file(data)}
Expected:
    {expected!r}
---

Actual:
    {actual!r}
---
''')

    def assertRunPyolin(self, prog, expected, *, input='data_nba.txt', **kwargs):
        actual = run_pyolin(prog, input=input, **kwargs)
        self.assertEqual(actual, expected)

    def assertPyolin(self, prog, expected, *, input='data_nba.txt', extra_args=[], **kwargs):
        actual = run_cli(prog, input=input, extra_args=extra_args, **kwargs)
        if isinstance(expected, str):
            self._myassert(actual.getvalue(), expected, prog, input)
        else:
            self._myassert(actual.getbytes(), expected, prog, input)

    def testLines(self):
        self.assertPyolin(
            'line for line in lines',
            '''\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            ''')

    def testLine(self):
        self.assertPyolin(
            'line',
            '''\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            ''')

    def testFields(self):
        self.assertPyolin(
            'fields',
            '''\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            ''')

    def testAwkOutputFormat(self):
        self.assertPyolin(
            'fields',
            '''\
            Bucks Milwaukee 60 22 0.732
            Raptors Toronto 58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston 49 33 0.598
            Pacers Indiana 48 34 0.585
            ''',
            output_format='awk')

    def testAwkOutputFormatFieldSeparator(self):
        self.assertPyolin(
            'printer.field_separator = ","; fields',
            '''\
            Bucks,Milwaukee,60,22,0.732
            Raptors,Toronto,58,24,0.707
            76ers,Philadelphia,51,31,0.622
            Celtics,Boston,49,33,0.598
            Pacers,Indiana,48,34,0.585
            ''',
            output_format='awk')

    def testAwkOutputFormatRecordSeparator(self):
        self.assertPyolin(
            'printer.record_separator = ";\\n"; fields',
            '''\
            Bucks Milwaukee 60 22 0.732;
            Raptors Toronto 58 24 0.707;
            76ers Philadelphia 51 31 0.622;
            Celtics Boston 49 33 0.598;
            Pacers Indiana 48 34 0.585;
            ''',
            output_format='awk')

    def testReorderFields(self):
        self.assertPyolin(
            'fields[1], fields[0]',
            '''\
            | 0            | 1       |
            | ------------ | ------- |
            | Milwaukee    | Bucks   |
            | Toronto      | Raptors |
            | Philadelphia | 76ers   |
            | Boston       | Celtics |
            | Indiana      | Pacers  |
            ''')

    def testConditional(self):
        self.assertPyolin(
            'record for record in records if record[1] == "Boston"',
            '''\
            | 0       | 1      | 2  | 3  | 4     |
            | ------- | ------ | -- | -- | ----- |
            | Celtics | Boston | 49 | 33 | 0.598 |
            ''')

    def testNumberConversion(self):
        self.assertPyolin(
            'record.str for record in records if record[2] > 50',
            '''\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            ''')

    def testExpressionRecord(self):
        self.assertPyolin(
            'len(records)',
            '''\
            5
            ''')

    def testIfExpression(self):
        self.assertPyolin(
            'fields[0] if fields[3] > 30',
            '''\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            ''')

    def testTernaryExplicit(self):
        self.assertPyolin(
            'r[1] if len(r[1]) > 8 else "Name too short" for r in records',
            '''\
            | value          |
            | -------------- |
            | Milwaukee      |
            | Name too short |
            | Philadelphia   |
            | Name too short |
            | Name too short |
            ''')

    def testTernaryImplicit(self):
        self.assertPyolin(
            'fields[1] if fields[2] > 50 else "Score too low"',
            '''\
            | value         |
            | ------------- |
            | Milwaukee     |
            | Toronto       |
            | Philadelphia  |
            | Score too low |
            | Score too low |
            ''')

    def testCountCondition(self):
        self.assertPyolin(
            'len([r for r in records if r[2] > 50])',
            '''\
            3
            ''')

    def testEnumerate(self):
        self.assertPyolin(
            '(i, line) for i, line in enumerate(lines)',
            '''\
            | 0 | 1                              |
            | - | ------------------------------ |
            | 0 | Bucks Milwaukee    60 22 0.732 |
            | 1 | Raptors Toronto    58 24 0.707 |
            | 2 | 76ers Philadelphia 51 31 0.622 |
            | 3 | Celtics Boston     49 33 0.598 |
            | 4 | Pacers Indiana     48 34 0.585 |
            ''')

    def testSkipNone(self):
        self.assertPyolin(
            '[None, 1, 2, 3]',
            '''\
            | value |
            | ----- |
            | None  |
            | 1     |
            | 2     |
            | 3     |
            ''')

    def testSingletonNone(self):
        """
        Just a singleton None, not in a sequence, should be printed (maybe?)
        """
        self.assertPyolin(
            'None',
            '''\
            None
            ''')

    def testRegex(self):
        self.assertPyolin(
            r'fields if re.match(r"^\d.*", fields[0])',
            '''\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            ''')

    def testAddition(self):
        self.assertPyolin(
            'fields[2] + 100',
            '''\
            | value |
            | ----- |
            | 160   |
            | 158   |
            | 151   |
            | 149   |
            | 148   |
            ''')

    def testRadd(self):
        self.assertPyolin(
            '100 + fields[2]',
            '''\
            | value |
            | ----- |
            | 160   |
            | 158   |
            | 151   |
            | 149   |
            | 148   |
            ''')

    def testFieldAddition(self):
        self.assertPyolin(
            'fields[2] + fields[3]',
            '''\
            | value |
            | ----- |
            | 82    |
            | 82    |
            | 82    |
            | 82    |
            | 82    |
            ''')

    def testFieldConcat(self):
        self.assertPyolin(
            'fields[2] + fields[0]',
            '''\
            | value     |
            | --------- |
            | 60Bucks   |
            | 58Raptors |
            | 5176ers   |
            | 49Celtics |
            | 48Pacers  |
            ''')

    def testFieldConcatReversed(self):
        self.assertPyolin(
            'fields[0] + fields[2]',
            '''\
            | value     |
            | --------- |
            | Bucks60   |
            | Raptors58 |
            | 76ers51   |
            | Celtics49 |
            | Pacers48  |
            ''')

    def testStringConcat(self):
        self.assertPyolin(
            'fields[0] + "++"',
            '''\
            | value     |
            | --------- |
            | Bucks++   |
            | Raptors++ |
            | 76ers++   |
            | Celtics++ |
            | Pacers++  |
            ''')

    def testLt(self):
        self.assertPyolin(
            'fields[0] if fields[2] < 51',
            '''\
            | value   |
            | ------- |
            | Celtics |
            | Pacers  |
            ''')

    def testLe(self):
        self.assertPyolin(
            'fields[0] if fields[2] <= 51',
            '''\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            ''')

    def testSubtraction(self):
        self.assertPyolin(
            'fields[2] - 50',
            '''\
            | value |
            | ----- |
            | 10    |
            | 8     |
            | 1     |
            | -1    |
            | -2    |
            ''')

    def testRsub(self):
        self.assertPyolin(
            '50 - fields[2]',
            '''\
            | value |
            | ----- |
            | -10   |
            | -8    |
            | -1    |
            | 1     |
            | 2     |
            ''')

    def testLeftShift(self):
        self.assertPyolin(
            'fields[2] << 2',
            '''\
            | value |
            | ----- |
            | 240   |
            | 232   |
            | 204   |
            | 196   |
            | 192   |
            ''')

    def testNeg(self):
        self.assertPyolin(
            '(-fields[2])',
            '''\
            | value |
            | ----- |
            | -60   |
            | -58   |
            | -51   |
            | -49   |
            | -48   |
            ''')

    def testRound(self):
        self.assertPyolin(
            'round(fields[2], -2)',
            '''\
            | value |
            | ----- |
            | 100   |
            | 100   |
            | 100   |
            | 0     |
            | 0     |
            ''')

    def testSkipFirstLine(self):
        self.assertPyolin(
            'l for l in lines[1:]',
            '''\
            | value                          |
            | ------------------------------ |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            | Celtics Boston     49 33 0.598 |
            | Pacers Indiana     48 34 0.585 |
            ''')

    def testAnd(self):
        self.assertPyolin(
            'record if fields[2] > 50 and fields[3] > 30',
            '''\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            ''')

    def testAddHeader(self):
        self.assertPyolin(
            'header = ("Team", "City", "Win", "Loss", "Winrate"); records',
            '''\
            | Team    | City         | Win | Loss | Winrate |
            | ------- | ------------ | --- | ---- | ------- |
            | Bucks   | Milwaukee    | 60  | 22   | 0.732   |
            | Raptors | Toronto      | 58  | 24   | 0.707   |
            | 76ers   | Philadelphia | 51  | 31   | 0.622   |
            | Celtics | Boston       | 49  | 33   | 0.598   |
            | Pacers  | Indiana      | 48  | 34   | 0.585   |
            ''')

    def testCountDots(self):
        self.assertPyolin(
            'sum(line.count("0") for line in lines)',
            '''\
            7
            ''')

    def testMaxScore(self):
        self.assertPyolin(
            'max(r[2] for r in records)',
            '''\
            60
            ''')

    def testContents(self):
        self.assertPyolin(
            'len(contents)',
            '''\
            154
            ''')

    def testEmptyList(self):
        self.assertPyolin(
            '[]',
            '''\
            ''')

    def testStreamingStdin(self):
        with pyolinPopen('parser.has_header = False; line', extra_args=['--output_format=awk']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Raptors Toronto    58 24 0.707\n')
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Celtics Boston     49 33 0.598\n')

    def testClosedStdout(self):
        with pyolinPopen('parser.has_header = False; line', extra_args=['--output_format=awk']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(proc.stdout.readline(), 'Raptors Toronto    58 24 0.707\n')
            # Command line tools like `head` will close the pipe when it is done getting the data
            # it needs. Make sure this doesn't crash
            proc.stdout.close()
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.close()
            # proc.stdin.flush()
            errmsg = proc.stderr.read()
            self.assertEqual(errmsg, '', errmsg)

    def testStreamingStdinBinary(self):
        with pyolinPopen(
                'file[:2]',
                extra_args=['--input_format=binary', '--output_format=binary'],
                universal_newlines=False) as proc:
            stdout, _ = proc.communicate(b'\x30\x62\x43\x00')  # type: ignore
            self.assertEqual(stdout, b'\x30\x62')

    def testStreamingSlice(self):
        with pyolinPopen(
                'parser.has_header = False; records[:2]', extra_args=['--output_format=awk']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Raptors Toronto 58 24 0.707\n')
                self.assertEqual(
                    proc.stdout.readline(),
                    'Celtics Boston 49 33 0.598\n')
            proc.stdin.write('Write more stuff...\n')

    def testStreamingIndex(self):
        with pyolinPopen(
                'parser.has_header = False; records[1].str',
                extra_args=['--output_format=awk']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Celtics Boston     49 33 0.598\n')
            proc.stdin.write('Write more stuff...\n')

    def testRecordsIndex(self):
        self.assertPyolin(
            'records[1]',
            '''\
            Raptors Toronto 58 24 0.707
            ''')

    def testDestructuring(self):
        self.assertPyolin(
            'city for team, city, _, _, _ in records',
            '''\
            | value        |
            | ------------ |
            | Milwaukee    |
            | Toronto      |
            | Philadelphia |
            | Boston       |
            | Indiana      |
            ''')

    def testPercentage(self):
        self.assertPyolin(
            '(r[0], round(r[3] / sum(r[3] for r in records), 2)) '
            'for r in records',
            '''\
            | 0       | 1    |
            | ------- | ---- |
            | Bucks   | 0.15 |
            | Raptors | 0.17 |
            | 76ers   | 0.22 |
            | Celtics | 0.23 |
            | Pacers  | 0.24 |
            ''')

    def testSingletonTuple(self):
        """
        Tuples are treated as fields in a single record, whereas other iterable
        types are treated as multiple records.
        """
        self.assertPyolin(
            'sum(r[3] for r in records), max(r[3] for r in records)',
            '''\
            144 34
            ''')

    def testModuleImport(self):
        self.assertPyolin(
            'record[0] if fnmatch.fnmatch(record[0], "*.txt")',
            '''\
            | value                  |
            | ---------------------- |
            | dir/file.txt           |
            | dir/file1.txt          |
            | dir/fileb.txt          |
            | dir/subdir/subfile.txt |
            ''',
            input='data_files.txt')

    def testRecordVariables(self):
        self.assertPyolin(
            'type(record).__name__, type(line).__name__, '
            'type(fields).__name__',
            '''\
            | 0      | 1   | 2      |
            | ------ | --- | ------ |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            | Record | str | Record |
            ''')

    def testFileVariables(self):
        self.assertPyolin(
            'type(lines).__name__, type(records).__name__, '
            'type(file).__name__, type(contents).__name__',
            '''\
            StreamingSequence RecordSequence str str
            ''')

    def testBoolean(self):
        self.assertPyolin(
            'record if record[1].bool',
            '''\
            | 0          | 1    | 2  | 3    |
            | ---------- | ---- | -- | ---- |
            | dir        | True | 30 | 40.0 |
            | dir/subdir | True | 12 | 42.0 |
            ''',
            input='data_files.txt')

    def testAwkHeaderDetection(self):
        self.assertPyolin(
            'record if record[1].bool',
            '''\
            | Path       | IsDir | Size | Score |
            | ---------- | ----- | ---- | ----- |
            | dir        | True  | 30   | 40.0  |
            | dir/subdir | True  | 12   | 42.0  |
            ''',
            input='data_files_with_header.txt')

    def testFilename(self):
        self.assertPyolin(
            'filename',
            f'''\
            {os.path.dirname(__file__)}/data_files.txt
            ''',
            input='data_files.txt')

    def testBytes(self):
        self.assertPyolin(
            'b"hello"',
            '''\
            hello
            ''')

    def testReversed(self):
        self.assertPyolin(
            'reversed(lines)',
            '''\
            | value                          |
            | ------------------------------ |
            | Pacers Indiana     48 34 0.585 |
            | Celtics Boston     49 33 0.598 |
            | 76ers Philadelphia 51 31 0.622 |
            | Raptors Toronto    58 24 0.707 |
            | Bucks Milwaukee    60 22 0.732 |
            ''')

    def testInOperator(self):
        self.assertPyolin(
            '"Raptors Toronto    58 24 0.707" in lines',
            '''\
            True
            ''')

    def testBase64(self):
        self.assertPyolin(
            'base64.b64encode(fields[0].bytes)',
            '''\
            | value        |
            | ------------ |
            | QnVja3M=     |
            | UmFwdG9ycw== |
            | NzZlcnM=     |
            | Q2VsdGljcw== |
            | UGFjZXJz     |
            ''')

    def testUrlQuote(self):
        self.assertPyolin(
            'urllib.parse.quote(line)',
            '''\
            | value                                          |
            | ---------------------------------------------- |
            | Bucks%20Milwaukee%20%20%20%2060%2022%200.732   |
            | Raptors%20Toronto%20%20%20%2058%2024%200.707   |
            | 76ers%20Philadelphia%2051%2031%200.622         |
            | Celtics%20Boston%20%20%20%20%2049%2033%200.598 |
            | Pacers%20Indiana%20%20%20%20%2048%2034%200.585 |
            ''')

    def testFieldsEqual(self):
        self.assertPyolin(
            'fields[2], fields[3], fields[2] == fields[3]',
            '''\
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
            ''',
            input='data_files.txt')

    def testFieldsComparison(self):
        self.assertPyolin(
            'fields[2], fields[3], fields[2] >= fields[3]',
            '''\
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
            ''',
            input='data_files.txt')

    def testMultiplication(self):
        self.assertPyolin(
            'fields[3] * 10',
            '''\
            | value |
            | ----- |
            | 220   |
            | 240   |
            | 310   |
            | 330   |
            | 340   |
            ''')

    def testFieldsMultiplication(self):
        self.assertPyolin(
            'fields[3] * fields[2]',
            '''\
            | value |
            | ----- |
            | 1320  |
            | 1392  |
            | 1581  |
            | 1617  |
            | 1632  |
            ''')

    def testStringMultiplication(self):
        self.assertPyolin(
            'fields[0] * 2',
            '''\
            | value          |
            | -------------- |
            | BucksBucks     |
            | RaptorsRaptors |
            | 76ers76ers     |
            | CelticsCeltics |
            | PacersPacers   |
            ''')

    def testPandasDataframe(self):
        self.assertPyolin(
            'df',
            '''\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            ''')

    def testPandasDtypes(self):
        self.assertPyolin(
            'df.dtypes',
            '''\
            | value   |
            | ------- |
            | object  |
            | object  |
            | int64   |
            | int64   |
            | float64 |
            ''')

    def testPandaNumericOperations(self):
        self.assertPyolin(
            'df[2] * 2',
            '''\
            | value |
            | ----- |
            | 120   |
            | 116   |
            | 102   |
            | 98    |
            | 96    |
            ''')

    def testNumpyNumericOperations(self):
        self.assertPyolin(
            'np.power(df[2], 2)',
            '''\
            | value |
            | ----- |
            | 3600  |
            | 3364  |
            | 2601  |
            | 2401  |
            | 2304  |
            ''')

    def testFieldSeparator(self):
        self.assertPyolin(
            'record',
            '''\
            | 0         | 1          | 2           | 3    | 4    | 5     | 6    | 7    | 8  |
            | --------- | ---------- | ----------- | ---- | ---- | ----- | ---- | ---- | -- |
            | Alfalfa   | Aloysius   | 123-45-6789 | 40.0 | 90.0 | 100.0 | 83.0 | 49.0 | D- |
            | Alfred    | University | 123-12-1234 | 41.0 | 97.0 | 96.0  | 97.0 | 48.0 | D+ |
            | Gerty     | Gramma     | 567-89-0123 | 41.0 | 80.0 | 60.0  | 40.0 | 44.0 | C  |
            | Android   | Electric   | 087-65-4321 | 42.0 | 23.0 | 36.0  | 45.0 | 47.0 | B- |
            | Franklin  | Benny      | 234-56-2890 | 50.0 | 1.0  | 90.0  | 80.0 | 90.0 | B- |
            | George    | Boy        | 345-67-3901 | 40.0 | 1.0  | 11.0  | -1.0 | 4.0  | B  |
            | Heffalump | Harvey     | 632-79-9439 | 30.0 | 1.0  | 20.0  | 30.0 | 40.0 | C  |
            ''',
            input='data_grades_simple_csv.csv',
            field_separator=r',')

    def testFieldSeparatorRegex(self):
        self.assertPyolin(
            'record',
            '''\
    | 0         | 1          | 2           | 3  | 4 | 5  | 6 | 7   | 8 | 9  | 10 | 11 | 12 | 13 |
    | --------- | ---------- | ----------- | -- | - | -- | - | --- | - | -- | -- | -- | -- | -- |
    | Alfalfa   | Aloysius   | 123-45-6789 | 40 | 0 | 90 | 0 | 100 | 0 | 83 | 0  | 49 | 0  | D- |
    | Alfred    | University | 123-12-1234 | 41 | 0 | 97 | 0 | 96  | 0 | 97 | 0  | 48 | 0  | D+ |
    | Gerty     | Gramma     | 567-89-0123 | 41 | 0 | 80 | 0 | 60  | 0 | 40 | 0  | 44 | 0  | C  |
    | Android   | Electric   | 087-65-4321 | 42 | 0 | 23 | 0 | 36  | 0 | 45 | 0  | 47 | 0  | B- |
    | Franklin  | Benny      | 234-56-2890 | 50 | 0 | 1  | 0 | 90  | 0 | 80 | 0  | 90 | 0  | B- |
    | George    | Boy        | 345-67-3901 | 40 | 0 | 1  | 0 | 11  | 0 | -1 | 0  | 4  | 0  | B  |
    | Heffalump | Harvey     | 632-79-9439 | 30 | 0 | 1  | 0 | 20  | 0 | 30 | 0  | 40 | 0  | C  |
    ''',
            input='data_grades_simple_csv.csv',
            field_separator=r'[\.,]')

    def testRecordSeparator(self):
        self.assertPyolin(
            'record',
            '''\
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
            ''',
            input='data_onerow.csv',
            record_separator=r',')

    def testRecordSeparatorMultipleChars(self):
        self.assertPyolin(
            'parser.has_header=False; record',
            '''\
            | value                                    |
            | ---------------------------------------- |
            | JET                                      |
            | 0031201                                  |
            | 0001006,53521,1.000E+01,NBIC,HSELM,TRANS |
            | .000E+00,1.000E+00                       |
            | ,1,0,0                                   |
            ''',
            input='data_onerow.csv',
            record_separator=r',2')

    def testRecordSeparatorRegex(self):
        self.assertPyolin(
            'record',
            '''\
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
            ''',
            input='data_onerow.csv',
            record_separator=r'[,.]')

    def testSimpleCsv(self):
        self.assertPyolin(
            'df[[0, 1, 2]]',
            '''\
            | 0         | 1          | 2           |
            | --------- | ---------- | ----------- |
            | Alfalfa   | Aloysius   | 123-45-6789 |
            | Alfred    | University | 123-12-1234 |
            | Gerty     | Gramma     | 567-89-0123 |
            | Android   | Electric   | 087-65-4321 |
            | Franklin  | Benny      | 234-56-2890 |
            | George    | Boy        | 345-67-3901 |
            | Heffalump | Harvey     | 632-79-9439 |
            ''',
            input='data_grades_simple_csv.csv',
            input_format='csv')

    def testQuotedCsv(self):
        self.assertPyolin(
            'record[0]',
            '''\
            | value            |
            | ---------------- |
            | 60 Minutes       |
            | 48 Hours Mystery |
            | 20/20            |
            | Nightline        |
            | Dateline Friday  |
            | Dateline Sunday  |
            ''',
            input='data_news_decline.csv',
            input_format='csv')

    def testQuotedCsvStr(self):
        self.assertPyolin(
            'record.str',
            '''\
            | value                             |
            | --------------------------------- |
            | "60 Minutes",       7.6, 7.4, 7.3 |
            | "48 Hours Mystery", 4.1, 3.9, 3.6 |
            | "20/20",            4.1, 3.7, 3.3 |
            | "Nightline",        2.7, 2.6, 2.7 |
            | "Dateline Friday",  4.1, 4.1, 3.9 |
            | "Dateline Sunday",  3.5, 3.2, 3.1 |
            ''',
            input='data_news_decline.csv',
            input_format='csv')

    def testAutoCsv(self):
        self.assertPyolin(
            'df[[0,1,2]]',
            '''\
            | 0                     | 1        | 2                                |
            | --------------------- | -------- | -------------------------------- |
            | John                  | Doe      | 120 jefferson st.                |
            | Jack                  | McGinnis | 220 hobo Av.                     |
            | John "Da Man"         | Repici   | 120 Jefferson St.                |
            | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
            |                       | Blankman |                                  |
            | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
            ''',
            input='data_addresses.csv',
            input_format='csv')

    def testCsvExcel(self):
        self.assertPyolin(
            'df[[0,1,2]]',
            '''\
            | 0                     | 1        | 2                                |
            | --------------------- | -------- | -------------------------------- |
            | John                  | Doe      | 120 jefferson st.                |
            | Jack                  | McGinnis | 220 hobo Av.                     |
            | John "Da Man"         | Repici   | 120 Jefferson St.                |
            | Stephen               | Tyler    | 7452 Terrace "At the Plaza" road |
            |                       | Blankman |                                  |
            | Joan "the bone", Anne | Jet      | 9th, at Terrace plc              |
            ''',
            input='data_addresses.csv',
            input_format='csv_excel')

    def testCsvUnix(self):
        self.assertPyolin(
            '"|".join((record[0], record[1], record[2]))',
            '''\
            | value                                          |
            | ---------------------------------------------- |
            | John|Doe|120 jefferson st.                     |
            | Jack|McGinnis|220 hobo Av.                     |
            | John "Da Man"|Repici|120 Jefferson St.         |
            | Stephen|Tyler|7452 Terrace "At the Plaza" road |
            | |Blankman|                                     |
            | Joan "the bone", Anne|Jet|9th, at Terrace plc  |
            ''',
            input='data_addresses_unix.csv',
            input_format='csv')

    def testQuotedTsv(self):
        self.assertPyolin(
            'record[0], record[2]',
            '''\
            | 0                | 1   |
            | ---------------- | --- |
            | 60 Minutes       | 7.4 |
            | 48 Hours Mystery | 3.9 |
            | 20/20            | 3.7 |
            | Nightline        | 2.6 |
            | Dateline Friday  | 4.1 |
            | Dateline Sunday  | 3.2 |
            ''',
            input='data_news_decline.tsv',
            input_format='csv',
            field_separator='\t')

    def testStatement(self):
        self.assertPyolin(
            'a = record[2]; b = 1; a + b',
            '''\
            | value |
            | ----- |
            | 61    |
            | 59    |
            | 52    |
            | 50    |
            | 49    |
            ''')

    def testStatementTable(self):
        self.assertPyolin(
            'a = len(records); b = 2; a * b',
            '''\
            10
            ''')

    def testSyntaxError(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('a..x')
        self.assertEqual(
            textwrap.dedent('''\
                Invalid syntax:
                  a..x
                    ^'''),
            str(context.exception.__cause__))

    def testSyntaxErrorInStatement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('a..x; a+1')
        self.assertEqual(
            textwrap.dedent('''\
                Invalid syntax:
                  a..x
                    ^'''),
            str(context.exception.__cause__))

    def testHeaderDetection(self):
        self.assertPyolin(
            'df[["Last name", "SSN", "Final"]]',
            '''\
            | Last name | SSN         | Final |
            | --------- | ----------- | ----- |
            | Alfalfa   | 123-45-6789 | 49    |
            | Alfred    | 123-12-1234 | 48    |
            | Gerty     | 567-89-0123 | 44    |
            | Android   | 087-65-4321 | 47    |
            | Franklin  | 234-56-2890 | 90    |
            | George    | 345-67-3901 | 4     |
            | Heffalump | 632-79-9439 | 40    |
            ''',
            input='data_grades_with_header.csv',
            input_format='csv')

    def testForceHasHeader(self):
        self.assertPyolin(
            'parser.has_header = True; (r[0], r[2], r[7]) for r in records',
            '''\
            | Alfalfa   | 123-45-6789 | 49.0 |
            | --------- | ----------- | ---- |
            | Alfred    | 123-12-1234 | 48.0 |
            | Gerty     | 567-89-0123 | 44.0 |
            | Android   | 087-65-4321 | 47.0 |
            | Franklin  | 234-56-2890 | 90.0 |
            | George    | 345-67-3901 | 4.0  |
            | Heffalump | 632-79-9439 | 40.0 |
            ''',
            input='data_grades_simple_csv.csv',
            input_format='csv')

    def testHeaderDetectionCsvExcel(self):
        self.assertPyolin(
            'df[["Last Name", "Address"]]',
            '''\
            | Last Name             | Address                          |
            | --------------------- | -------------------------------- |
            | John                  | 120 jefferson st.                |
            | Jack                  | 220 hobo Av.                     |
            | John "Da Man"         | 120 Jefferson St.                |
            | Stephen               | 7452 Terrace "At the Plaza" road |
            | Joan "the bone", Anne | 9th, at Terrace plc              |
            ''',
            input='data_addresses_with_header.csv',
            input_format='csv_excel')

    def testPrintDataframeHeader(self):
        self.assertPyolin(
            'list(df.columns.values)',
            '''\
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
            ''',
            input='data_grades_with_header.csv',
            input_format='csv')

    def testAssignToRecord(self):
        '''
        Try to confuse the parser by writing to a field called record
        '''
        self.assertPyolin(
            'record=1; record+1',
            '''\
            2
            ''')

    def testAccessRecordAndTable(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('a = record[0]; b = records; b')
        self.assertEqual(
            'Cannot access both record scoped and table scoped variables',
            str(context.exception.__cause__.__cause__))

    def testAccessTableAndRecord(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('a = records; b = record[0]; b')
        self.assertEqual(
            'Cannot access both record scoped and table scoped variables',
            str(context.exception.__cause__.__cause__))

    def testEmptyRecordScoped(self):
        self.assertPyolin(
            'record[0]',
            '',
            input=os.devnull)

    def testEmptyTableScoped(self):
        self.assertPyolin(
            'record for record in records',
            '',
            input=os.devnull)

    def testSemicolonInString(self):
        self.assertPyolin(
            '"hello; world"',
            'hello; world\n')

    def testStackTraceCleaning(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('urllib.parse.quote(12345)')
        assert isinstance(context.exception.__cause__, UserError)
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(5, len(formatted), pformat(formatted))
        self.assertIn('Traceback (most recent call last)', formatted[0])
        self.assertIn('pyolin_user_prog.py', formatted[1])
        self.assertIn('return quote_from_bytes', formatted[2])
        self.assertIn('"quote_from_bytes() expected bytes"', formatted[3])
        self.assertIn('quote_from_bytes() expected bytes', formatted[4])

    def testInvalidOutputFormat(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('1+1', output_format='invalid')
        self.assertEqual(
            'Unrecognized output format "invalid"',
            str(context.exception.__cause__))

    def testCsvOutputFormat(self):
        self.assertPyolin(
            'records',
            '''\
            Bucks,Milwaukee,60,22,0.732\r
            Raptors,Toronto,58,24,0.707\r
            76ers,Philadelphia,51,31,0.622\r
            Celtics,Boston,49,33,0.598\r
            Pacers,Indiana,48,34,0.585\r
            ''',
            output_format='csv')

    def testCsvOutputFormatUnix(self):
        self.assertPyolin(
            'printer.dialect = csv.unix_dialect; records',
            '''\
            "Bucks","Milwaukee","60","22","0.732"
            "Raptors","Toronto","58","24","0.707"
            "76ers","Philadelphia","51","31","0.622"
            "Celtics","Boston","49","33","0.598"
            "Pacers","Indiana","48","34","0.585"
            ''',
            output_format='csv')

    def testCsvOutputInvalidDialect(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('printer.dialect = "invalid"; records', output_format='csv')
        self.assertEqual(
            'Unknown dialect "invalid"',
            str(context.exception.__cause__))

    def testCsvOutputFormatDelimiter(self):
        self.assertPyolin(
            'printer.delimiter = "^"; records',
            '''\
            Bucks^Milwaukee^60^22^0.732\r
            Raptors^Toronto^58^24^0.707\r
            76ers^Philadelphia^51^31^0.622\r
            Celtics^Boston^49^33^0.598\r
            Pacers^Indiana^48^34^0.585\r
            ''',
            output_format='csv')

    def testCsvOutputNonTuple(self):
        self.assertPyolin(
            'record[2]',
            '''\
            60\r
            58\r
            51\r
            49\r
            48\r
            ''',
            output_format='csv')

    def testCsvOutputQuoting(self):
        self.assertPyolin(
            'records',
            '''\
            John,Doe,120 jefferson st.,Riverside, NJ, 08075\r
            Jack,McGinnis,220 hobo Av.,Phila, PA,09119\r
            "John ""Da Man""",Repici,120 Jefferson St.,Riverside, NJ,08075\r
            Stephen,Tyler,"7452 Terrace ""At the Plaza"" road",SomeTown,SD, 91234\r
            ,Blankman,,SomeTown, SD, 00298\r
            "Joan ""the bone"", Anne",Jet,"9th, at Terrace plc",Desert City,CO,00123\r
            ''',
            input_format='csv',
            output_format='csv',
            input='data_addresses.csv')

    def testCsvOutputWithHeader(self):
        self.assertPyolin(
            'printer.print_header = True; df[["Last name", "SSN", "Final"]]',
            '''\
            Last name,SSN,Final\r
            Alfalfa,123-45-6789,49\r
            Alfred,123-12-1234,48\r
            Gerty,567-89-0123,44\r
            Android,087-65-4321,47\r
            Franklin,234-56-2890,90\r
            George,345-67-3901,4\r
            Heffalump,632-79-9439,40\r
            ''',
            input='data_grades_with_header.csv',
            input_format='csv',
            output_format='csv')

    def testCsvOutputWithHeaderFunction(self):
        def func():
            # pylint: disable=undefined-variable
            printer.print_header = True  # type: ignore
            return df[["Last name", "SSN", "Final"]]  # type: ignore
            # pylint: enable=undefined-variable

        self.assertPyolin(
            func,
            '''\
            Last name,SSN,Final\r
            Alfalfa,123-45-6789,49\r
            Alfred,123-12-1234,48\r
            Gerty,567-89-0123,44\r
            Android,087-65-4321,47\r
            Franklin,234-56-2890,90\r
            George,345-67-3901,4\r
            Heffalump,632-79-9439,40\r
            ''',
            input='data_grades_with_header.csv',
            input_format='csv',
            output_format='csv')

    def testStreamingStdinCsv(self):
        with pyolinPopen('parser.has_header = False; record', ['--output_format', 'csv']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Raptors,Toronto,58,24,0.707\n')
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Celtics,Boston,49,33,0.598\n')

    def testNumericHeader(self):
        self.assertPyolin(
            'printer.print_header = True; record[0],record[2],record[7]',
            '''\
            0,1,2\r
            Alfalfa,123-45-6789,49.0\r
            Alfred,123-12-1234,48.0\r
            Gerty,567-89-0123,44.0\r
            Android,087-65-4321,47.0\r
            Franklin,234-56-2890,90.0\r
            George,345-67-3901,4.0\r
            Heffalump,632-79-9439,40.0\r
            ''',
            input='data_grades_simple_csv.csv',
            input_format='csv',
            output_format='csv')

    def testMarkdownOutput(self):
        self.assertPyolin(
            'df[["Last name", "SSN", "Final"]]',
            '''\
            | Last name | SSN         | Final |
            | --------- | ----------- | ----- |
            | Alfalfa   | 123-45-6789 | 49    |
            | Alfred    | 123-12-1234 | 48    |
            | Gerty     | 567-89-0123 | 44    |
            | Android   | 087-65-4321 | 47    |
            | Franklin  | 234-56-2890 | 90    |
            | George    | 345-67-3901 | 4     |
            | Heffalump | 632-79-9439 | 40    |
            ''',
            input='data_grades_with_header.csv',
            input_format='csv',
            output_format='markdown')

    def testTsvOutput(self):
        self.assertPyolin(
            'records',
            '''\
            Bucks	Milwaukee	60	22	0.732\r
            Raptors	Toronto	58	24	0.707\r
            76ers	Philadelphia	51	31	0.622\r
            Celtics	Boston	49	33	0.598\r
            Pacers	Indiana	48	34	0.585\r
            ''',
            output_format='tsv')

    def testMultilineInput(self):
        self.assertPyolin(
            textwrap.dedent('''\
            record = 1
            record + 1\
            '''),
            '''\
            2
            ''')

    def testMultilineMixedInput(self):
        self.assertPyolin(
            textwrap.dedent('''\
            record = 1; record += 1
            record += 1; record + 1
            '''),
            '''\
            4
            ''')

    def testLastStatement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('1+1;pass')
        self.assertEqual(
            textwrap.dedent('''\
            Cannot evaluate value from statement:
              pass'''),
            str(context.exception.__cause__))

    def testMultilinePythonProgram(self):
        self.assertPyolin(
            textwrap.dedent('''\
            result = []
            for i in range(5):
                result.append(range(i + 1))
            result
            '''),
            '''\
            0
            0 1
            0 1 2
            0 1 2 3
            0 1 2 3 4
            ''',
            output_format='awk')

    def testMarkdownWrapping(self):
        with mock.patch.dict(os.environ, {'PYOLIN_TABLE_WIDTH': '80'}):
            self.assertPyolin(
                'df[["marketplace", "review_body", "star_rating"]]',
                '''\
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
                ''',
                input='data_amazon_reviews.tsv',
                input_format='tsv',
                output_format='markdown')

    def testMarkdownWrapping2(self):
        with mock.patch.dict(os.environ, {'PYOLIN_TABLE_WIDTH': '80'}):
            self.assertPyolin(
                'records',
                '''\
                | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11    | 12           | 13  | 14 |
                | - | - | - | - | - | - | - | - | - | - | -- | ----- | ------------ | --- | -- |
                | a | b | c | d | e | f | g | h | i | j | k  | lmnop | qrstuv123456 | wxy | z  |
                :   :   :   :   :   :   :   :   :   :   :    :       : 789123456789 :     :    :
                :   :   :   :   :   :   :   :   :   :   :    :       : 123456789    :     :    :
                ''',
                input='data_formatting.txt',
                output_format='markdown')

    def testJsonOutput(self):
        self.assertPyolin(
            'records',
            '''\
            [
            {"0": "Bucks", "1": "Milwaukee", "2": 60, "3": 22, "4": 0.732},
            {"0": "Raptors", "1": "Toronto", "2": 58, "3": 24, "4": 0.707},
            {"0": "76ers", "1": "Philadelphia", "2": 51, "3": 31, "4": 0.622},
            {"0": "Celtics", "1": "Boston", "2": 49, "3": 33, "4": 0.598},
            {"0": "Pacers", "1": "Indiana", "2": 48, "3": 34, "4": 0.585}
            ]
            ''',
            output_format='json')

    def testJsonInput(self):
        self.assertPyolin(
            'df',
            '''\
            | color   | value |
            | ------- | ----- |
            | red     | #f00  |
            | green   | #0f0  |
            | blue    | #00f  |
            | cyan    | #0ff  |
            | magenta | #f0f  |
            | yellow  | #ff0  |
            | black   | #000  |
            ''',
            input='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testContains(self):
        self.assertPyolin(
            '("green", "#0f0") in records',
            '''\
            True
            ''',
            input='data_colors.json',
            input_format='json')

    def testSingleValueOutput(self):
        self.assertPyolin(
            'len(records)',
            '''\
            | value |
            | ----- |
            | 7     |
            ''',
            input='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testRecordOutput(self):
        self.assertPyolin(
            'records[0]',
            '''\
            | color | value |
            | ----- | ----- |
            | red   | #f00  |
            ''',
            input='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testRecordsWithHeader(self):
        self.assertPyolin(
            'records',
            '''\
            | color   | value |
            | ------- | ----- |
            | red     | #f00  |
            | green   | #0f0  |
            | blue    | #00f  |
            | cyan    | #0ff  |
            | magenta | #f0f  |
            | yellow  | #ff0  |
            | black   | #000  |
            ''',
            input='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testLinesWithHeader(self):
        self.assertPyolin(
            'lines',
            '''\
            | value                                 |
            | ------------------------------------- |
            | {"color": "red", "value": "#f00"}     |
            | {"color": "green", "value": "#0f0"}   |
            | {"color": "blue", "value": "#00f"}    |
            | {"color": "cyan", "value": "#0ff"}    |
            | {"color": "magenta", "value": "#f0f"} |
            | {"color": "yellow", "value": "#ff0"}  |
            | {"color": "black", "value": "#000"}   |
            ''',
            input='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testMarkdownNonUniformColumnCount(self):
        self.assertPyolin(
            'range(i) for i in range(1, 5)',
            '''\
            | value |
            | ----- |
            | 0     |
            | 0     | 1 |
            | 0     | 1 | 2 |
            | 0     | 1 | 2 | 3 |
            ''',
            output_format='markdown')

    def testReprPrinter(self):
        self.assertPyolin(
            'range(10)',
            '''\
            range(0, 10)
            ''',
            output_format='repr')

    def testReprPrinterTable(self):
        self.assertPyolin(
            'records',
            '''\
            [('Bucks', 'Milwaukee', '60', '22', '0.732'), ('Raptors', 'Toronto', '58', '24', '0.707'), ('76ers', 'Philadelphia', '51', '31', '0.622'), ('Celtics', 'Boston', '49', '33', '0.598'), ('Pacers', 'Indiana', '48', '34', '0.585')]
            ''',
            output_format='repr')

    def testReprPrinterRecords(self):
        self.assertPyolin(
            '"aloha\u2011\u2011\u2011"',
            '''\
            'aloha\u2011\u2011\u2011'
            ''',
            output_format='repr')

    def testStrPrinterRecords(self):
        self.assertPyolin(
            '"aloha\u2011\u2011\u2011"',
            '''\
            aloha\u2011\u2011\u2011
            ''',
            output_format='str')

    def testStrPrinterTable(self):
        self.assertPyolin(
            'records',
            '''\
            [('Bucks', 'Milwaukee', '60', '22', '0.732'), ('Raptors', 'Toronto', '58', '24', '0.707'), ('76ers', 'Philadelphia', '51', '31', '0.622'), ('Celtics', 'Boston', '49', '33', '0.598'), ('Pacers', 'Indiana', '48', '34', '0.585')]
            ''',
            output_format='str')

    def testSetPrinter(self):
        self.assertPyolin(
            'printer = new_printer("repr"); range(10)',
            '''\
            range(0, 10)
            ''')

    def testPrinterNone(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('printer = None; 123')
        self.assertEqual(
            'printer must be an instance of Printer. Found "None" instead',
            str(context.exception.__cause__))

    def testRaiseStopIteration(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('raise StopIteration(); None')
        assert isinstance(context.exception.__cause__, UserError)
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(3, len(formatted), pformat(formatted))
        self.assertIn('Traceback (most recent call last)', formatted[0])
        self.assertIn('pyolin_user_prog.py', formatted[1])
        self.assertIn('StopIteration', formatted[2])

    def testBinaryInputLen(self):
        self.assertPyolin(
            'len(file)',
            '''\
            21
            ''',
            input_format='binary',
            input='data_pickle')

    def testBinaryInputPickle(self):
        self.assertPyolin(
            'pickle.loads(file)',
            '''\
            hello world
            ''',
            input_format='binary',
            input='data_pickle')

    def testBinaryPrinter(self):
        self.assertPyolin(
            'b"\\x30\\x62\\x43\\x00"',
            b'\x30\x62\x43\x00',
            input_format='binary',
            output_format='binary')

    def testBinaryInputAccessRecords(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('records', input_format='binary', input='data_pickle')
        self.assertEqual(
            'Record based attributes are not supported in binary input mode',
            str(context.exception.__cause__))

    def testSetFieldSeparator(self):
        self.assertPyolin(
            'parser.field_separator = ","; record',
            '''\
            | 0         | 1          | 2           | 3    | 4    | 5     | 6    | 7    | 8  |
            | --------- | ---------- | ----------- | ---- | ---- | ----- | ---- | ---- | -- |
            | Alfalfa   | Aloysius   | 123-45-6789 | 40.0 | 90.0 | 100.0 | 83.0 | 49.0 | D- |
            | Alfred    | University | 123-12-1234 | 41.0 | 97.0 | 96.0  | 97.0 | 48.0 | D+ |
            | Gerty     | Gramma     | 567-89-0123 | 41.0 | 80.0 | 60.0  | 40.0 | 44.0 | C  |
            | Android   | Electric   | 087-65-4321 | 42.0 | 23.0 | 36.0  | 45.0 | 47.0 | B- |
            | Franklin  | Benny      | 234-56-2890 | 50.0 | 1.0  | 90.0  | 80.0 | 90.0 | B- |
            | George    | Boy        | 345-67-3901 | 40.0 | 1.0  | 11.0  | -1.0 | 4.0  | B  |
            | Heffalump | Harvey     | 632-79-9439 | 30.0 | 1.0  | 20.0  | 30.0 | 40.0 | C  |
            ''',
            input='data_grades_simple_csv.csv')

    def testSetRecordSeparator(self):
        self.assertPyolin(
            'parser.record_separator = ","; record',
            '''\
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
            ''',
            input='data_onerow.csv')

    def testSetParser(self):
        self.assertPyolin(
            'parser = new_parser("json"); df',
            '''\
            | color   | value |
            | ------- | ----- |
            | red     | #f00  |
            | green   | #0f0  |
            | blue    | #00f  |
            | cyan    | #0ff  |
            | magenta | #f0f  |
            | yellow  | #ff0  |
            | black   | #000  |
            ''',
            input='data_colors.json')

    def testSetParserRecord(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('a = records[0]; parser = 123; 123')
        self.assertEqual(
            'Cannot set parser after it has been used',
            str(context.exception.__cause__.__cause__))

    def testRecordsIfUndefined(self):
        self.assertPyolin(
            'records if False',
            '''\
            ''')

    def testGenRecordsIfUndefined(self):
        self.assertRunPyolin(
            'records if False',
            _UNDEFINED_)

    def testUndefinedRepr(self):
        self.assertPyolin(
            '_UNDEFINED_',
            '''\
            Undefined()
            ''',
            output_format='repr')

    def testUndefinedStr(self):
        self.assertPyolin(
            '_UNDEFINED_',
            '''\
            ''',
            output_format='str')

    def testNameError(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_cli('idontknowwhatisthis + 1')
        self.assertEqual(
            "name 'idontknowwhatisthis' is not defined",
            str(context.exception.__cause__.__cause__))

    def testBegin(self):
        self.assertPyolin(
            'if BEGIN: mysum = 0; header = ("sum", "value")\n'
            'mysum += record[2]\n'
            'mysum, record[2]',
            '''\
            | sum | value |
            | --- | ----- |
            | 60  | 60    |
            | 118 | 58    |
            | 169 | 51    |
            | 218 | 49    |
            | 266 | 48    |
            ''')

    def testTrailingNewline(self):
        self.assertPyolin(
            'records\n',
            '''\
            | 0       | 1            | 2  | 3  | 4     |
            | ------- | ------------ | -- | -- | ----- |
            | Bucks   | Milwaukee    | 60 | 22 | 0.732 |
            | Raptors | Toronto      | 58 | 24 | 0.707 |
            | 76ers   | Philadelphia | 51 | 31 | 0.622 |
            | Celtics | Boston       | 49 | 33 | 0.598 |
            | Pacers  | Indiana      | 48 | 34 | 0.585 |
            ''')

    def testExecuteFunction(self):
        def get_records():
            # pylint: disable=undefined-variable
            return records  # type: ignore
            # pylint: enable=undefined-variable
        self.assertRunPyolin(
            get_records,
            [
                ('Bucks', 'Milwaukee', 60, 22, 0.732),
                ('Raptors', 'Toronto', 58, 24, 0.707),
                ('76ers', 'Philadelphia', 51, 31, 0.622),
                ('Celtics', 'Boston', 49, 33, 0.598),
                ('Pacers', 'Indiana', 48, 34, 0.585),
            ])

    def testExecuteFunctionRecordScoped(self):
        def get_records():
            # pylint: disable=undefined-variable
            return record[0]  # type: ignore
            # pylint: enable=undefined-variable
        self.assertRunPyolin(
            get_records,
            ['Bucks', 'Raptors', '76ers', 'Celtics', 'Pacers'])

    def testDoubleSemiColon(self):
        self.assertPyolin(
            'record = 1; record += 1;; record += 1; record + 1',
            '''\
            4
            ''')

    def testIfBeginDoubleSemiColon(self):
        '''
        Double semi-colon is treated as a newline
        '''
        self.assertPyolin(
            'if BEGIN: sum = 0;; sum += record[2]; sum, record[2]',
            '''\
            | 0   | 1  |
            | --- | -- |
            | 60  | 60 |
            | 118 | 58 |
            | 169 | 51 |
            | 218 | 49 |
            | 266 | 48 |
            ''')

    def testUndefinedIsFalse(self):
        self.assertRunPyolin('bool(_UNDEFINED_)', False)

    def testEndWithDoubleSemiColon(self):
        self.assertPyolin(
            'record[2];;',
            '''\
            | value |
            | ----- |
            | 60    |
            | 58    |
            | 51    |
            | 49    |
            | 48    |
            ''')

    def testSysArgv(self):
        '''
        sys.argv should be shifted, so sys.argv[1] should be the first one after the pyolin prog
        '''
        self.assertPyolin(
            'sys.argv',
            '''\
            | value   |
            | ------- |
            | pyolin  |
            | testing |
            | 1       |
            | 2       |
            | 3       |
            ''',
            extra_args=['testing', '1', '2', '3']
        )


    # TODOs:
    # Bash / Zsh autocomplete integration
