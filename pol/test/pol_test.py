import contextlib
import os
from pol import pol
from pprint import pformat
import subprocess
import sys
import textwrap
import traceback
import unittest
from unittest import mock
from .utils import *


def _test_file(file):
    return os.path.join(os.path.dirname(__file__), file)


def run_pol(prog, *, data='data_nba.txt', **extra_args):
    with run_capturing_output(errmsg=f'Prog: {prog}') as output:
        pol.pol(prog, _test_file(data), **extra_args)
        return output.getvalue()


@contextlib.contextmanager
def polPopen(prog, extra_args=[], **kwargs):
    with subprocess.Popen(
        [sys.executable, 'main.py', prog] + extra_args,
        stdin=kwargs.get('stdin', subprocess.PIPE),
        stdout=kwargs.get('stdout', subprocess.PIPE),
        stderr=kwargs.get('stderr', subprocess.PIPE),
        universal_newlines=True,
        **kwargs
    ) as proc:
        yield proc


class PolTest(unittest.TestCase):

    maxDiff = None
    longMessage = False

    def assertPol(self, prog, expected_output, *, data='data_nba.txt', repr=False, **kwargs):
        actual = run_pol(prog, data=data, **kwargs)
        expected = textwrap.dedent(expected_output)
        self.assertEqual(
            actual,
            expected,
            msg=f'''\
Prog: pol \'{prog}\' {_test_file(data)}
Expected:
{textwrap.indent(expected, '    ')}
---

Actual:
{textwrap.indent(actual, '    ')}
---
''')

    def testLines(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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

    def testReorderFields(self):
        self.assertPol(
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
        self.assertPol(
            'record for record in records if record[1] == "Boston"',
            '''\
            | 0       | 1      | 2  | 3  | 4     |
            | ------- | ------ | -- | -- | ----- |
            | Celtics | Boston | 49 | 33 | 0.598 |
            ''')

    def testNumberConversion(self):
        self.assertPol(
            'record.str for record in records if record[2] > 50',
            '''\
            | value                          |
            | ------------------------------ |
            | Bucks Milwaukee    60 22 0.732 |
            | Raptors Toronto    58 24 0.707 |
            | 76ers Philadelphia 51 31 0.622 |
            ''')

    def testExpressionRecord(self):
        self.assertPol(
            'len(records)',
            '''\
            5
            ''')

    def testIfExpression(self):
        self.assertPol(
            'fields[0] if fields[3] > 30',
            '''\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            ''')

    def testTernaryExplicit(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
            'len([r for r in records if r[2] > 50])',
            '''\
            3
            ''')

    def testEnumerate(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
            'None',
            '''\
            None
            ''')

    def testRegex(self):
        self.assertPol(
            r'fields if re.match(r"^\d.*", fields[0])',
            '''\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            ''')

    def testAddition(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
            'fields[0] if fields[2] < 51',
            '''\
            | value   |
            | ------- |
            | Celtics |
            | Pacers  |
            ''')

    def testLe(self):
        self.assertPol(
            'fields[0] if fields[2] <= 51',
            '''\
            | value   |
            | ------- |
            | 76ers   |
            | Celtics |
            | Pacers  |
            ''')

    def testSubtraction(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
            'record if fields[2] > 50 and fields[3] > 30',
            '''\
            | 0     | 1            | 2  | 3  | 4     |
            | ----- | ------------ | -- | -- | ----- |
            | 76ers | Philadelphia | 51 | 31 | 0.622 |
            ''')

    def testAddHeader(self):
        self.assertPol(
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
        self.assertPol(
            'sum(line.count("0") for line in lines)',
            '''\
            7
            ''')

    def testMaxScore(self):
        self.assertPol(
            'max(r[2] for r in records)',
            '''\
            60
            ''')

    def testContents(self):
        self.assertPol(
            'len(contents)',
            '''\
            154
            ''')

    def testEmptyList(self):
        self.assertPol(
            '[]',
            '''\
            ''')

    def testStreamingStdin(self):
        with polPopen('line', extra_args=['--output_format=awk']) as proc:
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

    def testStreamingSlice(self):
        with polPopen(
                'records[:2]', extra_args=['--output_format=awk']) as proc:
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
        with polPopen(
                'records[1].str', extra_args=['--output_format=awk']) as proc:
            proc.stdin.write('Raptors Toronto    58 24 0.707\n')
            proc.stdin.write('Celtics Boston     49 33 0.598\n')
            proc.stdin.flush()
            with timeout(2):
                self.assertEqual(
                    proc.stdout.readline(),
                    'Celtics Boston     49 33 0.598\n')
            proc.stdin.write('Write more stuff...\n')

    def testRecordsIndex(self):
        self.assertPol(
            'records[1]',
            '''\
            Raptors Toronto 58 24 0.707
            ''')

    def testDestructuring(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
            'sum(r[3] for r in records), max(r[3] for r in records)',
            '''\
            144 34
            ''')

    def testModuleImport(self):
        self.assertPol(
            'import fnmatch; record[0] if fnmatch.fnmatch(record[0], "*.txt")',
            '''\
            | value                  |
            | ---------------------- |
            | dir/file.txt           |
            | dir/file1.txt          |
            | dir/fileb.txt          |
            | dir/subdir/subfile.txt |
            ''',
            data='data_files.txt')

    def testRecordVariables(self):
        self.assertPol(
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
        self.assertPol(
            'type(lines).__name__, type(records).__name__, '
            'type(file).__name__, type(contents).__name__',
            '''\
            StreamingSequence RecordSequence str str
            ''')

    def testBoolean(self):
        self.assertPol(
            'record if record[1].bool',
            '''\
            | 0          | 1    | 2  | 3    |
            | ---------- | ---- | -- | ---- |
            | dir        | True | 30 | 40.0 |
            | dir/subdir | True | 12 | 42.0 |
            ''',
            data='data_files.txt')

    def testFilename(self):
        self.assertPol(
            'filename',
            f'''\
            {os.path.dirname(__file__)}/data_files.txt
            ''',
            data='data_files.txt')

    def testBytes(self):
        self.assertPol(
            'b"hello"',
            '''\
            hello
            ''')

    def testReversed(self):
        self.assertPol(
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

    def testContains(self):
        self.assertPol(
            '"Raptors Toronto    58 24 0.707" in lines',
            '''\
            True
            ''')

    def testBase64(self):
        self.assertPol(
            'import base64; base64.b64encode(fields[0].bytes)',
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
        self.assertPol(
            'import urllib; urllib.parse.quote(line)',
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
        self.assertPol(
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
            data='data_files.txt')

    def testFieldsComparison(self):
        self.assertPol(
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
            data='data_files.txt')

    def testMultiplication(self):
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
        self.assertPol(
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
            data='data_grades_simple_csv.csv',
            field_separator=r',')

    def testFieldSeparatorRegex(self):
        self.assertPol(
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
            data='data_grades_simple_csv.csv',
            field_separator=r'[\.,]')

    def testRecordSeparator(self):
        self.assertPol(
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
            data='data_onerow.csv',
            record_separator=r',')

    def testRecordSeparatorMultipleChars(self):
        self.assertPol(
            'record',
            '''\
            | value                                    |
            | ---------------------------------------- |
            | JET                                      |
            | 0031201                                  |
            | 0001006,53521,1.000E+01,NBIC,HSELM,TRANS |
            | .000E+00,1.000E+00                       |
            | ,1,0,0                                   |
            ''',
            data='data_onerow.csv',
            record_separator=r',2')

    def testRecordSeparatorRegex(self):
        self.assertPol(
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
            data='data_onerow.csv',
            record_separator=r'[,.]')

    def testSimpleCsv(self):
        self.assertPol(
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
            data='data_grades_simple_csv.csv',
            input_format='csv')

    def testQuotedCsv(self):
        self.assertPol(
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
            data='data_news_decline.csv',
            input_format='csv')

    def testQuotedCsvStr(self):
        self.assertPol(
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
            data='data_news_decline.csv',
            input_format='csv')

    def testAutoCsv(self):
        self.assertPol(
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
            data='data_addresses.csv',
            input_format='csv')

    def testCsvExcel(self):
        self.assertPol(
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
            data='data_addresses.csv',
            input_format='csv_excel')

    def testCsvUnix(self):
        self.assertPol(
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
            data='data_addresses_unix.csv',
            input_format='csv')

    def testQuotedTsv(self):
        self.assertPol(
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
            data='data_news_decline.tsv',
            input_format='csv',
            field_separator='\t')

    def testStatement(self):
        self.assertPol(
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
        self.assertPol(
            'a = len(records); b = 2; a * b',
            '''\
            10
            ''')

    def testSyntaxError(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('a..x')
        self.assertEqual(
            textwrap.dedent('''\
                Invalid syntax:
                  a..x
                    ^'''),
            str(context.exception.__cause__))

    def testSyntaxErrorInStatement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('a..x; a+1')
        self.assertEqual(
            textwrap.dedent('''\
                Invalid syntax:
                  a..x
                    ^'''),
            str(context.exception.__cause__))

    def testHeaderDetection(self):
        self.assertPol(
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
            data='data_grades_with_header.csv',
            input_format='csv')

    def testPrintDataframeHeader(self):
        self.assertPol(
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
            data='data_grades_with_header.csv',
            input_format='csv')

    def testAssignToRecord(self):
        '''
        Try to confuse the parser by writing to a field called record
        '''
        self.assertPol(
            'record=1; record+1',
            '''\
            2
            ''')

    def testAccessRecordAndTable(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('a = record[0]; b = records; b')
        self.assertEqual(
            'Cannot access both record scoped and table scoped variables',
            str(context.exception.__cause__.__cause__))

    def testAccessTableAndRecord(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('a = records; b = record[0]; b')
        self.assertEqual(
            'Cannot access both record scoped and table scoped variables',
            str(context.exception.__cause__.__cause__))

    def testEmptyRecordScoped(self):
        self.assertPol(
            'record[0]',
            '',
            data=os.devnull)

    def testEmptyTableScoped(self):
        self.assertPol(
            'record for record in records',
            '',
            data=os.devnull)

    def testSemicolonInString(self):
        self.assertPol(
            '"hello; world"',
            'hello; world\n')

    def testStackTraceCleaning(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('import urllib; urllib.parse.quote(12345)')
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(5, len(formatted), pformat(formatted))
        self.assertIn('Traceback (most recent call last)', formatted[0])
        self.assertIn('pol_user_prog.py', formatted[1])
        self.assertIn('return quote_from_bytes', formatted[2])
        self.assertIn('"quote_from_bytes() expected bytes"', formatted[3])
        self.assertIn('quote_from_bytes() expected bytes', formatted[4])

    def testInvalidOutputFormat(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('1+1', output_format='invalid')
        self.assertEqual(
            'Unrecognized output format "invalid"',
            str(context.exception.__cause__))

    def testCsvOutputFormat(self):
        self.assertPol(
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
        self.assertPol(
            'printer.dialect = "unix"; records',
            '''\
            "Bucks","Milwaukee","60","22","0.732"
            "Raptors","Toronto","58","24","0.707"
            "76ers","Philadelphia","51","31","0.622"
            "Celtics","Boston","49","33","0.598"
            "Pacers","Indiana","48","34","0.585"
            ''',
            output_format='csv')

    def testCsvOutputNonTuple(self):
        self.assertPol(
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
        self.assertPol(
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
            data='data_addresses.csv')

    def testCsvOutputWithHeader(self):
        self.assertPol(
            'printer.header = True; df[["Last name", "SSN", "Final"]]',
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
            data='data_grades_with_header.csv',
            input_format='csv',
            output_format='csv')

    def testStreamingStdinCsv(self):
        with polPopen('record', ['--output_format', 'csv']) as proc:
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
        self.assertPol(
            'printer.header = True; record[0],record[2],record[7]',
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
            data='data_grades_simple_csv.csv',
            input_format='csv',
            output_format='csv')

    def testMarkdownOutput(self):
        self.assertPol(
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
            data='data_grades_with_header.csv',
            input_format='csv',
            output_format='markdown')

    def testTsvOutput(self):
        self.assertPol(
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
        self.assertPol(
            textwrap.dedent('''\
            record = 1
            record + 1\
            '''),
            '''\
            2
            ''')

    def testMultilineMixedInput(self):
        self.assertPol(
            textwrap.dedent('''\
            record = 1; record += 1
            record += 1; record + 1
            '''),
            '''\
            4
            ''')

    def testLastStatement(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('1+1;pass')
        self.assertEqual(
            textwrap.dedent('''\
            Cannot evaluate value from statement:
              pass'''),
            str(context.exception.__cause__))

    def testMultilinePythonProgram(self):
        self.assertPol(
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
        with mock.patch.dict(os.environ, {'POL_TABLE_WIDTH': '80'}):
            self.assertPol(
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
                data='data_amazon_reviews.tsv',
                input_format='tsv',
                output_format='markdown')

    def testMarkdownWrapping2(self):
        with mock.patch.dict(os.environ, {'POL_TABLE_WIDTH': '80'}):
            self.assertPol(
                'records',
                '''\
                | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11    | 12           | 13  | 14 |
                | - | - | - | - | - | - | - | - | - | - | -- | ----- | ------------ | --- | -- |
                | a | b | c | d | e | f | g | h | i | j | k  | lmnop | qrstuv123456 | wxy | z  |
                :   :   :   :   :   :   :   :   :   :   :    :       : 789123456789 :     :    :
                :   :   :   :   :   :   :   :   :   :   :    :       : 123456789    :     :    :
                ''',
                data='data_formatting.txt',
                output_format='markdown')

    def testJsonOutput(self):
        self.assertPol(
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
        self.assertPol(
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
            data='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testContains(self):
        self.assertPol(
            '("green", "#0f0") in records',
            '''\
            True
            ''',
            data='data_colors.json',
            input_format='json')

    def testSingleValueOutput(self):
        self.assertPol(
            'len(records)',
            '''\
            | value |
            | ----- |
            | 7     |
            ''',
            data='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testRecordOutput(self):
        self.assertPol(
            'records[0]',
            '''\
            | color | value |
            | ----- | ----- |
            | red   | #f00  |
            ''',
            data='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testRecordsWithHeader(self):
        self.assertPol(
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
            data='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testLinesWithHeader(self):
        self.assertPol(
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
            data='data_colors.json',
            input_format='json',
            output_format='markdown')

    def testMarkdownNonUniformColumnCount(self):
        self.assertPol(
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
        self.assertPol(
            'range(10)',
            '''\
            range(0, 10)
            ''',
            output_format='repr')

    def testReprPrinterRecords(self):
        self.assertPol(
            '"aloha\u2011\u2011\u2011"',
            '''\
            'aloha\u2011\u2011\u2011'
            ''',
            output_format='repr')

    def testStrPrinterRecords(self):
        self.assertPol(
            '"aloha\u2011\u2011\u2011"',
            '''\
            aloha\u2011\u2011\u2011
            ''',
            output_format='str')

    def testSetPrinter(self):
        self.assertPol(
            'printer = ReprPrinter(); range(10)',
            '''\
            range(0, 10)
            ''')

    def testPrinterNone(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('printer = None; 123')
        self.assertEqual(
            'printer must be an instance of Printer. Found "None" instead',
            str(context.exception.__cause__))

    def testRaiseStopIteration(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('raise StopIteration(); None')
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(3, len(formatted), pformat(formatted))
        self.assertIn('Traceback (most recent call last)', formatted[0])
        self.assertIn('pol_user_prog.py', formatted[1])
        self.assertIn('StopIteration', formatted[2])

    def testBinaryInputLen(self):
        self.assertPol(
            'len(file)',
            '''\
            21
            ''',
            input_format='binary',
            data='data_pickle')

    def testBinaryInputPickle(self):
        self.assertPol(
            'import pickle; pickle.loads(file)',
            '''\
            hello world
            ''',
            input_format='binary',
            data='data_pickle')

    def testBinaryInputAccessRecords(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('records', input_format='binary', data='data_pickle')
        self.assertEqual(
            'Record based attributes are not supported in binary input mode',
            str(context.exception.__cause__))

    def testSetFieldSeparator(self):
        self.assertPol(
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
            data='data_grades_simple_csv.csv')

    def testSetRecordSeparator(self):
        self.assertPol(
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
            data='data_onerow.csv')

    def testSetParser(self):
        self.assertPol(
            'parser = JsonParser(); df',
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
            data='data_colors.json')

    def testSetParserRecord(self):
        with self.assertRaises(ErrorWithStderr) as context:
            run_pol('a = records[0]; parser = 123; 123')
        self.assertEqual(
            'Cannot set parser after it has been used',
            str(context.exception.__cause__.__cause__))

    # Support windows
