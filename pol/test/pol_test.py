import contextlib
import os
from pol import pol
import subprocess
import sys
import textwrap
import traceback
import unittest
from .utils import *


def run_pol(prog, *, data='data_nba.txt', **extra_args):
    with run_capturing_output(errmsg=f'Prog: {prog}') as output:
        pol.pol(
            prog,
            os.path.join(os.path.dirname(__file__), data),
            **extra_args)
        return output.getvalue()


@contextlib.contextmanager
def polPopen(prog, **kwargs):
    with subprocess.Popen(
        [sys.executable, 'main.py', prog],
        stdin=kwargs.get('stdin', subprocess.PIPE),
        stdout=kwargs.get('stdout', subprocess.PIPE),
        stderr=kwargs.get('stderr', subprocess.PIPE),
        universal_newlines=True,
        **kwargs
    ) as proc:
        yield proc


class PolTest(unittest.TestCase):

    maxDiff = None

    def assertPol(self, prog, expected_output, **kwargs):
        self.assertEqual(
            run_pol(prog, **kwargs),
            textwrap.dedent(expected_output))

    def testLines(self):
        self.assertPol(
            'line for line in lines',
            '''\
            Bucks Milwaukee    60 22 0.732
            Raptors Toronto    58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston     49 33 0.598
            Pacers Indiana     48 34 0.585
            ''')

    def testLine(self):
        self.assertPol(
            'line',
            '''\
            Bucks Milwaukee    60 22 0.732
            Raptors Toronto    58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston     49 33 0.598
            Pacers Indiana     48 34 0.585
            ''')

    def testFields(self):
        self.assertPol(
            'fields',
            '''\
            Bucks Milwaukee 60 22 0.732
            Raptors Toronto 58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston 49 33 0.598
            Pacers Indiana 48 34 0.585
            ''')

    def testReorderFields(self):
        self.assertPol(
            'fields[1], fields[0]',
            '''\
            Milwaukee Bucks
            Toronto Raptors
            Philadelphia 76ers
            Boston Celtics
            Indiana Pacers
            ''')

    def testConditional(self):
        self.assertPol(
            'record for record in records if record[1] == "Boston"',
            '''\
            Celtics Boston 49 33 0.598
            ''')

    def testNumberConversion(self):
        self.assertPol(
            'record.str for record in records if record[2] > 50',
            '''\
            Bucks Milwaukee    60 22 0.732
            Raptors Toronto    58 24 0.707
            76ers Philadelphia 51 31 0.622
            ''')

    def testExpressionRecord(self):
        self.assertPol(
            'len(records)',
            '''\
            5
            ''')

    def testImplicitIteration(self):
        self.assertPol(
            'fields[0] if fields[3] > 30',
            '''\
            76ers
            Celtics
            Pacers
            ''')

    def testTernaryExplicit(self):
        self.assertPol(
            'r[1] if len(r[1]) > 8 else "Name too short" for r in records',
            '''\
            Milwaukee
            Name too short
            Philadelphia
            Name too short
            Name too short
            ''')

    def testTernaryImplicit(self):
        self.assertPol(
            'fields[1] if fields[2] > 50 else "Score too low"',
            '''\
            Milwaukee
            Toronto
            Philadelphia
            Score too low
            Score too low
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
            0 Bucks Milwaukee    60 22 0.732
            1 Raptors Toronto    58 24 0.707
            2 76ers Philadelphia 51 31 0.622
            3 Celtics Boston     49 33 0.598
            4 Pacers Indiana     48 34 0.585
            ''')

    def testSkipNone(self):
        self.assertPol(
            '[None, 1, 2, 3]',
            '''\
            1
            2
            3
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
            76ers Philadelphia 51 31 0.622
            ''')

    def testAddition(self):
        self.assertPol(
            'fields[2] + 100',
            '''\
            160
            158
            151
            149
            148
            ''')

    def testRadd(self):
        self.assertPol(
            '100 + fields[2]',
            '''\
            160
            158
            151
            149
            148
            ''')

    def testFieldAddition(self):
        self.assertPol(
            'fields[2] + fields[3]',
            '''\
            82
            82
            82
            82
            82
            ''')

    def testFieldConcat(self):
        self.assertPol(
            'fields[2] + fields[0]',
            '''\
            60Bucks
            58Raptors
            5176ers
            49Celtics
            48Pacers
            ''')

    def testFieldConcatReversed(self):
        self.assertPol(
            'fields[0] + fields[2]',
            '''\
            Bucks60
            Raptors58
            76ers51
            Celtics49
            Pacers48
            ''')

    def testStringConcat(self):
        self.assertPol(
            'fields[0] + "++"',
            '''\
            Bucks++
            Raptors++
            76ers++
            Celtics++
            Pacers++
            ''')

    def testLt(self):
        self.assertPol(
            'fields[0] if fields[2] < 51',
            '''\
            Celtics
            Pacers
            ''')

    def testLe(self):
        self.assertPol(
            'fields[0] if fields[2] <= 51',
            '''\
            76ers
            Celtics
            Pacers
            ''')

    def testSubtraction(self):
        self.assertPol(
            'fields[2] - 50',
            '''\
            10
            8
            1
            -1
            -2
            ''')

    def testRsub(self):
        self.assertPol(
            '50 - fields[2]',
            '''\
            -10
            -8
            -1
            1
            2
            ''')

    def testLeftShift(self):
        self.assertPol(
            'fields[2] << 2',
            '''\
            240
            232
            204
            196
            192
            ''')

    def testNeg(self):
        self.assertPol(
            '(-fields[2])',
            '''\
            -60
            -58
            -51
            -49
            -48
            ''')

    def testRound(self):
        self.assertPol(
            'round(fields[2], -2)',
            '''\
            100
            100
            100
            0
            0
            ''')

    def testSkipFirstLine(self):
        self.assertPol(
            'l for l in lines[1:]',
            '''\
            Raptors Toronto    58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston     49 33 0.598
            Pacers Indiana     48 34 0.585
            ''')

    def testAnd(self):
        self.assertPol(
            'record if fields[2] > 50 and fields[3] > 30',
            '''\
            76ers Philadelphia 51 31 0.622
            ''')

    def testAddHeader(self):
        self.assertPol(
            '[("Team", "City", "Win", "Loss", "Winrate")] + records',
            '''\
            Team City Win Loss Winrate
            Bucks Milwaukee 60 22 0.732
            Raptors Toronto 58 24 0.707
            76ers Philadelphia 51 31 0.622
            Celtics Boston 49 33 0.598
            Pacers Indiana 48 34 0.585
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

    def testStreamingStdin(self):
        with polPopen('line') as proc:
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
        with polPopen('records[:2]') as proc:
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
        with polPopen('records[1].str') as proc:
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
            Milwaukee
            Toronto
            Philadelphia
            Boston
            Indiana
            ''')

    def testPercentage(self):
        self.assertPol(
            '(r[0], round(r[3] / sum(r[3] for r in records), 2)) '
            'for r in records',
            '''\
            Bucks 0.15
            Raptors 0.17
            76ers 0.22
            Celtics 0.23
            Pacers 0.24
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
            'record[0] if fnmatch.fnmatch(record[0], "*.txt")',
            '''\
            dir/file.txt
            dir/file1.txt
            dir/fileb.txt
            dir/subdir/subfile.txt
            ''',
            data='data_files.txt',
            modules=('fnmatch',))

    def testRecordVariables(self):
        self.assertPol(
            'type(record).__name__, type(line).__name__, '
            'type(fields).__name__',
            '''\
            Record str Record
            Record str Record
            Record str Record
            Record str Record
            Record str Record
            ''')

    def testFileVariables(self):
        self.assertPol(
            'type(lines).__name__, type(records).__name__, '
            'type(file).__name__, type(contents).__name__',
            '''\
            LazySequence LazySequence str str
            ''')

    def testBoolean(self):
        self.assertPol(
            'line if record[1].bool',
            '''\
            dir True 30 40.0
            dir/subdir True 12 42.0
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
            Pacers Indiana     48 34 0.585
            Celtics Boston     49 33 0.598
            76ers Philadelphia 51 31 0.622
            Raptors Toronto    58 24 0.707
            Bucks Milwaukee    60 22 0.732
            ''')

    def testContains(self):
        self.assertPol(
            '"Raptors Toronto    58 24 0.707" in lines',
            '''\
            True
            ''')

    def testBase64(self):
        self.assertPol(
            'base64.b64encode(bytes(fields[0]))',
            '''\
            QnVja3M=
            UmFwdG9ycw==
            NzZlcnM=
            Q2VsdGljcw==
            UGFjZXJz
            ''',
            modules=('base64',))

    def testUrlQuote(self):
        self.assertPol(
            'urllib.parse.quote(line)',
            '''\
            Bucks%20Milwaukee%20%20%20%2060%2022%200.732
            Raptors%20Toronto%20%20%20%2058%2024%200.707
            76ers%20Philadelphia%2051%2031%200.622
            Celtics%20Boston%20%20%20%20%2049%2033%200.598
            Pacers%20Indiana%20%20%20%20%2048%2034%200.585
            ''',
            modules=('urllib',))

    def testFieldsEqual(self):
        self.assertPol(
            'fields[2], fields[3], fields[2] == fields[3]',
            '''\
            30 40.0 False
            40 32.0 False
            23 56.0 False
            15 85.0 False
            31 31.0 True
            44 16.0 False
            12 42.0 False
            11 53.0 False
            ''',
            data='data_files.txt')

    def testFieldsComparison(self):
        self.assertPol(
            'fields[2], fields[3], fields[2] >= fields[3]',
            '''\
            30 40.0 False
            40 32.0 True
            23 56.0 False
            15 85.0 False
            31 31.0 True
            44 16.0 True
            12 42.0 False
            11 53.0 False
            ''',
            data='data_files.txt')

    def testMultiplication(self):
        self.assertPol(
            'fields[3] * 10',
            '''\
            220
            240
            310
            330
            340
            ''')

    def testFieldsMultiplication(self):
        self.assertPol(
            'fields[3] * fields[2]',
            '''\
            1320
            1392
            1581
            1617
            1632
            ''')

    def testStringMultiplication(self):
        self.assertPol(
            'fields[0] * 2',
            '''\
            BucksBucks
            RaptorsRaptors
            76ers76ers
            CelticsCeltics
            PacersPacers
            ''')

    def testPandasDataframe(self):
        # TODO: Remove the header rows, for easier continued parsing?
        self.assertPol(
            'df',
            '''\
                     0             1   2   3      4
            0    Bucks     Milwaukee  60  22  0.732
            1  Raptors       Toronto  58  24  0.707
            2    76ers  Philadelphia  51  31  0.622
            3  Celtics        Boston  49  33  0.598
            4   Pacers       Indiana  48  34  0.585
            ''')

    def testPandasDtypes(self):
        self.assertPol(
            'df.dtypes',
            '''\
            object
            object
            int64
            int64
            float64
            ''')

    def testPandaNumericOperations(self):
        self.assertPol(
            'df[2] * 2',
            '''\
            120
            116
            102
            98
            96
            ''')

    def testNumpyNumericOperations(self):
        self.assertPol(
            'np.power(df[2], 2)',
            '''\
            3600
            3364
            2601
            2401
            2304
            ''')

    def testFieldSeparator(self):
        self.assertPol(
            'record',
            '''\
            Alfalfa Aloysius 123-45-6789 40.0 90.0 100.0 83.0 49.0 D-
            Alfred University 123-12-1234 41.0 97.0 96.0 97.0 48.0 D+
            Gerty Gramma 567-89-0123 41.0 80.0 60.0 40.0 44.0 C
            Android Electric 087-65-4321 42.0 23.0 36.0 45.0 47.0 B-
            Franklin Benny 234-56-2890 50.0 1.0 90.0 80.0 90.0 B-
            George Boy 345-67-3901 40.0 1.0 11.0 -1.0 4.0 B
            Heffalump Harvey 632-79-9439 30.0 1.0 20.0 30.0 40.0 C
            ''',
            data='data_grades_simple_csv.csv',
            field_separator=r',')

    def testFieldSeparatorRegex(self):
        self.assertPol(
            'record',
            '''\
            Alfalfa Aloysius 123-45-6789 40 0 90 0 100 0 83 0 49 0 D-
            Alfred University 123-12-1234 41 0 97 0 96 0 97 0 48 0 D+
            Gerty Gramma 567-89-0123 41 0 80 0 60 0 40 0 44 0 C
            Android Electric 087-65-4321 42 0 23 0 36 0 45 0 47 0 B-
            Franklin Benny 234-56-2890 50 0 1 0 90 0 80 0 90 0 B-
            George Boy 345-67-3901 40 0 1 0 11 0 -1 0 4 0 B
            Heffalump Harvey 632-79-9439 30 0 1 0 20 0 30 0 40 0 C
            ''',
            data='data_grades_simple_csv.csv',
            field_separator=r'[\.,]')

    def testRecordSeparator(self):
        self.assertPol(
            'record',
            '''\
            JET
            20031201
            20001006
            53521
            1.000E+01
            NBIC
            HSELM
            TRANS
            2.000E+00
            1.000E+00
            2
            1
            0
            0
            ''',
            data='data_onerow.csv',
            record_separator=r',')

    def testRecordSeparatorMultipleChars(self):
        self.assertPol(
            'record',
            '''\
            JET
            0031201
            0001006,53521,1.000E+01,NBIC,HSELM,TRANS
            .000E+00,1.000E+00
            ,1,0,0
            ''',
            data='data_onerow.csv',
            record_separator=r',2')

    def testRecordSeparatorRegex(self):
        self.assertPol(
            'record',
            '''\
            JET
            20031201
            20001006
            53521
            1
            000E+01
            NBIC
            HSELM
            TRANS
            2
            000E+00
            1
            000E+00
            2
            1
            0
            0
            ''',
            data='data_onerow.csv',
            record_separator=r'[,.]')

    def testSimpleCsv(self):
        self.assertPol(
            'df[[0, 1, 2]]',
            '''\
                       0           1            2
            0    Alfalfa    Aloysius  123-45-6789
            1     Alfred  University  123-12-1234
            2      Gerty      Gramma  567-89-0123
            3    Android    Electric  087-65-4321
            4   Franklin       Benny  234-56-2890
            5     George         Boy  345-67-3901
            6  Heffalump      Harvey  632-79-9439
            ''',
            data='data_grades_simple_csv.csv',
            input_format='csv')

    def testQuotedCsv(self):
        self.assertPol(
            'record[0]',
            '''\
            60 Minutes
            48 Hours Mystery
            20/20
            Nightline
            Dateline Friday
            Dateline Sunday
            ''',
            data='data_news_decline.csv',
            input_format='csv')

    def testQuotedCsvStr(self):
        self.assertPol(
            'record.str',
            '''\
            "60 Minutes",       7.6, 7.4, 7.3
            "48 Hours Mystery", 4.1, 3.9, 3.6
            "20/20",            4.1, 3.7, 3.3
            "Nightline",        2.7, 2.6, 2.7
            "Dateline Friday",  4.1, 4.1, 3.9
            "Dateline Sunday",  3.5, 3.2, 3.1
            ''',
            data='data_news_decline.csv',
            input_format='csv')

    def testAutoCsv(self):
        self.assertPol(
            'df[[0,1,2]]',
            '''\
                       0         1                                 2
0                   John       Doe                 120 jefferson st.
1                   Jack  McGinnis                      220 hobo Av.
2          John "Da Man"    Repici                 120 Jefferson St.
3                Stephen     Tyler  7452 Terrace "At the Plaza" road
4                         Blankman                                  
5  Joan "the bone", Anne       Jet               9th, at Terrace plc
            ''',
            data='data_addresses.csv',
            input_format='csv')

    def testCsvExcel(self):
        self.assertPol(
            'df[[0,1,2]]',
            '''\
                       0         1                                 2
0                   John       Doe                 120 jefferson st.
1                   Jack  McGinnis                      220 hobo Av.
2          John "Da Man"    Repici                 120 Jefferson St.
3                Stephen     Tyler  7452 Terrace "At the Plaza" road
4                         Blankman                                  
5  Joan "the bone", Anne       Jet               9th, at Terrace plc
            ''',
            data='data_addresses.csv',
            input_format='csv_excel')

    def testCsvUnix(self):
        self.assertPol(
            '"|".join((record[0], record[1], record[2]))',
            '''\
            John|Doe|120 jefferson st.
            Jack|McGinnis|220 hobo Av.
            John "Da Man"|Repici|120 Jefferson St.
            Stephen|Tyler|7452 Terrace "At the Plaza" road
            |Blankman|
            Joan "the bone", Anne|Jet|9th, at Terrace plc
            ''',
            data='data_addresses_unix.csv',
            input_format='csv')

    def testQuotedTsv(self):
        self.assertPol(
            'record[0], record[2]',
            '''\
            60 Minutes 7.4
            48 Hours Mystery 3.9
            20/20 3.7
            Nightline 2.6
            Dateline Friday 4.1
            Dateline Sunday 3.2
            ''',
            data='data_news_decline.tsv',
            input_format='csv',
            field_separator='\t')

    def testStatement(self):
        self.assertPol(
            'a = record[2]; b = 1; a + b',
            '''\
            61
            59
            52
            50
            49
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
               Last name          SSN  Final
            0    Alfalfa  123-45-6789   49.0
            1     Alfred  123-12-1234   48.0
            2      Gerty  567-89-0123   44.0
            3    Android  087-65-4321   47.0
            4   Franklin  234-56-2890   90.0
            5     George  345-67-3901    4.0
            6  Heffalump  632-79-9439   40.0
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
            run_pol('urllib.parse.quote(12345)', modules=('urllib',))
        formatted = context.exception.__cause__.formatted_tb()
        self.assertEqual(5, len(formatted))
        self.assertTrue('Traceback (most recent call last)' in formatted[0])
        self.assertTrue('pol_user_prog.py' in formatted[1])
        self.assertTrue('return quote_from_bytes' in formatted[2])
        self.assertTrue('"quote_from_bytes() expected bytes"' in formatted[3])
        self.assertTrue('quote_from_bytes() expected bytes' in formatted[4])


    # Support different output formats (unix, csv, markdown)
