import contextlib
import io
import pol
import signal
import subprocess
import sys
import textwrap
import unittest


class SubprocessError(Exception):

    def __init__(self, error, prog):
        self.error = error
        self.prog = prog

    def __str__(self):
        return f'''Subprocess failed

Prog: {self.prog}

===== STDERR =====
{self.error}
==================
'''


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


def run_pol(prog, *, data='data_nba.txt', **extra_args):
    out = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(out):
        with contextlib.redirect_stderr(err):
            try:
                pol.pol(prog, data, **extra_args)
            except Exception as e:
                raise SubprocessError(err.getvalue(), prog)
    return out.getvalue()


class PolTest(unittest.TestCase):

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
            'record.line for record in records if record[2] > 50',
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
        with subprocess.Popen(
                [sys.executable, 'pol.py', 'line'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
        ) as proc:
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
        with subprocess.Popen(
                [sys.executable, 'pol.py', 'records[:2]'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
        ) as proc:
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
        with subprocess.Popen(
                [sys.executable, 'pol.py', 'records[1].line'],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
        ) as proc:
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
            '''\
            data_files.txt
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


    # Support csv files
    # Support other separators
