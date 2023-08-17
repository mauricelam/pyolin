"""Examples from AWK tutorials.

https://www.pement.org/awk/awk1line.txt. Used as a stress test for how good
pyolin / Python is at similar tasks."""

import pytest
from .conftest import File


# FILE SPACING:


def test_double_space_file(pyolin):
    """awk '1;{print ""}'"""
    in_ = """\
        1
        2
        3"""
    assert pyolin(r'record[0] + "\n"', input_=in_, output_format="awk") == (
        """\
        1

        2

        3

        """
    )


def test_double_space_file_alt(pyolin):
    """awk 'BEGIN{ORS="\n\n"};1'"""
    in_ = """\
        1
        2
        3"""
    assert pyolin(
        r'cfg.printer.record_separator="\n\n"; record[0]',
        input_=in_,
        output_format="awk",
    ) == (
        """\
        1

        2

        3

        """
    )


def test_double_space_already_blank(pyolin):
    """
    # double space a file which already has blank lines in it. Output file
    # should contain no more than one blank line between lines of text.
    # NOTE: On Unix systems, DOS lines which have only CRLF (\r\n) are
    # often treated as non-blank, and thus 'NF' alone will return TRUE.
    awk 'NF{print $0 "\n"}'
    """
    in_ = """\
        1
        2

        3"""
    assert pyolin(r'record[0] + "\n" if record', input_=in_, output_format="awk") == (
        """\
        1

        2

        3

        """
    )


# NUMBERING AND CALCULATIONS:


def test_add_line_numbers(pyolin):
    """
    # precede each line by its line number FOR THAT FILE (left alignment).
    # Using a tab (\t) instead of space will preserve margins.
    awk '{print FNR "\t" $0}' files*
    """
    assert pyolin(
        'f"{i+1}\t{r[0]}" for i, r in enumerate(records)',
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
        1\tBucks
        2\tRaptors
        3\t76ers
        4\tCeltics
        5\tPacers
        """
    )


@pytest.mark.skip(reason="Multiple file inputs not supported")
def test_add_line_number_for_all_files(pyolin):
    """
    # precede each line by its line number FOR ALL FILES TOGETHER, with tab.
    awk '{print NR "\t" $0}' files*
    """


def test_add_line_number_printf(pyolin):
    """
    # number each line of a file (number on left, right-aligned)
    awk '{printf("%5d : %s\n", NR,$0)}'
    """
    assert pyolin(
        'f"{i+1:5d} : {line}" for i, line in enumerate(lines)',
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
            1 : Bucks Milwaukee    60 22 0.732
            2 : Raptors Toronto    58 24 0.707
            3 : 76ers Philadelphia 51 31 0.622
            4 : Celtics Boston     49 33 0.598
            5 : Pacers Indiana     48 34 0.585
        """
    )


def test_number_lines_if_not_blank(pyolin):
    """
    # number each line of file, but only print numbers if line is not blank
    awk 'NF{$0=++a " :" $0};1'
    awk '{print (NF? ++a " :" :"") $0}'
    """
    in_ = """\
        1
        2

        3"""
    assert pyolin(
        'i = 0;; f"{(i:=i+1):5d} : {r.str}" if r else r.str for r in records',
        input_=in_,
        output_format="awk",
    ) == (
        """\
            1 : 1
            2 : 2

            3 : 3
        """
    )


def test_count_end_lines(pyolin):
    """
    # count lines (emulates "wc -l")
    awk 'END{print NR}'
    """
    assert pyolin("len(lines)", input_=File("data_nba.txt"), output_format="awk") == (
        """\
        5
        """
    )


def test_print_sums(pyolin):
    """
    # print the sums of the fields of every line
    awk '{s=0; for (i=1; i<=NF; i++) s=s+$i; print s}'
    """
    in_ = """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    assert pyolin("sum(record)", input_=in_, output_format="awk") == (
        """\
        1
        2
        4
        8
        """
    )


def test_print_sum_of_all_rows(pyolin):
    """
    # add all fields in all lines and print the sum
    awk '{for (i=1; i<=NF; i++) s=s+$i}; END{print s}'
    """
    in_ = """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    assert pyolin(
        "sum(field for record in records for field in record)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        15
        """
    )


def test_absolute_value(pyolin):
    """
    # print every line after replacing each field with its absolute value
    awk '{for (i=1; i<=NF; i++) if ($i < 0) $i = -$i; print }'
    awk '{for (i=1; i<=NF; i++) $i = ($i < 0) ? -$i : $i; print }'
    """
    in_ = """\
        1 -1
        2 -2
        3 -3
        """
    assert pyolin(
        "sum(abs(f) for f in record)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        2
        4
        6
        """
    )


def test_total_number_of_fields(pyolin):
    """
    # print the total number of fields ("words") in all lines
    awk '{ total = total + NF }; END {print total}' file
    """
    in_ = """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    assert pyolin(
        "sum(len(r) for r in records)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        10
        """
    )


def test_word_occurence_count(pyolin):
    """
    # print the total number of lines that contain "Beth"
    awk '/Beth/{n++}; END {print n+0}' file
    """
    in_ = """\
        Richard
        Beth
        Ricky
        Bonnie
        Beth
        Thomas
        Richard
        """
    assert pyolin(
        "sum(1 for line in lines if 'Beth' in line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        2
        """
    )


def test_print_largest(pyolin):
    """
    # print the largest first field and the line that contains it
    # Intended for finding the longest string in field #1
    awk '$1 > max {max=$1; maxline=$0}; END{ print max, maxline}'
    """
    assert pyolin(
        "max(((r[0], r.str) for r in records), key=lambda i: i[0])",
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
        Raptors Raptors Toronto    58 24 0.707
        """
    )


def test_print_number_of_fields(pyolin):
    """
    # print the number of fields in each line, followed by the line
    awk '{ print NF ":" $0 } '
    """
    in_ = """\
    1
    1 1
    1 2 1
    1 3 3 1
    """
    assert pyolin("len(record), line", input_=in_, output_format="awk") == (
        """\
        1 1
        2 1 1
        3 1 2 1
        4 1 3 3 1
        """
    )


def test_print_last_field(pyolin):
    """
    # print the last field of each line
    awk '{ print $NF }'
    """
    assert pyolin("record[-1]", input_=File("data_nba.txt"), output_format="awk") == (
        """\
        0.732
        0.707
        0.622
        0.598
        0.585
        """
    )


def test_print_last_field_of_last_line(pyolin):
    """
    # print the last field of the last line
    awk '{ field = $NF }; END{ print field }'
    """
    assert pyolin(
        "records[-1][-1]", input_=File("data_nba.txt"), output_format="awk"
    ) == (
        """\
        0.585
        """
    )


def test_print_more_than_four_fields(pyolin):
    """
    # print every line with more than 4 fields
    awk 'NF > 4'
    """
    in_ = """\
    1
    1 1
    1 2 1
    1 3 3 1
    1 4 6 4 1
    1 5 10 10 5 1
    """
    assert pyolin("record if len(record) > 4", input_=in_, output_format="awk") == (
        """\
        1 4 6 4 1
        1 5 10 10 5 1
        """
    )


# STRING CREATION:


def test_generate_string_repeated(pyolin):
    """
    # create a string of a specific length (e.g., generate 90 x's)
    awk 'BEGIN{while (a++<90) s=s "x"; print s}'
    """
    assert pyolin("'x' * 90", output_format="awk") == (
        """\
        xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
        """
    )


def test_insert_string_at_position(pyolin):
    """
    # insert a string of specific length at a certain character position
    # Example: insert 5 dashes after column #6 of each input line.
    gawk --re-interval 'BEGIN{while(a++<5)s=s "-"};{sub(/^.{6}/,"&" s)};1'
    """
    assert pyolin(
        "f'{line[:6]}-----{line[6:]}'", input_=File("data_nba.txt"), output_format="awk"
    ) == (
        """\
        Bucks -----Milwaukee    60 22 0.732
        Raptor-----s Toronto    58 24 0.707
        76ers -----Philadelphia 51 31 0.622
        Celtic-----s Boston     49 33 0.598
        Pacers----- Indiana     48 34 0.585
        """
    )


# TEXT CONVERSION AND SUBSTITUTION:


def test_convert_crlf_to_lf(pyolin):
    """
    # IN UNIX ENVIRONMENT: convert DOS newlines (CR/LF) to Unix format
    awk '{sub(/\r$/,"")};1'   # assumes EACH line ends with Ctrl-M
    """
    in_ = """\
    1\r
    1 1\r
    1 2 1\r
    1 3 3 1\r
    """
    assert pyolin(r"line.rstrip('\r')", input_=in_, output_format="awk") == (
        """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    )


def test_convert_lf_to_crlf(pyolin):
    """
    # IN UNIX ENVIRONMENT: convert Unix newlines (LF) to DOS format
    awk '{sub(/$/,"\r")};1'
    """
    in_ = """\
    1
    1 1
    1 2 1
    1 3 3 1
    """
    assert pyolin(r"line + '\r'", input_=in_, output_format="awk") == (
        """\
        1\r
        1 1\r
        1 2 1\r
        1 3 3 1\r
        """
    )


def test_remove_leading_whitespace(pyolin):
    """
    # delete leading whitespace (spaces, tabs) from front of each line
    # aligns all text flush left
    awk '{sub(/^[ \t]+/, "")};1'
    """
    in_ = """\
       1
    1 1
        1 2 1
    \t1 3 3 1
    """
    assert pyolin(r"line.lstrip(' \t')", input_=in_, output_format="awk") == (
        """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    )


def test_remove_trailing_whitespace(pyolin):
    """
    # delete trailing whitespace (spaces, tabs) from end of each line
    awk '{sub(/[ \t]+$/, "")};1'
    """
    in_ = """\
    1
    1 1   
    1 2 1 \t   
    1 3 3 1
    """  # noqa: W291
    assert pyolin(r"line.rstrip(' \t')", input_=in_, output_format="awk") == (
        """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    )


def test_remove_leading_and_trailing_whitespace(pyolin):
    """
    # delete BOTH leading and trailing whitespace from each line
    awk '{gsub(/^[ \t]+|[ \t]+$/,"")};1'
    awk '{$1=$1};1'           # also removes extra space between fields
    """
    in_ = """\
    1
       1 1   
      1 2 1 \t   
      \t1 3 3 1
    """  # noqa: W291
    assert pyolin(r"line.strip(' \t')", input_=in_, output_format="awk") == (
        """\
        1
        1 1
        1 2 1
        1 3 3 1
        """
    )


def test_add_indentation(pyolin):
    """
    # insert 5 blank spaces at beginning of each line (make page offset)
    awk '{sub(/^/, "     ")};1'
    """
    assert pyolin(
        r"'     ' + line", input_=File("data_nba.txt"), output_format="awk"
    ) == (
        """\
             Bucks Milwaukee    60 22 0.732
             Raptors Toronto    58 24 0.707
             76ers Philadelphia 51 31 0.622
             Celtics Boston     49 33 0.598
             Pacers Indiana     48 34 0.585
        """
    )


def test_right_align(pyolin):
    """
    # align all text flush right on a 79-column width
    awk '{printf "%79s\n", $0}' file*
    """
    assert pyolin(
        r"f'{line:>79s}'", input_=File("data_nba.txt"), output_format="awk"
    ) == (
        """\
                                                         Bucks Milwaukee    60 22 0.732
                                                         Raptors Toronto    58 24 0.707
                                                         76ers Philadelphia 51 31 0.622
                                                         Celtics Boston     49 33 0.598
                                                         Pacers Indiana     48 34 0.585
        """
    )


def test_center_align(pyolin):
    """
    # center all text on a 79-character width
    awk '{l=length();s=int((79-l)/2); printf "%"(s+l)"s\n",$0}' file*
    """
    assert pyolin(
        r"f'{line:^79s}'", input_=File("data_nba.txt"), output_format="awk"
    ) == (
        """\
                                Bucks Milwaukee    60 22 0.732                         
                                Raptors Toronto    58 24 0.707                         
                                76ers Philadelphia 51 31 0.622                         
                                Celtics Boston     49 33 0.598                         
                                Pacers Indiana     48 34 0.585                         
        """  # noqa: W291
    )


def test_string_substitution(pyolin):
    """
    # substitute (find and replace) "foo" with "bar" on each line
    awk '{gsub(/foo/,"bar")}; 1'          # replace ALL instances in a line
    """
    in_ = """\
    The foolish food fight fooled the football fans
    The foolish fool fell off the footbridge while foozling with his footsie
    """
    assert pyolin(r"line.replace('foo', 'bar')", input_=in_, output_format="awk") == (
        """\
        The barlish bard fight barled the bartball fans
        The barlish barl fell off the bartbridge while barzling with his bartsie
        """
    )


def test_string_substitution_fourth(pyolin):
    """
    # substitute (find and replace) "foo" with "bar" on each line
    gawk '{$0=gensub(/foo/,"bar",4)}; 1'  # replace only 4th instance
    """
    # This solution employs what I call the "iterator trick", which is to
    # preallocate an iterator (`itertools.count()`) so that it keeps track of
    # the state for us, removing the need for reassigning to a global variable,
    # which lends itself better for one-liners. Similar things can be done by
    # allocating and operating on a mutable object such as a list with one item.
    in_ = """\
    The foolish food fight fooled the football fans
    The foolish fool fell off the footbridge while foozling with his footsie
    """
    assert pyolin(
        "r=itertools.count();re.sub(r'foo', lambda m: 'bar' if next(r) == 3 else m.group(), line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The foolish food fight fooled the bartball fans
        The foolish fool fell off the footbridge while barzling with his footsie
        """
    )


def test_string_substitution_with_baz(pyolin):
    """
    # substitute "foo" with "bar" ONLY for lines which contain "baz"
    awk '/baz/{gsub(/foo/, "bar")}; 1'
    """
    in_ = """\
    The foolish food fight fooled the football fans
    The bazooka-wielding fool fell off the footbridge while foozling with his footsie
    """
    assert pyolin(
        "line.replace('foo', 'bar') if 'baz' in line else line",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The foolish food fight fooled the football fans
        The bazooka-wielding barl fell off the bartbridge while barzling with his bartsie
        """
    )


def test_string_substitution_without_baz(pyolin):
    """
    # substitute "foo" with "bar" EXCEPT for lines which contain "baz"
    awk '!/baz/{gsub(/foo/, "bar")}; 1'
    """
    in_ = """\
    The foolish food fight fooled the football fans
    The bazooka-wielding fool fell off the footbridge while foozling with his footsie
    """
    assert pyolin(
        "line.replace('foo', 'bar') if 'baz' not in line else line",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The barlish bard fight barled the bartball fans
        The bazooka-wielding fool fell off the footbridge while foozling with his footsie
        """
    )


def test_substitute_multiple_strings(pyolin):
    """
    # change "scarlet" or "ruby" or "puce" to "red"
    awk '{gsub(/scarlet|ruby|puce/, "red")}; 1'
    """
    in_ = """\
    The bouquet of flowers was a riot of color, with scarlet roses, puce lilies, and ruby carnations
    """
    assert pyolin(
        "re.sub(r'scarlet|ruby|puce', 'red', line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The bouquet of flowers was a riot of color, with red roses, red lilies, and red carnations
        """
    )


def test_reverse_order_of_lines(pyolin):
    """
    # reverse order of lines (emulates "tac")
    awk '{a[i++]=$0} END {for (j=i-1; j>=0;) print a[j--] }' file*
    """
    in_ = """\
    c
    a
    t
    """
    assert pyolin("lines[::-1]", input_=in_, output_format="awk") == (
        """\
        t
        a
        c
        """
    )


def test_backslash_newline(pyolin):
    """
    # if a line ends with a backslash, append the next line to it (fails if
    # there are multiple lines ending with backslash...)
    awk '/\\$/ {sub(/\\$/,""); getline t; print $0 t; next}; 1' file*
    """
    in_ = (
        "The art of war is of vital \\\nimportance to the State.\n"
        "It is a matter of life and death, \\\na road either to safety or to ruin."
    )
    assert pyolin(
        # TODO: Make this yield-based method less clunky
        # r"acc = '';; for line in lines: ((acc := line[:-1]) if line[-1] == '\\' else (yield acc + line)); None",
        r"''.join(line[:-1] if line[-1] == '\\' else f'{line}\n' for line in lines).rstrip('\n')",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The art of war is of vital importance to the State.
        It is a matter of life and death, a road either to safety or to ruin.
        """
    )


def test_sort(pyolin):
    """
    # print and sort the login names of all users
    awk -F ":" '{print $1 | "sort" }' /etc/passwd
    """
    in_ = """\
    foo
    bar
    baz
    """
    assert pyolin(
        r"sorted(lines)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        bar
        baz
        foo
        """
    )


def test_swap_field(pyolin):
    """
    # print the first 2 fields, in opposite order, of every line
    awk '{print $2, $1}' file
    """
    assert pyolin(
        r"(record[1], record[0])",
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
        Milwaukee Bucks
        Toronto Raptors
        Philadelphia 76ers
        Boston Celtics
        Indiana Pacers
        """
    )


def test_delete_field(pyolin):
    """
    # print every line, deleting the second field of that line
    awk '{ $2 = ""; print }'
    """
    assert pyolin(
        r"(record[0], *record[2:])",
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
        Bucks 60 22 0.732
        Raptors 58 24 0.707
        76ers 51 31 0.622
        Celtics 49 33 0.598
        Pacers 48 34 0.585
        """
    )


def test_print_fields_in_reverse(pyolin):
    """
    # print in reverse order the fields of every line
    awk '{for (i=NF; i>0; i--) printf("%s ",$i);print ""}' file
    """
    assert pyolin(
        r"reversed(record)",
        input_=File("data_nba.txt"),
        output_format="awk",
    ) == (
        """\
        0.732 22 60 Milwaukee Bucks
        0.707 24 58 Toronto Raptors
        0.622 31 51 Philadelphia 76ers
        0.598 33 49 Boston Celtics
        0.585 34 48 Indiana Pacers
        """
    )


def test_concatenate_five_lines(pyolin):
    """
    # concatenate every 5 lines of input, using a comma separator
    # between fields
    awk 'ORS=NR%5?",":"\n"' file
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        r"cfg.printer.field_separator=','; lines[i:i+5] for i in range(0, 20, 5)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        100,101,102,103,104
        105,106,107,108,109
        110,111,112,113,114
        115,116,117,118,119
        """
    )


# SELECTIVE PRINTING OF CERTAIN LINES:


def test_print_first_10_lines(pyolin):
    """
    # print first 10 lines of file (emulates behavior of "head")
    awk 'NR < 11'
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        r"lines[:10]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        100
        101
        102
        103
        104
        105
        106
        107
        108
        109
        """
    )


def test_print_first_line(pyolin):
    """
    # print first line of file (emulates "head -1")
    awk 'NR>1{exit};1'
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        r"lines[0]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        100
        """
    )


def test_print_last_two_lines(pyolin):
    """
    # print the last 2 lines of a file (emulates "tail -2")
    awk '{y=x "\n" $0; x=$0};END{print y}'
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        r"lines[-2:]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        118
        119
        """
    )


def test_print_last_line(pyolin):
    """
    # print the last line of a file (emulates "tail -1")
    awk 'END{print}'
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        r"lines[-1]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        119
        """
    )


def test_regex_match(pyolin):
    """
    # print only lines which match regular expression (emulates "grep")
    awk '/regex/'
    """
    in_ = """\
    This is a demo
    of regex matching
    """
    assert pyolin(
        r"line if re.search(r'regex', line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        of regex matching
        """
    )


def test_regex_not_match(pyolin):
    """
    # print only lines which do NOT match regex (emulates "grep -v")
    awk '!/regex/'
    """
    in_ = """\
    This is a demo
    of regex matching
    """
    assert pyolin(
        r"line if not re.search(r'regex', line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        This is a demo
        """
    )


def test_print_matching_lines(pyolin):
    """
    # print any line where field #5 is equal to "abc123"
    awk '$5 == "abc123"'
    """
    in_ = """\
    Bucks Milwaukee    60 22 foo001
    Raptors Toronto    58 24 bar2
    76ers Philadelphia 51 31 abc123
    Celtics Boston     49 33 abc404
    Pacers Indiana     48 34 baz123
    """
    assert pyolin(
        r"line if record[4] == 'abc123'",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        76ers Philadelphia 51 31 abc123
        """
    )


def test_print_not_matching_lines(pyolin):
    """
    # print only those lines where field #5 is NOT equal to "abc123"
    # This will also print lines which have less than 5 fields.
    awk '$5 != "abc123"'
    awk '!($5 == "abc123")'
    """
    in_ = """\
    Bucks Milwaukee    60 22 foo001
    Raptors Toronto    58 24 bar2
    76ers Philadelphia 51 31 abc123
    Celtics Boston     49 33 abc404
    Pacers Indiana     48 34
    """
    assert pyolin(
        r"line if len(record) < 5 or record[4] != 'abc123'",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        Bucks Milwaukee    60 22 foo001
        Raptors Toronto    58 24 bar2
        Celtics Boston     49 33 abc404
        Pacers Indiana     48 34
        """
    )


def test_field_match_regex(pyolin):
    """
    # matching a field against a regular expression
    awk '$7  ~ /^[a-f]/'    # print line if field #7 matches regex
    awk '$7 !~ /^[a-f]/'    # print line if field #7 does NOT match regex
    """
    in_ = """\
    dir True 30 40.0 memory
    dir/file.txt False 40 32.0 random
    dir/file1.txt False 23 56.0 deadbeef
    dir/file2.mp4 False 15 85.0 0x30add
    dir/filea.png False 31 31.0 246692
    dir/fileb.txt False 44 16.0 304f53
    dir/subdir True 12 42.0 directory
    dir/subdir/subfile.txt False 11 53.0 subfile
    """
    assert pyolin(
        r"line if re.match(r'^[a-f]', record[4])",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        dir/file1.txt False 23 56.0 deadbeef
        dir/subdir True 12 42.0 directory
        """
    )


def test_print_line_before_match(pyolin):
    """
    # print the line immediately before a regex, but not the line
    # containing the regex
    awk '/regex/{print x};{x=$0}'
    awk '/regex/{print (NR==1 ? "match on line 1" : x)};{x=$0}'
    """
    in_ = """\
    Bucks Milwaukee    60 22 0.732
    Raptors Toronto    58 24 0.707
    76ers Philadelphia 51 31 0.622
    Celtics Boston     49 33 0.598
    Pacers Indiana     48 34 0.585
    """
    assert pyolin(
        (
            r"shifted = ['match on line 1', *lines];"
            r"(shifted[i] for i, l in enumerate(lines) if re.search(r'Bucks|Celtics', l))"
        ),
        input_=in_,
        output_format="awk",
    ) == (
        """\
        match on line 1
        76ers Philadelphia 51 31 0.622
        """
    )


def test_print_line_after_match(pyolin):
    """
    # print the line immediately after a regex, but not the line
    # containing the regex
    awk '/regex/{getline;print}'
    """
    in_ = """\
    Bucks Milwaukee    60 22 0.732
    Raptors Toronto    58 24 0.707
    76ers Philadelphia 51 31 0.622
    Celtics Boston     49 33 0.598
    Pacers Indiana     48 34 0.585
    """
    assert pyolin(
        (
            r"(lines[i+1] for i, l in enumerate(lines[:-1]) if re.search(r'Bucks|Celtics', l))"
        ),
        input_=in_,
        output_format="awk",
    ) == (
        """\
        Raptors Toronto    58 24 0.707
        Pacers Indiana     48 34 0.585
        """
    )


def test_multiple_matches(pyolin):
    """
    # grep for AAA and BBB and CCC (in any order on the same line)
    awk '/AAA/ && /BBB/ && /CCC/'
    """
    in_ = """\
    AAA BBB
    CCC BBB AAA
    AAA BBB CCC DDD
    ABABC
    """
    assert pyolin(
        "line if all(item in line for item in ('AAA', 'BBB', 'CCC'))",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        CCC BBB AAA
        AAA BBB CCC DDD
        """
    )


def test_match_in_order(pyolin):
    """
    # grep for AAA and BBB and CCC (in that order)
    awk '/AAA.*BBB.*CCC/'
    """
    in_ = """\
    AAA BBB
    CCC BBB AAA
    AAA BBB CCC DDD
    ABABC
    """
    assert pyolin(
        "line if re.match(r'AAA.*BBB.*CCC', line)",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        AAA BBB CCC DDD
        """
    )


def test_line_length_greater(pyolin):
    """
    # print only lines of 65 characters or longer
    awk 'length > 64'
    """
    in_ = """\
    I have a dream
    Ask not what your country can do for you, ask what you can do for your country
    The only thing we have to fear is fear itself
    The greatest glory in living lies not in never falling, but in rising every time we fall
    The journey of a thousand miles begins with a single step
    """
    assert pyolin(
        "line if len(line) > 64",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        Ask not what your country can do for you, ask what you can do for your country
        The greatest glory in living lies not in never falling, but in rising every time we fall
        """
    )


def test_line_length_shorter(pyolin):
    """
    # print only lines of less than 65 characters
    awk 'length < 64'
    """
    in_ = """\
    I have a dream
    Ask not what your country can do for you, ask what you can do for your country
    The only thing we have to fear is fear itself
    The greatest glory in living lies not in never falling, but in rising every time we fall
    The journey of a thousand miles begins with a single step
    """
    assert pyolin(
        "line if len(line) < 64",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        I have a dream
        The only thing we have to fear is fear itself
        The journey of a thousand miles begins with a single step
        """
    )


def test_print_from_match_to_end_of_file(pyolin):
    """
    # print section of file from regular expression to end of file
    awk '/regex/,0'
    awk '/regex/,EOF'
    """
    in_ = """\
    I have a dream
    Ask not what your country can do for you, ask what you can do for your country
    The only thing we have to fear is fear itself
    The greatest glory in living lies not in never falling, but in rising every time we fall
    The journey of a thousand miles begins with a single step
    """
    assert pyolin(
        (
            "matched = False;"
            "line for line in lines if matched or (matched := re.search(r'glory', line))"
        ),
        input_=in_,
        output_format="awk",
    ) == (
        """\
        The greatest glory in living lies not in never falling, but in rising every time we fall
        The journey of a thousand miles begins with a single step
        """
    )


def test_print_by_line_number(pyolin):
    """
    # print section of file based on line numbers (lines 8-12, inclusive)
    awk 'NR==8,NR==12'
    """
    in_ = "\n".join(str(i) for i in range(100, 120))
    assert pyolin(
        "lines[7:12]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        107
        108
        109
        110
        111
        """
    )


def test_print_specific_line(pyolin):
    """
    # print line number 52
    awk 'NR==52'
    awk 'NR==52 {print;exit}'          # more efficient on large files
    """
    in_ = "\n".join(str(i) for i in range(100, 200))
    assert pyolin(
        "lines[51]",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        151
        """
    )


def test_print_between_matches(pyolin):
    """
    # print section of file between two regular expressions (inclusive)
    awk '/Iowa/,/Montana/'             # case sensitive
    """
    in_ = """\
    I have a dream
    Ask not what your country can do for you, ask what you can do for your country
    The only thing we have to fear is fear itself
    The greatest glory in living lies not in never falling, but in rising every time we fall
    The journey of a thousand miles begins with a single step
    """
    assert pyolin(
        (
            "ilines = enumerate(lines);;"
            "start = next((i for i, l in ilines if re.search(r'country', l)), len(lines));;"
            "end = next((i + 1 for i, l in ilines if re.search(r'glory', l)), len(lines));;"
            "lines[start:end]"
        ),
        input_=in_,
        output_format="awk",
    ) == (
        """\
        Ask not what your country can do for you, ask what you can do for your country
        The only thing we have to fear is fear itself
        The greatest glory in living lies not in never falling, but in rising every time we fall
        """
    )


# SELECTIVE DELETION OF CERTAIN LINES:


def test_delete_blank_lines(pyolin):
    """
    # delete ALL blank lines from a file (same as "grep '.' ")
    awk NF
    awk '/./'
    """
    in_ = """\
    I have a dream
    Ask not what your country can do for you, ask what you can do for your country
    The only thing we have to fear is fear itself

    The greatest glory in living lies not in never falling, but in rising every time we fall

    The journey of a thousand miles begins with a single step
    """
    assert pyolin(
        "line if line.str",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        I have a dream
        Ask not what your country can do for you, ask what you can do for your country
        The only thing we have to fear is fear itself
        The greatest glory in living lies not in never falling, but in rising every time we fall
        The journey of a thousand miles begins with a single step
        """
    )


def test_remove_consecutive_duplicates(pyolin):
    """
    # remove duplicate, consecutive lines (emulates "uniq")
    awk 'a !~ $0; {a=$0}'
    """
    in_ = """\
    3
    0
    6
    2
    4
    3
    0
    0
    """
    assert pyolin(
        "line for line, next_line in itertools.zip_longest(lines, lines[1:]) if line != next_line",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        3
        0
        6
        2
        4
        3
        0
        """
    )


def test_remove_duplicate_lines(pyolin):
    """
    # remove duplicate, nonconsecutive lines
    awk '!a[$0]++'                     # most concise script
    awk '!($0 in a){a[$0];print}'      # most efficient script
    """
    in_ = """\
    3
    0
    6
    2
    4
    3
    0
    0
    """
    assert pyolin(
        "list(dict.fromkeys(lines))",
        input_=in_,
        output_format="awk",
    ) == (
        """\
        3
        0
        6
        2
        4
        """
    )
