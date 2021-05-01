#!/usr/bin/env python3

import argparse
from pol import pol
from hashbang import command, Argument


@command(
    Argument('modules', aliases='m', append=True),
    Argument('field_separator', aliases='F'),
    Argument('input_format', choices=('awk', 'tsv', 'csv')),
    formatter_class=argparse.RawDescriptionHelpFormatter)
def main(prog, input_file=None, *,
         field_separator=None,
         record_separator='\n',
         input_format='awk',
         modules=()):
    '''
    pol - Python one liners to easily parse and process data in Python.

    Pol processes text information from stdin or a given file and evaluates
    the given input `prog` and prints the result.

    Example:
        pol 'record[0] + record[1] if record[2] > 50'

    In pol, the input file is treated as a table, which consists of many
    records (lines). Each record is then consisted of many fields (columns).
    The separator for records and fields are configurable. (Using what???)

    Available variables:
      - Record scoped:
        record, fields - A tuple of the fields in the current line.
            Additionally, `record.str` gives the original string of the given
            line before processing.
        line – Alias for `record.str`.

        When referencing a variable in record scope, `prog` must not access
        any other variables in table scope. In this mode, pol iterates through
        each record from the input file and prints the result of `prog`.

      - Table scoped:
        records – A sequence of records (as described in "Record scoped"
            section above).
        lines – A sequence of lines (as described in "Record scoped" section
            above).
        file, contents – Contents of the entire file as a single string.
        df – Contents of the entire file as a pandas.DataFrame. (Available
            only if pandas is installed).
      - General:
        filename – The name of the file being processed, possibly None if
            reading from stdin.
        re – The regex module.
        pd – The pandas module, if installed.
    '''
    pol.pol(
        prog, input_file, field_separator=field_separator,
        record_separator=record_separator,
        input_format=input_format, modules=modules)


if __name__ == '__main__':
    main.execute()
