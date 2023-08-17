"""Detects whether a header is present in a table."""

import itertools
from typing import Iterator, List, Type, Union

from .record import Record
from .util import debug


def has_header(stream: Iterator[Record]) -> bool:
    """
    Creates a dictionary of types of data in each column. If any column is of a
    single type (say, integers), *except* for the first row, then the first row
    is presumed to be labels. If the type can't be determined, it is assumed to
    be a string in which case the length of the string is the determining
    factor: if all of the rows except for the first are the same length, it's a
    header. Finally, a 'vote' is taken at the end for each column, adding or
    subtracting from the likelihood of the first row being a header.
    """

    try:
        header = next(stream)  # assume first row is header
    except StopIteration:
        return False

    columns = len(header)
    column_types: List[Union[Type, int, None]] = [None] * columns
    irregular_row_count = 0
    sample_row_count = 0

    # Find the type or length of each column from a sample row
    for row in itertools.islice(stream, 0, 20):
        if len(row) != columns:
            irregular_row_count += 1
            if irregular_row_count > 4:
                return False
            continue  # skip rows that have irregular number of columns

        sample_row_count += 1

        for col, _ in enumerate(column_types):
            for this_type in (int, float, complex):
                try:
                    this_type(row[col].str)
                    break
                except (ValueError, OverflowError):
                    pass
            else:
                # fallback to length of string
                this_type = len(row[col].str)

            if this_type != column_types[col]:
                if column_types[col] is None:  # add new column type
                    column_types[col] = this_type
                else:
                    # type is inconsistent, remove column from
                    # consideration
                    column_types[col] = "Disqualified"

    if sample_row_count < irregular_row_count:
        return False

    debug("column types", column_types)
    # finally, compare results against first row and "vote"
    # on whether it's a header
    has_header_score = 0
    for col, col_type in enumerate(column_types):
        if isinstance(col_type, int):  # it's a length
            if len(header[col].str) != col_type:
                has_header_score += 1
            else:
                has_header_score -= 1
        elif col_type != "Disqualified" and col_type is not None:  # attempt typecast
            try:
                col_type(header[col].str)
            except (ValueError, TypeError):
                has_header_score += 1
            else:
                has_header_score -= 1
    debug("hasHeader", has_header_score)

    return has_header_score > 0
