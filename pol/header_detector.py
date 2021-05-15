import itertools

from .util import debug


def has_header(stream):
    # Creates a dictionary of types of data in each column. If any
    # column is of a single type (say, integers), *except* for the first
    # row, then the first row is presumed to be labels. If the type
    # can't be determined, it is assumed to be a string in which case
    # the length of the string is the determining factor: if all of the
    # rows except for the first are the same length, it's a header.
    # Finally, a 'vote' is taken at the end for each column, adding or
    # subtracting from the likelihood of the first row being a header.

    try:
        header = next(stream)  # assume first row is header
    except StopIteration:
        return False

    columns = len(header)
    columnTypes = [None] * columns

    for row in itertools.islice(stream, 0, 20):
        if len(row) != columns:
            continue  # skip rows that have irregular number of columns

        for col, _ in enumerate(columnTypes):
            for thisType in (int, float, complex):
                try:
                    thisType(row[col])
                    break
                except (ValueError, OverflowError):
                    pass
            else:
                # fallback to length of string
                thisType = len(row[col])

            if thisType != columnTypes[col]:
                if columnTypes[col] is None:  # add new column type
                    columnTypes[col] = thisType
                else:
                    # type is inconsistent, remove column from
                    # consideration
                    columnTypes[col] = 'Disqualified'

    debug('column types', columnTypes)
    # finally, compare results against first row and "vote"
    # on whether it's a header
    hasHeader = 0
    for col, colType in enumerate(columnTypes):
        if isinstance(colType, int):  # it's a length
            if len(header[col]) != colType:
                hasHeader += 1
            else:
                hasHeader -= 1
        elif colType != 'Disqualified' and colType is not None:  # attempt typecast
            try:
                colType(header[col])
            except (ValueError, TypeError):
                hasHeader += 1
            else:
                hasHeader -= 1
    debug('hasHeader', hasHeader)

    return hasHeader > 0
