# Pyolin

![CI](https://github.com/mauricelam/pyolin/actions/workflows/python-package.yml/badge.svg)
[![PyPI](https://img.shields.io/pypi/v/pyolin.svg?color=%230073b7)](https://pypi.org/project/pyolin/)

Tool to easily write Python one-liners

Pyolin processes data from stdin or a given file, evaluates the given input `prog` and prints the result.

Example:
```sh
cat table | pyolin 'record[0] + record[1] if record[2] > 50'
```

## Syntax

The first argument of Pyolin is the program. The program is a valid Python expression, optionally preceded by zero or more Python statements. Informally, this means the last part of your program must evaluate to a value. For example, the program can be `42`, `1 + 3`, or `list(range(10))`, but it cannot end in `a = 10` or `raise Exception()`.

For convenience, a few extensions were made to the Python syntax.

1. For the expression part of the program, you may use generator expressions without putting them in parentheses. For example, you can say

    ```sh
    pyolin 'len(r) for r in records'  # Valid in pyolin
    ```

    instead of

    ```sh
    pyolin '(len(r) for r in records)'  # Longer, but also valid in pyolin
    ```

2. The [conditional expression syntax](https://www.python.org/dev/peps/pep-0308/) `<expression1> if <condition> else <expression2>` is extended such that `else <expression2>` can be omitted, in which case its value will be ignored. For example, you can say

   ```sh
   pyolin 'record if record[2] > 10'
   ```

   to get the list of all records for which the second field is greater than 10. Under the hood, a special "_UNDEFINED_" value is returned for this expression, which is skipped when the result is printed. This is especially useful with record-scoped programs.

## Scopes

Pyolin has a concept of scope, which allows it to interpret the program differently based on which variables are being accessed.

### Table scope

If any of the variables `lines`, `records`, `file`, `contents`, and `df` are accessed, pyolin will run in table scoped mode, which means the given program will be run only once, and the result of the evaluation will be printed.

Examples:

```sh
pyolin 'sum(r[0] for r in records) / len(records)'  # Calculate the average of the first field
```

```sh
pyolin 'len(file)'  # Get the length of the given file
```

### Record scope

If any of the variables `record`, `fields`, `line` are accessed, pyolin will run in record scoped mode. In this mode Pyolin will loop through all the records from the incoming file, running the given program once per each line of the file. The results of executing on each record are then gathered into a list as the overall result.

Examples:

```sh
pyolin 'len(line)'  # Get the length of each line
```

```sh
pyolin 'record if record[0] > 10'  # Get all records where the first field is greater than 10
```

## Data format

In Pyolin, the input file is treated as a table, which consists of many
records (lines). Each record is then consisted of many fields (columns).
The separator for records and fields are configurable through the
`--record_separator` and `--field_separator` options.

Available variables:
  - Record scoped:
    - `record`, `fields` - A tuple of the fields in the current line.
        Additionally, `record.str` gives the original string of the given
        line before processing.
    - `line` – Alias for `record.str`.

    When referencing a variable in record scope, `prog` must not access
    any other variables in table scope. In this mode, pyolin iterates through
    each record from the input file and prints the result of `prog`.

  - Table scoped:
    - `records` – A sequence of records (as described in "Record scoped"
        section above).
    - `lines` – A sequence of lines (as described in "Record scoped" section
        above).
    - `file`, `contents` – Contents of the entire file as a single string.
    - `df` – Contents of the entire file as a pandas.DataFrame. (Available
        only if pandas is installed).
  - General:
    - `filename` – The name of the file being processed, possibly None if
        reading from stdin.
  - Assignable fields:
    - `header` – A tuple that contains the headers of the columns in the
        output data. This assumes the output format is a table (list of tuples).
        If `None` (the default) and the header cannot be inferred from the input
        data, the columns will be numbered from zero.
    - `parser` – A parser instance that is used to parse the data. Any changes
        made to this field must be made before the input file contents are accessed.
        See the Parsers section for more.
    - `printer` – A printer instance that determines the format of the output data.
        See the Printers section for more.
  - Modules:
    - `re`, `csv`, `pd` (pandas), `np` (numpy)

## Parsers

### `awk`
alias: `unix`

An input format parser that is similar to Awk's parsing strategy. It reads until the `record_separator` is found (default: `'\n'`), and for each record, it assumes the fields are separated by the `field_separator` (default: `' '` or `'\t'`). Regular expressions are allowed for both `record_separator` and `field_separator` for this parser.

### `csv`

Treats the input file as a "delimiter-separated value". By default the delimiter is comma (hence "csv"), but changing the `field_separator` can change the delimiter to another value. Regular expressions are only allowed for `record_separator` for this parser.

__`tsv`__: Same as `csv`, but the delimiter is tab instead.

__`csv_excel`, `csv_unix`__: Similar to `csv`, but parsed using the given dialect. See the [Python csv module documentation](https://docs.python.org/3/library/csv.html) for details on the dialects. Regular expressions are only allowed for `record_separator` for this parser.

### `json`

Treats the input file as JSON. This parser does not support streaming (the entire JSON content needs to be loaded into memory first).

### `binary`

Treats the input file as binary. The input file will be read in binary mode. `records`, `lines`, `df`, and other variables derived from these are not available when using this parser.

## Printers

### `auto`

Automatically detect the suitable output format for best human-readability depending on the result data type. If the result datatype is a list, this will be printed in a markdown table. Otherwise, this will be printed in the "Awk" format.

### `awk`
alias: `unix`

A printer that mimics the behavior of Awk. By default it prints each record in a new line, where each field within a record is separated by a space. The separators can be modified by setting the corresponding fields of the printer. e.g. `printer.field_separator = ","` or `printer.record_separator = "\r\n"`

### `csv`

A printer that prints the data out in CSV format. This uses the [`excel` dialect](https://docs.python.org/3/library/csv.html#csv.excel) by default, but can be modified by setting the dialect field of the printer. e.g. `printer.dialect = csv.unix_dialect`

__`tsv`__: Same as `csv` but uses tabs as the delimiter.

### `markdown`
alias: `table`

A printer that prints the data out in [markdown table format](https://www.markdownguide.org/extended-syntax/#tables). Note that this does not always print out a valid markdown table if the result data does not conform. For example, if the header has fewer number of fields than the data itself, the data will still be printed without a corresponding header, but depending on the markdown parser, that may be ignored or rejected. Similarly, if the resulting data is an empty list, it will print out only the header and divider rows, which is not a valid markdown table.

### `json`

A printer that prints the data out in JSON format. The output JSON will be an array, and each record will be treated as an object, where the key is the header label.

### `repr`

A printer that prints the result out using the Python built-in `repr` function.

### `str`

A printer that prints the result out using the Python built-in `str` function.

### `binary`

A printer that writes the raw binary the result, expected to be bytes or bytearray, to stdout.

## Motivation for creating Pyolin

Python is a powerful language that is easy to read and write, and it has lots of tools, built-in or libraries that helps with text and data manipulation extremely quickly. However, there are not a lot of usage for Python in the command line in the form of quick one-liner scripts. To start using Python in the command line you have to be somewhat committed in creating a script file.

In my opinion, with a few changes that provides implicit functionality, Python can also be a great language for writing short, simple one-liners in the command line like AWK and perl.
