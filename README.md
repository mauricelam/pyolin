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

### Scope

It is possible for the Pyolin program to run multiple times over an
iterable sequence of data, called a scope. `record` is a scope that runs
the given program multiple times based on the parser, for example.

Only one scope can be accessed in a Pyolin program. An exception will be
raised if multiple scopes are mixed.

### Available variables

 - Record parsing (for table-like data):
    - `records` – Parses the input data into a sequence of records according
        to `cfg.parser`, and generates this `records` sequence. Each
        record is a tuple (often parsed from one line) that consists of
        many fields (columns). The separator for records and fields are
        configurable through the `--record_separator` and
        `--field_separator` options.
    - `record`, `fields` – A scope that will run the given program
        iteratively for each record. Additionally, `record.source` gives the
        original string of the given line before processing.
  - Line by line
    - `lines` – A sequence of lines separated by the newline character. For
        other line separators, use `contents.split(separator)`.
    - `line` – A scoped version of `lines` that iterates over each line,
        running the Pyolin program repeatedly.
    - File scope:
    - `file`, `contents` – Contents of the entire file as a single string.
    - `df` – Contents of the entire file as a pandas.DataFrame. (Available
        only if pandas is installed).
    - JSON scope:
    - `jsonobjs` – Reads one or more concatenated JSON objects from the
        input file.
    - `jsonobj` – Scoped version of `jsonobjs`. Note that if the input data
        contains only one JSON object, the result will return a single item
        rather than a sequence. To always return a sequence, use
        `foo(jsonobj) for jsonobj in jsonobjs`, or to always return a single
        value, use `jsonobj[0]`.
  - General:
    - `filename` – The name of the file being processed, possibly None if
        reading from stdin.
    - `cfg` – The Pyolin program configuration that can configure various
        beahviors of the program
        - `cfg.header` – A tuple that contains the headers of the columns in
        the output data. This assumes the output format is a table (list of
        tuples).
        If `None` (the default) and the header cannot be inferred from the
        input data, the columns will be numbered from zero.
        - `cfg.parser` – A parser instance that is used to parse the data. Any
        changes made to this field must be made before the input file
        contents are accessed.
            See the Parsers section for more.
        - `cfg.printer` – A printer instance that determines the format of the
        output data.
            See the Printers section for more.
    - Common module aliases
        - `pd` – pandas.
        - `np` – numpy.
        - All other modules can be directly referenced by name without
            explicitly using an import statement.

## Parsers

### `auto`

A parser that automatically detects the input data format. Supports JSON, field separated text (awk style), CSV, and TSV. The input detection logic is roughly as follows:
- If the input data contains a relatively uniform number of comma or tab delimiters, parse as CSV / TSV
- If the input data starts with '{' or '[', try to parse as JSON
- Otherwise, treat as `txt`.

### `txt`
alias: `awk`, `unix`

An input format parser that is similar to Awk's parsing strategy. It reads until the `record_separator` is found (default: `'\n'`), and for each record, it assumes the fields are separated by the `field_separator` (default: `' '` or `'\t'`). Regular expressions are allowed for both `record_separator` and `field_separator` for this parser.

### `csv`

Treats the input file as a "delimiter-separated value". By default the delimiter is comma (hence "csv"), but changing the `field_separator` can change the delimiter to another value. Regular expressions are only allowed for `record_separator` for this parser.

__`tsv`__: Same as `csv`, but the delimiter is tab instead.

__`csv_excel`, `csv_unix`__: Similar to `csv`, but parsed using the given dialect. See the [Python csv module documentation](https://docs.python.org/3/library/csv.html) for details on the dialects. Regular expressions are only allowed for `record_separator` for this parser.

### `json`

Treats the input file as JSON. This parser does not support streaming (the entire JSON content needs to be loaded into memory first).

## Printers

### `auto`

Automatically detect the suitable output format for best human-readability depending on the result data type. If the result datatype is table-like, this will be printed in a markdown table. Otherwise, if result is a complex dict or list, it will be printed in JSON. Otherwise, this will be printed in the "txt" format.

### `txt`
alias: `awk`, `unix`

A printer that prints out the records in text format, similar to Awk. By default it prints each record in a new line, where each field within a record is separated by a space. The separators can be modified by setting the corresponding fields of the printer. e.g. `printer.field_separator = ","` or `printer.record_separator = "\r\n"`

### `csv`

A printer that prints the data out in CSV format. This uses the [`excel` dialect](https://docs.python.org/3/library/csv.html#csv.excel) by default, but can be modified by setting the dialect field of the printer. e.g. `printer.dialect = csv.unix_dialect`

__`tsv`__: Same as `csv` but uses tabs as the delimiter.

### `markdown`
alias: `md`, `table`

Prints the data out in [markdown table format](https://www.markdownguide.org/extended-syntax/#tables). Note that this does not always print out a valid markdown table if the result data does not conform. For example, if the header has fewer number of fields than the data itself, the data will still be printed without a corresponding header, but depending on the markdown parser, that may be ignored or rejected. Similarly, if the resulting data is an empty list, it will print out only the header and divider rows, which is not a valid markdown table.

### `json`

Prints the data out in JSON format. The output JSON will be an array, and each record will be treated as an object, where the key is the header label.

### `jsonl`

Prints out the data in JSON-lines format, where each line is a valid JSON value.

### `repr`

Prints the result out using the Python built-in `repr` function.

### `str`

Prints the result out using the Python built-in `str` function.

### `binary`

Prints the raw binary the result, expected to be bytes or bytearray, to stdout. Typically piped to another command since the output may contain non-printable characters or special escape sequences.

## Motivation for creating Pyolin

Python is a powerful language that is easy to read and write, and it has lots of tools, built-in or libraries that helps with text and data manipulation extremely quickly. However, there are not a lot of usage for Python in the command line in the form of quick one-liner scripts. To start using Python in the command line you have to be somewhat committed in creating a script file.

In my opinion, with a few changes that provides implicit functionality, Python can also be a great language for writing short, simple one-liners in the command line like AWK and perl.
