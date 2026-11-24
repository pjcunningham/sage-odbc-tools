# Sage ODBC Tools

A command-line tool for extracting data and schemas from Sage Line 50 through an ODBC connection.

Use it to create repeatable snapshots of Sage tables, detect schema changes between exports, and generate application schemas or MySQL DDL and load scripts. Export the results of custom queries as CSV, JSON, Excel, or XML for reporting and analysis, or use the generated files to support integrations, backups, and migrations to other systems.

## Requirements and configuration

- Python 3.12 or later.
- A working Sage Line 50 ODBC DSN configured on the machine running the tool.
- The Python dependencies in `pyproject.toml` (install with `uv sync` when using `uv`).

The CLI reads its connection settings from a dotenv file. By default, `python-dotenv` searches for a `.env` file; use the global `--env-file` option (or `ENV_FILE`) to specify another file. The file must contain:

```dotenv
SAGE_ODBC_DSN=Your Sage ODBC DSN
SAGE_USERNAME=your_username
SAGE_PASSWORD=your_password
```

Keep credentials out of source control. The DSN must be configured in the ODBC Data Source Administrator and accessible to the Python process.

## Running the CLI

Run the module from the project environment:

```text
python -m sageodbc [OPTIONS] COMMAND [ARGS]...
```

After installing the package (for example with `uv sync` or `pip install .`), the `sageodbc` console command is also available. The same CLI can be started with `python -m sageodbc.cli`. Global options go before the command:

| Option | Description |
| --- | --- |
| `-V`, `--version` | Print the CLI version and exit. |
| `-v`, `--verbosity` | Set log level to `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG`; defaults to `WARNING`. |
| `--env-file PATH` | Read connection settings from `PATH`; can also be selected with `ENV_FILE`. |
| `--debug` / `--no-debug` | Enable or disable debug tracebacks for command errors; defaults to `--no-debug`. The `SAGEODBC_DEBUG` environment variable can set this option. |

Use `python -m sageodbc --help` for the command list and `python -m sageodbc COMMAND --help` for a command's arguments and options.

## Commands

In the syntax below, `FORMAT` is one of `csv`, `json`, `xlsx`, or `xml` (case-insensitive); it defaults to `csv`. The `dump-table` and `dump-tables` commands exclude rows marked deleted (`RECORD_DELETED = 1`) unless `--include-deleted` is specified. Single-file text output defaults to standard output; use `--output PATH` (or `-o PATH`) to write a file. XLSX is binary and requires an explicit output file. Bulk output directories must already exist. Existing output files are overwritten.

Output data is written to stdout and diagnostics to stderr. Commands return a nonzero exit status for usage, configuration, or database errors.

### Data export and table information

| Command | What it does |
| --- | --- |
| `about` | Prints a short program description. |
| `dump-table [--include-deleted] [--format FORMAT] [--output PATH] TABLE_NAME` | Writes one table's data to stdout or a file in the selected format. XLSX requires `--output`. |
| `dump-tables [--include-deleted] [--format FORMAT] --output-directory DIR` | Writes each table to the existing directory; filenames use lowercase snake case and the selected format extension. |
| `dump-table-names [--output PATH]` | Writes table names, one per line. |
| `dump-table-count TABLE_NAME` | Prints the number of rows in one table to standard output. |
| `dump-table-counts [--output PATH]` | Writes all table counts as `TABLE_NAME:COUNT`, one per line. |

`--include-deleted` is a boolean option and can also be written `--no-include-deleted`. It defaults to `--no-include-deleted`.

### Schema export

| Command | What it does |
| --- | --- |
| `dump-table-schema [--output PATH] TABLE_NAME` | Writes the selected table's ODBC column metadata as text. |
| `dump-table-schemas --output-directory DIR` | Writes each table's column metadata to a `.txt` file named with lowercase snake case. |
| `dump-table-rest-schema [--output PATH] TABLE_NAME` | Generates a `flask-rest-jsonapi` schema for one table. |
| `dump-table-rest-schemas --output-directory DIR` | Generates a `.py` schema file per table, named with lowercase snake case. |
| `dump-table-json-net-schema [--output PATH] TABLE_NAME NAMESPACE` | Generates a JSON.NET/C# schema for one table using the supplied C# namespace. |
| `dump-table-json-net-schemas NAMESPACE --output-directory DIR` | Generates a `.cs` schema file per table in the supplied C# namespace. |
| `schema-to-mysql-ddl [--output PATH] TABLE_NAME` | Generates a MySQL DDL script for one table. |
| `schemas-to-mysql-ddl --output-directory DIR` | Generates a `.sql` DDL file per table and an `_mysql_load_schemas.sql` loader file. |
| `compare-schemas [--format FORMAT] [--output PATH] LEFT_PATH RIGHT_PATH` | Compares two schema text files or common `.txt` filenames in two directories and exports line-level differences. |

`compare-schemas` is an offline command and does not require ODBC configuration. For directory inputs, only `.txt` files whose names occur in both directories are compared; unmatched files are ignored. The output contains left/right filenames, change type (`modified`, `added`, or `removed`), and line numbers. Schema lines represented as `Column(...)` are emitted as JSON objects in JSON output, or as individual `left_text_...` and `right_text_...` columns/elements in CSV, XLSX, and XML. Other changed lines remain in `left_text` and `right_text`. Differences default to CSV on stdout; use `--format csv`, `json`, `xlsx`, or `xml` and optionally `--output PATH` to save them. XLSX requires an output path.

### Queries and MySQL data loading

| Command | What it does |
| --- | --- |
| `query [--format FORMAT] [--output PATH] QUERY` | Executes a query and writes the result to stdout or a file. Quote a query containing spaces so it is passed as one argument. XLSX requires `--output`. |
| `query-from-file --input PATH_OR_- [--format FORMAT] [--output PATH]` | Executes SQL from a UTF-8 file or stdin (`--input -`) and writes the result in the selected format. SQL newlines are preserved. XLSX requires `--output`. |
| `query-to-sql [--output PATH] QUERY TABLE_NAME` | Executes a query and writes its results as SQL `INSERT` statements targeting `TABLE_NAME`. |
| `generate-mysql-load-data [--exclude-table TABLE_NAME]... --input-directory DIR [--output PATH]` | Generates a MySQL load-data script using the Sage table metadata and CSV files in the input directory. CSV filenames are expected to use lowercase snake case; repeat `--exclude-table` (or `-et`) to omit tables by their Sage table name. |

For example, migrate positional I/O arguments to the named options:

| Previous syntax | Current syntax |
| --- | --- |
| `dump-table CUSTOMERS .\exports\customers.csv csv` | `dump-table --format csv --output .\exports\customers.csv CUSTOMERS` |
| `dump-tables .\exports json` | `dump-tables --format json --output-directory .\exports` |
| `query-from-file .\queries\customers.sql .\exports\customers.xlsx xlsx` | `query-from-file --input .\queries\customers.sql --format xlsx --output .\exports\customers.xlsx` |

## Examples

```text
# Export one table as CSV to stdout, including deleted rows
python -m sageodbc --env-file .env dump-table --include-deleted SALES_LEDGER

# Export every table as JSON into an existing directory
python -m sageodbc dump-tables --format json --output-directory .\exports

# Run a query from a file and write an Excel workbook
python -m sageodbc query-from-file --input .\queries\customers.sql --format xlsx --output .\exports\customers.xlsx

# Save one table's schema, then print its row count
python -m sageodbc dump-table-schema --output .\exports\sales-ledger-schema.txt SALES_LEDGER
python -m sageodbc dump-table-count SALES_LEDGER

# Compare one schema file with another and save the differences as JSON
python -m sageodbc compare-schemas --format json --output .\exports\schema-diff.json .\old\customers.txt .\new\customers.txt

# Generate MySQL table DDL for every table
python -m sageodbc schemas-to-mysql-ddl --output-directory .\mysql-schema

# Read a multiline SQL query from stdin
python -m sageodbc query-from-file --input - < .\queries\customers.sql
```
