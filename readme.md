# Sage ODBC Tools

A command-line tool for extracting data and schemas from Sage Line 50 through an ODBC connection.

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

The same CLI can also be started with `python -m sageodbc.cli`. Global options go before the command:

| Option | Description |
| --- | --- |
| `-V`, `--version` | Print the CLI version and exit. |
| `-v`, `--verbosity` | Set log level to `CRITICAL`, `ERROR`, `WARNING`, `INFO`, or `DEBUG`; defaults to `WARNING`. |
| `--env-file PATH` | Read connection settings from `PATH`; can also be selected with `ENV_FILE`. |
| `--debug` / `--no-debug` | Enable or disable debug tracebacks for command errors; defaults to `--no-debug`. The `SAGEODBC_DEBUG` environment variable can set this option. |

Use `python -m sageodbc --help` for the command list and `python -m sageodbc COMMAND --help` for a command's arguments and options.

## Commands

In the syntax below, `OUTPUT_FORMAT` is one of `csv`, `json`, `xlsx`, or `xml` (case-insensitive). The `dump-table` and `dump-tables` commands exclude rows marked deleted (`RECORD_DELETED = 1`) unless `--include-deleted` is specified. Output directories must already exist. Output files may already exist and are overwritten.

### Data export and table information

| Command | What it does |
| --- | --- |
| `about` | Prints a short program description. |
| `dump-table [--include-deleted] TABLE_NAME SAVE_TO OUTPUT_FORMAT` | Writes one table's data to a file in the selected format. |
| `dump-tables [--include-deleted] OUTPUT_DIRECTORY OUTPUT_FORMAT` | Writes each table to a file in the existing directory; filenames use lowercase snake case and the selected format extension. |
| `dump-table-names SAVE_TO` | Writes table names, one per line. |
| `dump-table-count TABLE_NAME` | Prints the number of rows in one table to standard output. |
| `dump-table-counts SAVE_TO` | Writes all table counts as `TABLE_NAME:COUNT`, one per line. |

`--include-deleted` is a boolean option and can also be written `--no-include-deleted`. It defaults to `--no-include-deleted`.

### Schema export

| Command | What it does |
| --- | --- |
| `dump-table-schema TABLE_NAME SAVE_TO` | Writes the selected table's ODBC column metadata as text. |
| `dump-table-schemas OUTPUT_DIRECTORY` | Writes each table's column metadata to a `.txt` file named with lowercase snake case. |
| `dump-table-rest-schema TABLE_NAME SAVE_TO` | Generates a `flask-rest-jsonapi` schema file for one table. |
| `dump-table-rest-schemas OUTPUT_DIRECTORY` | Generates a `.py` schema file per table, named with lowercase snake case. |
| `dump-table-json-net-schema TABLE_NAME NAMESPACE SAVE_TO` | Generates a JSON.NET/C# schema for one table using the supplied C# namespace. |
| `dump-table-json-net-schemas NAMESPACE OUTPUT_DIRECTORY` | Generates a `.cs` schema file per table in the supplied C# namespace. |
| `schema-to-mysql-ddl TABLE_NAME SAVE_TO` | Generates a MySQL DDL file for one table. |
| `schemas-to-mysql-ddl OUTPUT_DIRECTORY` | Generates a `.sql` DDL file per table and an `_mysql_load_schemas.sql` loader file. |

### Queries and MySQL data loading

| Command | What it does |
| --- | --- |
| `query QUERY SAVE_TO OUTPUT_FORMAT` | Executes a query and writes the result in the selected format. Quote a query containing spaces so it is passed as one argument. |
| `query-from-file QUERY_FILE SAVE_TO OUTPUT_FORMAT` | Executes the query in an existing text file and writes the result in the selected format. |
| `query-to-sql QUERY TABLE_NAME SAVE_TO` | Executes a query and writes its results as SQL `INSERT` statements targeting `TABLE_NAME`. |
| `generate-mysql-load-data [--exclude-table TABLE_NAME]... INPUT_DIRECTORY SAVE_TO` | Generates a MySQL load-data script using the Sage table metadata and CSV files in the input directory. CSV filenames are expected to use lowercase snake case; repeat `--exclude-table` (or `-et`) to omit tables by their Sage table name. |

## Examples

```text
# Export one table, including deleted rows
python -m sageodbc --env-file .env dump-table --include-deleted SALES_LEDGER sales-ledger.csv csv

# Export every table as JSON into an existing directory
python -m sageodbc dump-tables .\exports json

# Run a query from a file and write an Excel workbook
python -m sageodbc query-from-file .\queries\customers.sql .\exports\customers.xlsx xlsx

# Save one table's schema, then print its row count
python -m sageodbc dump-table-schema SALES_LEDGER .\exports\sales-ledger-schema.txt
python -m sageodbc dump-table-count SALES_LEDGER

# Generate MySQL table DDL for every table
python -m sageodbc schemas-to-mysql-ddl .\mysql-schema
```
