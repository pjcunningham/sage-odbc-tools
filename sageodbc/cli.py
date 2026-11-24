# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import logging
import re
import sys
import tempfile
import traceback
from functools import wraps
from pathlib import Path

import click
import click_log
from dotenv import dotenv_values

from .manager import Manager
from .models import OutputFormatEnum, OutputFormatType
from .version import __version__

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

def _redact_sensitive_values(text, manager):
    password = manager.database.password
    if password:
        text = text.replace(password, '[REDACTED]')
    return re.sub(r'(?i)(\b(?:pwd|password)\s*=\s*)[^;\s]+', r'\1[REDACTED]', text)


def _handle_command_error(error, manager):
    message = _redact_sensitive_values(str(error), manager).strip()
    if not message:
        message = f'Command failed with {type(error).__name__}.'

    if manager.debug:
        diagnostics = ''.join(traceback.format_exception(error))
        logger.error('%s', _redact_sensitive_values(diagnostics, manager).rstrip())
    else:
        logger.error('%s', message)

    raise click.ClickException(message) from error


def _get_manager(ctx):
    root_context = ctx.find_root()
    options = root_context.obj
    manager = options.get('manager')
    if manager is not None:
        return manager

    try:
        values = dotenv_values(options['env_file'])
    except Exception as ex:
        raise click.ClickException('Unable to read the ODBC configuration file.') from ex

    required_settings = ('SAGE_ODBC_DSN', 'SAGE_USERNAME', 'SAGE_PASSWORD')
    missing_settings = [setting for setting in required_settings if not values.get(setting)]
    if missing_settings:
        raise click.ClickException(
            f"Missing required ODBC configuration setting(s): {', '.join(missing_settings)}."
        )

    manager = Manager(
        logger,
        values['SAGE_ODBC_DSN'],
        values['SAGE_USERNAME'],
        values['SAGE_PASSWORD'],
        options['debug'],
    )
    options['manager'] = manager
    root_context.call_on_close(manager.close)
    return manager


def pass_manager(function):
    @click.pass_context
    @wraps(function)
    def wrapper(ctx, *args, **kwargs):
        manager = _get_manager(ctx)
        return ctx.invoke(function, manager, *args, **kwargs)

    return wrapper


def _write_text_output(output, callback, output_format=None, suffix='.txt'):
    if output != '-':
        return callback(output)

    if output_format == OutputFormatEnum.xlsx:
        raise click.ClickException('XLSX output requires a file path; use --output PATH.')

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_output = Path(temporary_directory) / f'output{suffix}'
        result = callback(str(temporary_output))
        with temporary_output.open('r', encoding='utf-8', newline='') as output_file:
            click.echo(output_file.read(), nl=False)
        return result


def _read_query_file(query_file):
    if query_file == '-':
        return sys.stdin.read()

    with open(query_file, 'r', encoding='utf-8', newline='') as input_file:
        return input_file.read()


@click.group()
@click.version_option(
    __version__, '-V', '--version', prog_name='sageodbc',
    message='%(prog)s v%(version)s',
)
@click_log.simple_verbosity_option(logger, default="WARNING")
@click.option('--env-file', envvar='ENV_FILE')
@click.option('--debug/--no-debug', default=False, envvar='SAGEODBC_DEBUG')
@click.pass_context
def cli(ctx, env_file, debug):
    ctx.obj = {'env_file': env_file, 'debug': debug}
    logger.info(f"Debug mode is {'on' if debug else 'off'}")


@cli.command('about')
def about():
    click.echo('Sage ODBC ... not for accountants')


@cli.command('dump-table')
@click.option('--include-deleted/--no-include-deleted', default=False)
@click.option('-f', '--format', 'output_format', type=OutputFormatType(OutputFormatEnum), default=OutputFormatEnum.csv, show_default=True)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@click.argument('table-name', type=str)
@pass_manager
def dump_table(manager: Manager, include_deleted: bool, table_name: str, save_to: str, output_format: OutputFormatEnum):
    """Dump a Sage table's data to stdout or a file using the selected format.

       TABLE_NAME is the name of the table to dump. XLSX output requires --output.
    """
    logger.info(f'Table name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    logger.info(f'Include Deleted: {include_deleted}')
    try:
        _write_text_output(
            save_to,
            lambda output: manager.dump_table(table_name, output, output_format, include_deleted),
            output_format,
            f'.{output_format.value}',
        )
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-tables')
@click.option('--include-deleted/--no-include-deleted', default=False)
@click.option('-f', '--format', 'output_format', type=OutputFormatType(OutputFormatEnum), default=OutputFormatEnum.csv, show_default=True)
@click.option('--output-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@pass_manager
def dump_tables(manager: Manager, include_deleted: bool, output_directory: str, output_format: OutputFormatEnum):
    """Dump all Sage tables data to a directory using a given format

       Each table is saved to an individual file in the OUTPUT_DIRECTORY

       Filename is snake-cased, lower-cased table name

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """

    logger.info(f'Output directory: {output_directory}')
    logger.info(f'Output format: {output_format}')
    logger.info(f'Include Deleted: {include_deleted}')
    try:
        manager.dump_tables(output_directory, output_format, include_deleted)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-schema')
@click.argument('table_name', type=str)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def dump_table_schema(manager: Manager, table_name: str, save_to: str):
    """Dump a Sage table's schema to stdout or a file.

       TABLE_NAME is the name of the table to dump.
    """

    logger.info(f'Table name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, lambda output: manager.dump_table_schema(table_name, output))
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-schemas')
@click.option('--output-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@pass_manager
def dump_table_schemas(manager: Manager, output_directory: str):
    """Dump all Sage table schemas to a directory

       Each table is saved to an individual text file in the OUTPUT_DIRECTORY

       Filename is snake-cased, lower-cased table name .txt extension

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """

    logger.info(f'Output Directory: {output_directory}')
    try:
        manager.dump_table_schemas(output_directory)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-names')
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False), default='-', show_default=True)
@pass_manager
def dump_table_names(manager: Manager, save_to: str):
    """Dump all Sage table names to stdout or a file."""

    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, manager.dump_table_names)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-counts')
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False), default='-', show_default=True)
@pass_manager
def dump_table_counts(manager: Manager, save_to: str):
    """Dump all Sage table counts to stdout or a file."""

    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, manager.dump_table_counts)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-count')
@click.argument('table_name', type=str)
@pass_manager
def dump_table_count(manager: Manager, table_name: str):
    """Dump a Sage table's count to STDOUT

       TABLE_NAME is the name of the table to count.
    """

    logger.info(f'Table Name: {table_name}')
    try:
        _count = manager.dump_table_count(table_name)
        click.echo(_count)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-rest-schema')
@click.argument('table_name', type=str)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def dump_table_rest_schema(manager: Manager, table_name: str, save_to: str):
    """Create a flask-rest-jsonapi schema on stdout or in a file.

       TABLE_NAME is the name of the table.
    """

    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, lambda output: manager.dump_table_rest_schema(table_name, output))
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-rest-schemas')
@click.option('--output-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@pass_manager
def dump_table_rest_schemas(manager: Manager, output_directory: str):
    """Create a flask-rest-jsonapi schema file for all Sage tables

       Each table is saved to an individual python file in the OUTPUT_DIRECTORY

       Filename is snake-cased, lower-cased table name .py extension

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """

    logger.info(f'Output Directory: {output_directory}')
    try:
        manager.dump_table_rest_schemas(output_directory)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-json-net-schema')
@click.argument('table_name', type=str)
@click.argument('namespace', type=str)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def dump_table_json_net_schema(manager: Manager, table_name: str, namespace: str, save_to: str):
    """Create a JSON.NET schema on stdout or in a file.

       TABLE_NAME is the name of the table.

       NAMESPACE is the namespace of c# class.
    """

    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, lambda output: manager.dump_table_json_net_schema(table_name, namespace, output))
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('dump-table-json-net-schemas')
@click.argument('namespace', type=str)
@click.option('--output-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@pass_manager
def dump_table_json_net_schemas(manager: Manager, namespace: str, output_directory: str):
    """Create a JSON.NET schema file for all Sage tables

       Each table is saved to an individual c# file in the OUTPUT_DIRECTORY

       NAMESPACE is the namespace of c# class.

       Filename is snake-cased, lower-cased table name .cs extension

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """

    logger.info(f'Output Directory: {output_directory}')
    try:
        manager.dump_table_json_net_schemas(namespace, output_directory)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('query')
@click.argument('query', type=str)
@click.option('-f', '--format', 'output_format', type=OutputFormatType(OutputFormatEnum), default=OutputFormatEnum.csv, show_default=True)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def query(manager: Manager, query: str, save_to: str, output_format: OutputFormatEnum):
    """Execute a Sage query and dump results to stdout or a file.

        QUERY is the SQL statement to execute. XLSX output requires --output.
    """
    logger.info(f'Query: {query}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        _write_text_output(
            save_to,
            lambda output: manager.query(query, output, output_format),
            output_format,
            f'.{output_format.value}',
        )
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('query-from-file')
@click.option('--input', 'query_file', required=True, help='SQL file to read, or - to read SQL from stdin.')
@click.option('-f', '--format', 'output_format', type=OutputFormatType(OutputFormatEnum), default=OutputFormatEnum.csv, show_default=True)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def query_from_file(manager: Manager, query_file: str, save_to: str, output_format: OutputFormatEnum):
    """Execute SQL from a file or stdin and dump results to stdout or a file.

        Use --input - to read SQL from stdin. XLSX output requires --output.
    """
    logger.info(f'Query File: {query_file}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        query_text = _read_query_file(query_file)
        _write_text_output(
            save_to,
            lambda output: manager.query(query_text, output, output_format),
            output_format,
            f'.{output_format.value}',
        )
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('query-to-sql')
@click.argument('query', type=str)
@click.argument('table_name', type=str)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def query_to_sql(manager: Manager, query: str, table_name: str, save_to: str):
    """Execute a Sage query and dump results to a sql INSERT INTO file

        QUERY: the query to execute a

        TABLE_NAME is the name of the INSERT INTO {table_name}.

        SAVE_TO is the name of the SQL file to dump to.

    """
    logger.info(f'Query: {query}')
    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, lambda output: manager.query_to_sql(query, table_name, output), suffix='.sql')
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('schema-to-mysql-ddl')
@click.argument('table_name', type=str)
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def schema_to_mysql_ddl(manager: Manager, table_name: str, save_to: str):
    """Create a MySQL DDL schema file for a table

       TABLE_NAME is the name of the table.

       SAVE_TO is the name of the file to dump to.
    """
    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(save_to, lambda output: manager.schema_to_mysql_ddl(table_name, output), suffix='.sql')
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('schemas-to-mysql-ddl')
@click.option('--output-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@pass_manager
def schemas_to_mysql_ddl(manager: Manager, output_directory: str):
    """Create MySQL DDL schema files for all tables

       Filenames are snake-cased, lower-cased table name .sql extension

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """
    logger.info(f'Output Directory: {output_directory}')
    try:
        manager.schemas_to_mysql_ddl(output_directory)
    except Exception as ex:
        _handle_command_error(ex, manager)


@cli.command('generate-mysql-load-data')
@click.option('--exclude-table', '-et', type=str, multiple=True)
@click.option('--input-directory', required=True, type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.option('-o', '--output', 'save_to', type=click.Path(exists=False, dir_okay=False, file_okay=True), default='-', show_default=True)
@pass_manager
def generate_mysql_load_data(manager: Manager, exclude_table: list[str], input_directory: str, save_to: str):
    """Creates a MySQL sql file to load multiple sql insert files

       INPUT_DIRECTORY is the directory containing the insert .sql files

       SAVE_TO is the name of the file to dump to.
    """
    for table in exclude_table:
        logger.info(f'Exclude table: {table}')
    logger.info(f'Input Directory: {input_directory}')
    logger.info(f'Save to file: {save_to}')
    try:
        _write_text_output(
            save_to,
            lambda output: manager.generate_mysql_load_data(input_directory, output, exclude_table),
            suffix='.sql',
        )
    except Exception as ex:
        _handle_command_error(ex, manager)


commands = [
    about,
    dump_table,
    dump_table_schema,
    dump_table_schemas,
    dump_table_names,
    dump_table_counts,
    dump_table_count,
    dump_table_rest_schema,
    dump_table_rest_schemas,
    dump_table_json_net_schema,
    dump_table_json_net_schemas,
    query,
    query_from_file,
    query_to_sql,
    schema_to_mysql_ddl,
    schemas_to_mysql_ddl,
]

for command in commands:
    cli.add_command(command)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
