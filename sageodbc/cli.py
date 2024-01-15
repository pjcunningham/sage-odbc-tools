# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import sys
import logging
from dotenv import dotenv_values
from pathlib import Path
import click
import click_log
from version import __version__
from manager import Manager
from models import OutputFormatEnum, OutputFormatType


logger = logging.getLogger(__name__)
click_log.basic_config(logger)

pass_manager = click.make_pass_decorator(Manager)


@click.group()
@click.version_option(__version__, '-V', '--version', message='%(prog)s v%(version)s')
@click_log.simple_verbosity_option(logger, default="WARNING")
@click.option('--env-file', envvar='ENV_FILE')
@click.option('--debug/--no-debug', default=False, envvar='SAGEODBC_DEBUG')
@click.pass_context
def cli(ctx, env_file, debug):
    _values = dotenv_values(env_file)
    ctx.obj = Manager(
        logger,
        _values['SAGE_ODBC_DSN'],
        _values['SAGE_USERNAME'],
        _values['SAGE_PASSWORD'],
        debug
    )
    logger.info(f"Debug mode is {'on' if debug else 'off'}")


@cli.command('about')
def about():
    click.echo('Sage ODBC ... not for accountants')


@cli.command('dump-table')
@click.argument('table-name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def dump_table(manager: Manager, table_name: str, save_to: str, output_format: OutputFormatEnum):
    """Dump a Sage table's data to file using a given format

       TABLE_NAME is the name of the table to dump.

       SAVE_TO is the name of the file to dump to.
    """
    logger.info(f'Table name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        manager.dump_table(table_name, save_to, output_format)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-tables')
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def dump_tables(manager: Manager, output_directory: str, output_format: OutputFormatEnum):
    """Dump all Sage tables data to a directory using a given format

       Each table is saved to an individual file in the OUTPUT_DIRECTORY

       Filename is snake-cased, lower-cased table name

       OUTPUT_DIRECTORY is the name of the directory to dump to.
    """

    logger.info(f'Output directory: {output_directory}')
    logger.info(f'Output format: {output_format}')
    try:
        manager.dump_tables(output_directory, output_format)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-schema')
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@pass_manager
def dump_table_schema(manager: Manager, table_name: str, save_to: str):
    """Dump a Sage table's schema to a text file

       TABLE_NAME is the name of the table to dump.

       SAVE_TO is the name of the file to dump to.
    """

    logger.info(f'Table name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        manager.dump_table_schema(table_name, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-schemas')
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
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
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-names')
@click.argument('save-to', type=click.Path(exists=False), )
@pass_manager
def dump_table_names(manager: Manager, save_to: str):
    """Dump all Sage table names to a text file

       SAVE_TO is the name of the file to dump to.
    """

    logger.info(f'Save to file: {save_to}')
    try:
        manager.dump_table_names(save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-counts')
@click.argument('save-to', type=click.Path(exists=False), )
@pass_manager
def dump_table_counts(manager: Manager, save_to: str):
    """Dump all Sage table counts to a text file

       SAVE_TO is the name of the file to dump to.
    """

    logger.info(f'Save to file: {save_to}')
    try:
        manager.dump_table_counts(save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


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
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-rest-schema')
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@pass_manager
def dump_table_rest_schema(manager: Manager, table_name: str, save_to: str):
    """Create a flask-rest-jsonapi schema file

       TABLE_NAME is the name of the table.

       SAVE_TO is the name of the file to dump to.
    """

    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        manager.dump_table_rest_schema(table_name, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-rest-schemas')
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
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
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-json-net-schema')
@click.argument('table_name', type=str)
@click.argument('namespace', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@pass_manager
def dump_table_json_net_schema(manager: Manager, table_name: str, namespace: str, save_to: str):
    """Create a JSON.NET schema file

       TABLE_NAME is the name of the table.

       NAMESPACE is the namespace of c# class.

       SAVE_TO is the name of the file to dump to.
    """

    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        manager.dump_table_json_net_schema(table_name, namespace, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-json-net-schemas')
@click.argument('namespace', type=str)
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
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
        logger.error(ex, exc_info=manager.debug)


@cli.command('query')
@click.argument('query', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def query(manager: Manager, query: str, save_to: str, output_format: OutputFormatEnum):
    """Execute a Sage query and dump results to file using a given format

        QUERY: the query to execute a

        SAVE_TO is the name of the file to dump to.

    """
    logger.info(f'Query: {query}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        manager.query(query, save_to, output_format)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('query-from-file')
@click.argument('query-file', type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def query_from_file(manager: Manager, query_file: str, save_to: str, output_format: OutputFormatEnum):
    """Execute a Sage query and dump results to file using a given format

        QUERY_FILE: the query file to execute a

        SAVE_TO is the name of the file to dump to.

    """
    logger.info(f'Query File: {query_file}')
    logger.info(f'Save to file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        query_text = Path(query_file).read_text().replace('\n', ' ')
        manager.query(query_text, save_to, output_format)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('query-to-sql')
@click.argument('query', type=str)
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
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
        manager.query_to_sql(query, table_name, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('schema-to-mysql-ddl')
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@pass_manager
def schema_to_mysql_ddl(manager: Manager, table_name: str, save_to: str):
    """Create a MySQL DDL schema file for a table

       TABLE_NAME is the name of the table.

       SAVE_TO is the name of the file to dump to.
    """
    logger.info(f'Table Name: {table_name}')
    logger.info(f'Save to file: {save_to}')
    try:
        manager.schema_to_mysql_ddl(table_name, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('schemas-to-mysql-ddl')
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
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
        logger.error(ex, exc_info=manager.debug)


@cli.command('generate-mysql-load-data')
@click.argument('input-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True), )
@pass_manager
def generate_mysql_load_data(manager: Manager, input_directory: str, save_to: str):
    """Creates a MySQL sql file to load multiple sql insert files

       INPUT_DIRECTORY is the directory containing the insert .sql files

       SAVE_TO is the name of the file to dump to.
    """
    logger.info(f'Input Directory: {input_directory}')
    logger.info(f'Save to file: {save_to}')
    try:
        manager.generate_mysql_load_data(input_directory, save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


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
