# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import sys
import logging
from dotenv import dotenv_values
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
    logger.info(f'Output file: {save_to}')
    logger.info(f'Output format: {output_format}')
    try:
        manager.dump_table(table_name, save_to, output_format)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-tables')
@click.argument('output-directory', type=click.Path(exists=True, dir_okay=True, file_okay=False), )
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def dump_table(manager: Manager, output_directory: str, output_format: OutputFormatEnum):
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
    logger.info(f'Output file: {save_to}')
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

    logger.info(f'Output file: {save_to}')
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

    logger.info(f'Output file: {save_to}')
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
    logger.info(f'Output file: {save_to}')
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


cli.add_command(about)
cli.add_command(dump_table)
cli.add_command(dump_table_schema)
cli.add_command(dump_table_schemas)
cli.add_command(dump_table_names)
cli.add_command(dump_table_counts)
cli.add_command(dump_table_count)
cli.add_command(dump_table_rest_schema)
cli.add_command(dump_table_rest_schemas)


def main():
    cli(sys.argv[1:])


if __name__ == "__main__":
    main()
