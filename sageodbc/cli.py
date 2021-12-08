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
@click_log.simple_verbosity_option(logger, default="INFO")
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
@pass_manager
def about(manager):
    logger.info('Sage ODBC ... not for accountants')


@cli.command('dump-table')
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@click.argument('output-format', type=OutputFormatType(OutputFormatEnum))
@pass_manager
def dump_table(manager: Manager, table_name: str, save_to: str, output_format: OutputFormatEnum):
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
    logger.info(f'Output Directory: {output_directory}')
    try:
        manager.dump_table_schemas(output_directory)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-names')
@click.argument('save-to', type=click.Path(exists=False), )
@pass_manager
def dump_table_names(manager: Manager, save_to: str):
    logger.info(f'Output file: {save_to}')
    try:
        manager.dump_table_names(save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-counts')
@click.argument('save-to', type=click.Path(exists=False), )
@pass_manager
def dump_table_counts(manager: Manager, save_to: str):
    logger.info(f'Output file: {save_to}')
    try:
        manager.dump_table_counts(save_to)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-count')
@click.argument('table_name', type=str)
@pass_manager
def dump_table_count(manager: Manager, table_name: str):
    logger.info(f'Table Name: {table_name}')
    try:
        manager.dump_table_count(table_name)
    except Exception as ex:
        logger.error(ex, exc_info=manager.debug)


@cli.command('dump-table-rest-schema')
@click.argument('table_name', type=str)
@click.argument('save-to', type=click.Path(exists=False, dir_okay=False, file_okay=True))
@pass_manager
def dump_table_rest_schema(manager: Manager, table_name: str, save_to: str):
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
