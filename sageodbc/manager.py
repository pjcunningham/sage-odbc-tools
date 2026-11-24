# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import os.path as op
import sys
from contextlib import closing
from logging import Logger

from jinja2 import Environment, FileSystemLoader
from stringutils import snake_case

from . import exports
from . import generators
from .database import Database
from .models import OutputFormatEnum, Table


def resource_path(relative_path: str):
    base_path = getattr(sys, '_MEIPASS', op.dirname(op.abspath(__file__)))
    return op.join(base_path, relative_path)


data_type_lookup = generators.data_type_lookup
json_net_type_lookup = generators.json_net_type_lookup
mysql_primary_keys = generators.mysql_primary_keys
mysql_type_lookup = generators.mysql_type_lookup


class Manager(object):

    def __init__(self, logger: Logger, sage_odbc_dsn: str, sage_username: str, sage_password: str, debug: bool = False):
        self.logger = logger
        self.dsn = sage_odbc_dsn
        self.database = Database(sage_odbc_dsn, sage_username, sage_password)
        self.connection_string = self.database.connection_string
        self.debug = debug
        self.env = Environment(loader=FileSystemLoader(resource_path("templates")), autoescape=False)
        self.logger.info(f'Sage DSN:{sage_odbc_dsn}')

    @property
    def connection(self):
        return self.database.connection

    @connection.setter
    def connection(self, connection):
        self.database.connection = connection

    def close(self):
        self.database.close()

    def get_connection(self):
        return self.database.get_connection()

    def _get_tables(self, connection):
        return self.database.get_tables(connection)

    def _get_columns(self, connection, table_name):
        return self.database.get_columns(connection, table_name)

    def _get_table(self, connection, table_name):
        for table in self._get_tables(connection):
            if table.name == table_name:
                return table
        raise ValueError(f'Table {table_name} does not exist.')

    def _internal_table_pandas_datatypes(self, connection, table_name):
        return exports.table_pandas_datatypes(
            self._get_columns(connection, table_name), table_name, self.logger
        )

    def _internal_output_to_pandas(self, dataframe, output_filename, output_format):
        return exports.write_dataframe(dataframe, output_filename, output_format)

    def _internal_dump_table(self, connection, table_name, output_filename, output_format, include_deleted):
        _types = self._internal_table_pandas_datatypes(connection, table_name)
        _dataframe = exports.read_table(
            connection, table_name, include_deleted, _types, self.logger
        )
        self._internal_output_to_pandas(_dataframe, output_filename, output_format)
        self.logger.info(
            f'outputted table {table_name} data to: {output_filename}, '
            f'format: {str(output_format)} '
        )

    def _internal_dump_table_rest_schema(self, connection, table_name, output_filename):
        generators.dump_rest_schema(
            self._get_columns(connection, table_name), table_name, self.dsn,
            self.env, output_filename, self.logger,
        )
        self.logger.info(f'outputted table {table_name} REST schema to: {output_filename} ')

    def _internal_dump_table_json_net_schema(self, connection, table_name: str, namespace: str, output_filename: str):
        generators.dump_json_net_schema(
            self._get_columns(connection, table_name), table_name, namespace,
            self.dsn, self.env, output_filename, self.logger,
        )
        self.logger.info(f'outputted table {table_name} JSON NET schema to: {output_filename} ')

    def _internal_dump_table_schema(self, connection, table_name, output_filename):
        _columns = self._get_columns(connection, table_name)
        with open(output_filename, 'w') as f:
            for _column in _columns:
                f.write(f'{str(_column)}\n')

        self.logger.info(f'outputted table {table_name} schema to: {output_filename} ')

    def dump_table(self, table_name: str, output_filename: str, output_format: OutputFormatEnum, include_deleted: bool):
        _connection = self.get_connection()
        self._get_table(_connection, table_name)
        self._internal_dump_table(_connection, table_name, output_filename, output_format, include_deleted)

    def dump_tables(self, output_directory: str, output_format: OutputFormatEnum, include_deleted: bool):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = op.join(output_directory,
                                       f'{snake_case(_table.name).lower()}.{str(output_format).lower()}')
            self._internal_dump_table(_connection, _table.name, _output_filename, output_format, include_deleted)

    def dump_table_schema(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        self._get_table(_connection, table_name)
        self._internal_dump_table_schema(_connection, table_name, output_filename)

    def dump_table_schemas(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = op.join(output_directory, f'{snake_case(_table.name).lower()}.txt')
            self._internal_dump_table_schema(_connection, _table.name, _output_filename)

    def dump_table_names(self, output_filename: str):
        _connection = self.get_connection()
        with closing(_connection.cursor()) as _cursor:
            _table_rows = [table_name for table_name in _cursor.tables()]
            _table_rows.sort(key=lambda name: name)
            _tables = [Table.from_row(t) for t in _table_rows]
            with open(output_filename, 'w') as f:
                for _table in _tables:
                    f.write(f'{_table.name}\n')
                    self.logger.info(_table.name)

    def dump_table_counts(self, output_filename: str):
        _connection = self.get_connection()
        with closing(_connection.cursor()) as _cursor:
            _table_rows = [table_name for table_name in _cursor.tables()]
            _table_rows.sort(key=lambda name: name)
            _tables = [Table.from_row(t) for t in _table_rows]
            with open(output_filename, 'w') as f:
                for _table in _tables:
                    _count = _cursor.execute(f"select count(*) from {_table.name}").fetchone()
                    _output = f'{_table.name}:{_count[0]}'  # _count is a tuple
                    self.logger.info(_output)
                    f.write(f'{_output}\n')

    def dump_table_count(self, table_name: str):
        _connection = self.get_connection()
        with closing(_connection.cursor()) as _cursor:
            self._get_table(_connection, table_name)
            _count = _cursor.execute(f"select count(*) from {table_name}").fetchone()
            self.logger.info(f'outputted table {table_name} count: {_count[0]}')
            return _count[0]  # _count is a tuple

    def dump_table_rest_schema(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        self._get_table(_connection, table_name)
        self._internal_dump_table_rest_schema(_connection, table_name, output_filename)

    def dump_table_rest_schemas(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = op.join(output_directory, f'{snake_case(_table.name).lower()}.py')
            self._internal_dump_table_rest_schema(_connection, _table.name, _output_filename)

    def query(self, query: str, output_filename: str, output_format: OutputFormatEnum):
        _connection = self.get_connection()
        _dataframe = exports.read_query(_connection, query, self.logger)
        self._internal_output_to_pandas(_dataframe, output_filename, output_format)

    def query_to_sql(self, query: str, table_name: str, output_filename: str):
        _connection = self.get_connection()
        dataframe = exports.read_query(_connection, query, self.logger)
        generators.write_insert_sql(dataframe, table_name, output_filename)

    def dump_table_json_net_schema(self, table_name: str, namespace: str, output_filename: str):
        _connection = self.get_connection()
        self._get_table(_connection, table_name)
        self._internal_dump_table_json_net_schema(_connection, table_name, namespace, output_filename)

    def dump_table_json_net_schemas(self, namespace: str, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = op.join(output_directory, f'{snake_case(_table.name).lower()}.cs')
            self._internal_dump_table_json_net_schema(_connection, _table.name, namespace, _output_filename)

    def _safe_name(self, name):
        return generators.safe_mysql_name(name)

    def _internal_schema_to_mysql_ddl(self, connection, table_name: str, output_filename: str):
        generators.schema_to_mysql_ddl(
            self._get_columns(connection, table_name), table_name, self.dsn,
            self.env, output_filename, self.logger,
        )
        self.logger.info(f'outputted table {table_name} MySQL DDL create to: {output_filename} ')

    def schema_to_mysql_ddl(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        self._get_table(_connection, table_name)
        self._internal_schema_to_mysql_ddl(_connection, table_name, output_filename)

    def schemas_to_mysql_ddl(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = op.join(output_directory, f'{snake_case(_table.name).lower()}.sql')
            self._internal_schema_to_mysql_ddl(_connection, _table.name, _output_filename)

        _schema_load_output_filename = op.join(output_directory, '_mysql_load_schemas.sql')
        generators.write_mysql_schema_loader(
            _tables, output_directory, self.env, _schema_load_output_filename
        )

    def generate_mysql_load_data(self, input_directory: str, output_filename: str, exclude_tables: list[str]):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        generators.generate_mysql_load_data(
            _tables,
            lambda table_name: self._get_columns(_connection, table_name),
            input_directory,
            output_filename,
            exclude_tables,
            self.env,
            self.logger,
        )

        self.logger.info(f'outputted load data file to: {output_filename} ')
