# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import os.path
from datetime import datetime
from logging import Logger
from jinja2 import Environment, FileSystemLoader, select_autoescape
import pyodbc
import re
from stringutils import snake_case, pascal_case, title_case
import pandas as pd
from models import OutputFormatEnum, Column, Table


data_type_lookup = {
    1: 'fields.Str',
    4: 'fields.Integer',
    5: 'fields.Integer',
    8: 'fields.Double',
    9: 'fields.Date',
    10: 'fields.Time',
    11: 'fields.DateTime',
    12: 'fields.Str',
    65530: 'fields.Bool',
    65534: 'fields.Integer',
    65535: 'fields.Str',
}


json_net_type_lookup = {
    1: 'string',
    4: 'int',
    5: 'int',
    8: 'decimal',
    9: 'DateTime',
    10: 'DateTime',
    11: 'DateTime',
    12: 'string',
    65530: 'int',
    65534: 'int',
    65535: 'string',
}


class Manager(object):

    def __init__(self, logger: Logger, sage_odbc_dsn: str, sage_username: str, sage_password: str, debug: bool = False):
        self.connection = None
        self.logger = logger
        self.dsn = sage_odbc_dsn
        self.connection_string = f'DSN={sage_odbc_dsn};UID={sage_username};PWD={sage_password}'
        self.debug = debug
        self.env = Environment(loader=FileSystemLoader("templates"), autoescape=select_autoescape())
        self.logger.info(f'Sage DSN:{sage_odbc_dsn}')
        self.logger.info(f'Sage Username:{sage_username}')

    def get_connection(self):
        if self.connection:
            return self.connection

        _connection = None
        try:
            self.connection = pyodbc.connect(self.connection_string)
            return self.connection
        except Exception as ex:
            self.logger.exception(ex, exc_info=self.debug)

    def _get_tables(self, connection):
        try:
            _cursor = connection.cursor()
            _table_rows = [table_name for table_name in _cursor.tables()]
            return [Table.from_row(t) for t in _table_rows]
        finally:
            _cursor.close()

    def _get_columns(self, connection, table_name):
        try:
            _cursor = connection.cursor()
            _columns = [Column.from_row(row) for row in _cursor.columns(table=table_name)]
            return _columns
        finally:
            _cursor.close()

    def _internal_query(self, connection, query, output_filename, output_format):

        try:
            self.logger.info(f'Running query: {query}')
            _df = pd.io.sql.read_sql(query, connection)
            self.logger.info(f'Query results shape, {_df.shape[0]} rows, {_df.shape[1]} columns')
        except Exception as ex:
            self.logger.error(ex)
            raise Exception(f'Invalid query: {query}')
        else:
            if output_format == OutputFormatEnum.csv:
                _df.to_csv(output_filename, header=True, index=False)
            elif output_format == OutputFormatEnum.json:
                _df.to_json(output_filename, orient='table', index=False, indent=2)
            elif output_format == OutputFormatEnum.xlsx:
                _df.to_excel(output_filename, header=True, index=False)
            elif output_format == OutputFormatEnum.xml:
                _df.to_xml(output_filename, index=False)
            else:
                raise Exception(f'Don'f't know how to output {output_format} format')

    def _internal_dump_table(self, connection, table_name, output_filename, output_format):
        _query = f'select * from {table_name}'
        self._internal_query(connection, _query, output_filename, output_format)
        self.logger.info(f'outputted table {table_name} data to: {output_filename}, format: {str(output_format)} ')

    def _internal_dump_table_rest_schema(self, connection, table_name, output_filename):

        _columns = self._get_columns(connection, table_name)
        _fields = []
        for _column in _columns:
            _field = data_type_lookup.get(_column.data_type)
            if not _field:
                self.logger.error(f'Field type not found for data type: {_column.data_type}, column: {_column.name}, table: {table_name}')
                continue
            _fields.append(
                {
                    'data': f'{snake_case(_column.name).lower()} = {data_type_lookup.get(_column.data_type)}(as_string=True, dump_only=True)',
                    'remarks': _column.remarks
                }
            )

        table_name = table_name.lower()

        _context = {
            'now': datetime.utcnow().isoformat(),
            'dsn': self.dsn,
            'class_name': pascal_case(table_name),
            'meta_type': f'{table_name}',
            'meta_self_view': f'{table_name}_view',
            'meta_self_view_many': f'{table_name}_view_list',
            'fields': _fields,
        }

        _template = self.env.get_template('rest-schema.html')
        with open(output_filename, 'w') as f:
            f.write(_template.render(context=_context))

        self.logger.info(f'outputted table {table_name} REST schema to: {output_filename} ')

    def _internal_dump_table_json_net_schema(self, connection, table_name: str, namespace: str, output_filename: str):

        _class_name = pascal_case(title_case(table_name.lower()))

        _columns = self._get_columns(connection, table_name)
        _fields = []
        for _column in _columns:
            _field_type = json_net_type_lookup.get(_column.data_type)
            if not _field_type:
                self.logger.error(f'Field type not found for data type: {_column.data_type}, column: {_column.name}, table: {table_name}')
                continue

            _name = pascal_case(title_case(_column.name.lower()))

            # c# make sure field name doesn't clash with class name
            if _name == _class_name:
                _name = f"{_name}_"

            _fields.append(
                {
                    'type': _field_type,
                    'name': _name,
                    'property_name': _column.name,
                    'remarks': _column.remarks
                }
            )

        _fields.sort(key=lambda item: item['name'])

        table_name = table_name.lower()

        _context = {
            'now': datetime.utcnow().isoformat(),
            'dsn': self.dsn,
            'class_name': _class_name,
            'namespace': namespace,
            'fields': _fields,
        }

        _template = self.env.get_template('json-net.html')
        with open(output_filename, 'w') as f:
            f.write(_template.render(context=_context))

        self.logger.info(f'outputted table {table_name} JSON NET schema to: {output_filename} ')

    def _internal_dump_table_schema(self, connection, table_name, output_filename):
        try:
            _cursor = connection.cursor()
            _columns = [Column.from_row(row) for row in _cursor.columns(table=table_name)]
            with open(output_filename, 'w') as f:
                for _column in _columns:
                    f.write(f'{str(_column)}\n')
        finally:
            _cursor.close()

        self.logger.info(f'outputted table {table_name} schema to: {output_filename} ')

    def dump_table(self, table_name: str, output_filename: str, output_format: OutputFormatEnum):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_dump_table(_connection, table_name, output_filename, output_format)

    def dump_tables(self, output_directory: str, output_format: OutputFormatEnum):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = os.path.join(output_directory, f'{snake_case(_table.name).lower()}.{str(output_format).lower()}')
            self._internal_dump_table(_connection, _table.name, _output_filename, output_format)

    def dump_table_schema(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_dump_table_schema(_connection, table_name, output_filename)

    def dump_table_schemas(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = os.path.join(output_directory, f'{snake_case(_table.name).lower()}.txt')
            self._internal_dump_table_schema(_connection, _table.name, _output_filename)

    def dump_table_names(self, output_filename: str):
        _connection = self.get_connection()
        try:
            _cursor = _connection.cursor()
            _table_rows = [table_name for table_name in _cursor.tables()]
            _table_rows.sort(key=lambda name: name)
            _tables = [Table.from_row(t) for t in _table_rows]
            for _table in _tables:
                self.logger.info(_table.name)
        finally:
            _cursor.close()

    def dump_table_counts(self, output_filename: str):
        _connection = self.get_connection()
        try:
            _cursor = _connection.cursor()
            _table_rows = [table_name for table_name in _cursor.tables()]
            _table_rows.sort(key=lambda name: name)
            _tables = [Table.from_row(t) for t in _table_rows]
            with open(output_filename, 'w') as f:
                for _table in _tables:
                    _count = _cursor.execute(f"select count(*) from {_table.name}").fetchone()
                    _output = f'{_table.name}:{_count[0]}'  # _count is a tuple
                    self.logger.info(_output)
                    f.write(f'{_output}\n')
        finally:
            _cursor.close()

    def dump_table_count(self, table_name: str):
        _connection = self.get_connection()
        try:
            _cursor = _connection.cursor()
            _tables = self._get_tables(_connection)
            if table_name not in [t.name for t in _tables]:
                raise Exception(f'Table {table_name} does not exist.')
            _count = _cursor.execute(f"select count(*) from {table_name}").fetchone()
            self.logger.info(f'outputted table {table_name} count: {_count[0]}')
            return _count[0]  # _count is a tuple
        finally:
            _cursor.close()

    def dump_table_rest_schema(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_dump_table_rest_schema(_connection, table_name, output_filename)

    def dump_table_rest_schemas(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = os.path.join(output_directory, f'{snake_case(_table.name).lower()}.py')
            self._internal_dump_table_rest_schema(_connection, _table.name, _output_filename)

    def query(self, query: str, output_filename: str, output_format: OutputFormatEnum):
        _connection = self.get_connection()
        self._internal_query(_connection, query, output_filename, output_format)

    def query_to_sql(self, query: str, table_name: str, output_filename: str):
        _connection = self.get_connection()
        try:
            self.logger.info(f'Running query: {query}')
            _df = pd.io.sql.read_sql(query, _connection)
            self.logger.info(f'Query results shape, {_df.shape[0]} rows, {_df.shape[1]} columns')
        except Exception as ex:
            self.logger.error(ex)
            raise Exception(f'Invalid query: {query}')
        else:
            cols = ', '.join(_df.columns.to_list())
            vals = []

            for index, r in _df.iterrows():
                row = []
                for x in r:
                    row.append(f"'{str(x)}'")

                row_str = ', '.join(row)
                vals.append(row_str)

            f_values = []
            for v in vals:
                f_values.append(f'({v})')

            # Handle inputting NULL values
            f_values = ', '.join(f_values)
            f_values = re.sub(r"('None')", "NULL", f_values)

            sql = f"insert into {table_name} ({cols}) values {f_values};"

            with open(output_filename, "w") as f:
                f.write(sql)

    def dump_table_json_net_schema(self, table_name: str, namespace: str, output_filename: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_dump_table_json_net_schema(_connection, table_name, namespace, output_filename)

    def dump_table_json_net_schemas(self, namespace: str, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = os.path.join(output_directory, f'{snake_case(_table.name).lower()}.cs')
            self._internal_dump_table_json_net_schema(_connection, _table.name, namespace,  _output_filename)
