# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

import os
from datetime import datetime
from logging import Logger
from jinja2 import Environment, FileSystemLoader, select_autoescape
import csv
import pyodbc
import re
from stringutils import snake_case, pascal_case, title_case
import pandas as pd
from models import OutputFormatEnum, Column, Table
from helpers import mysql_keywords


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

# These are table/column names as in SAGE schemas all uppercase
# if not in this list the first column is the primary key, either None or a tuple
mysql_primary_keys = {
    'AUDIT_JOURNAL': None,
    'AUDIT_VAT': None,
    'AUDIT_USAGE': ('USAGE_NUMBER', ),
    'BANK_STATEMENT_LINE': ('TRAN_NUMBER', ),
    'CLEARED_TRAN_RANGE': ('START_TRAN_NUMBER', 'END_TRAN_NUMBER'),
    'GDN_ITEM': ('GDN_NUMBER', 'ITEM_NUMBER', 'ORDER_NUMBER', 'ORDER_ITEM'),
    'GRN_ITEM': ('GRN_NUMBER', 'ITEM_NUMBER', 'ORDER_NUMBER'),
    'STOCK_COMP': None,
    'INVOICE_ITEM': ('ITEMID', ),
    'SOP_ITEM': ('ITEMID', ),
    'POP_ITEM': ('ITEMID', ),
    'PRICE_LIST': ('PRICING_REF', ),
}

mysql_type_lookup = {
    1: 'varchar',
    4: 'integer',
    5: 'integer',
    8: 'decimal',
    9: 'datetime',
    10: 'datetime',
    11: 'datetime',
    12: 'varchar',
    65530: 'integer',
    65534: 'integer',
    65535: 'varchar',
}


pandas_type_lookup = {
    1: 'object',
    4: 'int64',
    5: 'int64',
    8: 'float64',
    9: 'datetime64[ns]',
    10: 'object', # Time
    11: 'datetime64[ns]',
    12: 'object',
    65530: 'int64',
    65534: 'int64',
    65535: 'object',
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

    def _internal_table_pandas_datatypes(self, connection, table_name):
        _columns = self._get_columns(connection, table_name)
        _types = {}
        for _column in _columns:
            _type = pandas_type_lookup.get(_column.data_type)
            if not _type:
                self.logger.error(f'Field type not found for data type: {_column.data_type}, column: {_column.name}, table: {table_name}')
                return
            _types[_column.name] = _type

        return _types

    def _internal_output_to_pandas(self, dataframe, output_filename, output_format):
        if output_format == OutputFormatEnum.csv:
            dataframe.to_csv(output_filename, header=True, index=False, quoting=csv.QUOTE_NONNUMERIC)
        elif output_format == OutputFormatEnum.json:
            dataframe.to_json(output_filename, orient='table', index=False, indent=2)
        elif output_format == OutputFormatEnum.xlsx:
            dataframe.to_excel(output_filename, header=True, index=False)
        elif output_format == OutputFormatEnum.xml:
            dataframe.to_xml(output_filename, index=False)
        else:
            raise Exception(f'Don'f't know how to output {output_format} format')

    def _internal_dump_table(self, connection, table_name, output_filename, output_format, include_deleted):
        _types = self._internal_table_pandas_datatypes(connection, table_name)
        if include_deleted:
            _query = f'select * from {table_name}'
        else:
            _query = f'select * from {table_name} where RECORD_DELETED = 0'
        try:
            self.logger.info(f'Running query: {_query}')
            _df = pd.io.sql.read_sql(_query, connection, dtype=_types)
            self.logger.info(f'Query results shape, {_df.shape[0]} rows, {_df.shape[1]} columns')
        except Exception as ex:
            self.logger.error(ex)
            raise Exception(f'Invalid query: {_query}')
        else:
            self.logger.info(f'Table: {table_name} dataframe shape, {_df.shape[0]} rows, {_df.shape[1]} columns')
            self._internal_output_to_pandas(_df, output_filename, output_format)
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

    def dump_table(self, table_name: str, output_filename: str, output_format: OutputFormatEnum, include_deleted: bool):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_dump_table(_connection, table_name, output_filename, output_format, include_deleted)

    def dump_tables(self, output_directory: str, output_format: OutputFormatEnum, include_deleted: bool):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        for _table in _tables:
            _output_filename = path.join(output_directory, f'{snake_case(_table.name).lower()}.{str(output_format).lower()}')
            self._internal_dump_table(_connection, _table.name, _output_filename, output_format, include_deleted)

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
        _cursor = _connection.cursor()
        try:
            _table_rows = [table_name for table_name in _cursor.tables()]
            _table_rows.sort(key=lambda name: name)
            _tables = [Table.from_row(t) for t in _table_rows]
            for _table in _tables:
                self.logger.info(_table.name)
        finally:
            _cursor.close()

    def dump_table_counts(self, output_filename: str):
        _connection = self.get_connection()
        _cursor = _connection.cursor()
        try:
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
        _cursor = _connection.cursor()
        try:
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

    def _safe_name(self, name):
        _name = snake_case(name.lower())

        if _name.upper() in mysql_keywords:
            _name = f"{_name}_"

        return _name

    def _internal_schema_to_mysql_ddl(self, connection, table_name: str, output_filename: str):

        _table_name = self._safe_name(table_name)

        _columns = self._get_columns(connection, table_name)
        _fields = []
        _primary_keys = []
        for index, _column in enumerate(_columns):
            _is_in_pk = False
            _field_type = mysql_type_lookup.get(_column.data_type)
            if not _field_type:
                self.logger.error(f'Field type not found for data type: {_column.data_type}, column: {_column.name}, table: {table_name}')
                continue

            if _field_type == 'varchar' and _column.column_size > 255:
                _field_type = 'text'

            _column_name = self._safe_name(_column.name)

            _is_pk_field = index == 0

            if table_name in mysql_primary_keys:
                _pk_fields = mysql_primary_keys[table_name]
                if _pk_fields is not None:
                    if _column.name in _pk_fields:
                        _primary_keys.append(_column_name)
                        _is_in_pk = True
            else:
                if index == 0:
                    _primary_keys.append(_column_name)
                    _is_in_pk = True

            _fields.append(
                {
                    'is_in_pk': _is_in_pk,
                    'type': _field_type,
                    'size': _column.column_size,
                    'name': _column_name,
                    'comment': _column.remarks
                }
            )

        _context = {
            'now': datetime.utcnow().isoformat(),
            'dsn': self.dsn,
            'table_name': _table_name,
            'fields': _fields,
            'primary_keys': _primary_keys
        }

        _template = self.env.get_template('mysql-create.html')
        with open(output_filename, 'w') as f:
            f.write(_template.render(context=_context))

        self.logger.info(f'outputted table {table_name} MySQL DDL create to: {output_filename} ')

    def schema_to_mysql_ddl(self, table_name: str, output_filename: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        if table_name not in [t.name for t in _tables]:
            raise Exception(f'Table {table_name} does not exist.')
        self._internal_schema_to_mysql_ddl(_connection, table_name, output_filename)

    def schemas_to_mysql_ddl(self, output_directory: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)
        _schema_files = []
        for _table in _tables:
            _output_filename = os.path.join(output_directory, f'{snake_case(_table.name).lower()}.sql')
            _schema_files.append(_output_filename.replace('\\', '/'))
            self._internal_schema_to_mysql_ddl(_connection, _table.name, _output_filename)

        _schema_load_output_filename = os.path.join(output_directory, '_mysql_load_schemas.sql')
        _template = self.env.get_template('mysql-load-schemas.html')
        with open(_schema_load_output_filename, 'w') as f:
            f.write(_template.render(schema_files=_schema_files))

    def generate_mysql_load_data(self, input_directory: str, output_filename: str):
        _connection = self.get_connection()
        _tables = self._get_tables(_connection)

        tables = []
        for _table in _tables:
            _safe_table_name = self._safe_name(_table.name)
            _load_data_filename = os.path.join(input_directory, f'{snake_case(_table.name).lower()}.csv').replace('\\', '/')

            _fields = []
            _variables = []
            for index, _column in enumerate(self._get_columns(_connection, _table.name)):
                _field_type = mysql_type_lookup.get(_column.data_type)
                _column_name = self._safe_name(_column.name)
                if _field_type in ['datetime', 'decimal']:
                    _variables.append(_column_name)
                    _column_name = f"@{_column_name}"

                _fields.append(_column_name)

            tables.append(
                {
                    'path': _load_data_filename,
                    'name': _safe_table_name,
                    'columns': _fields,
                    'variables': _variables
                }
            )

        _template = self.env.get_template('mysql-load-data.html')
        with open(output_filename, 'w') as f:
            f.write(_template.render(tables=tables))

        self.logger.info(f'outputted load data file to: {output_filename} ')
