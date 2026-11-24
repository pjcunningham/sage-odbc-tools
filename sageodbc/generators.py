import keyword
import math
import numbers
import os.path as op
import re
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pandas as pd
from stringutils import pascal_case, snake_case, title_case

from .helpers import mysql_keywords

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

mysql_type_lookup = {
    1: 'varchar',
    4: 'integer',
    5: 'integer',
    8: 'decimal (40, 10)',
    9: 'datetime',
    10: 'datetime',
    11: 'datetime',
    12: 'varchar',
    65530: 'integer',
    65534: 'integer',
    65535: 'text',
}

# Table and column names are the uppercase names in Sage schemas.
# An explicit None means the table has no primary key; an absent table uses its first column.
mysql_primary_keys = {
    'AUDIT_JOURNAL': None,
    'AUDIT_VAT': None,
    'AUDIT_USAGE': ('USAGE_NUMBER',),
    'BANK_STATEMENT_LINE': ('TRAN_NUMBER',),
    'CATEGORY': None,
    'CLEARED_TRAN_RANGE': ('START_TRAN_NUMBER', 'END_TRAN_NUMBER'),
    'COMMUNICATION_ADDRESS': ('ACCOUNT_TYPE', 'ACCOUNT_REF', 'LETTER_TYPE_ID'),
    'DELETED_RECORDS': ('UUID',),
    'FINANCIAL_BUDGET': None,
    'GDN_ITEM': ('GDN_NUMBER', 'ITEM_NUMBER', 'ORDER_NUMBER', 'ORDER_ITEM'),
    'GRN_ITEM': ('GRN_NUMBER', 'ITEM_NUMBER', 'ORDER_NUMBER'),
    'STOCK_COMP': None,
    'INVOICE_ITEM': ('ITEMID',),
    'SOP_ITEM': ('ITEMID',),
    'POP_ITEM': ('ITEMID',),
    'PRICE_LIST': ('PRICING_REF',),
}


def safe_mysql_name(name):
    safe_name = snake_case(str(name).lower())
    if safe_name.upper() in mysql_keywords:
        safe_name = f'{safe_name}_'
    return safe_name


def quote_mysql_identifier(name):
    return f'`{str(name).replace("`", "``")}`'


def quote_mysql_table_name(name):
    parts = str(name).split('.')
    if any(not part for part in parts):
        raise ValueError(f'Invalid MySQL table name: {name}')
    return '.'.join(quote_mysql_identifier(part) for part in parts)


def mysql_string_literal(value):
    escaped = str(value)
    escaped = escaped.replace('\\', '\\\\')
    escaped = escaped.replace('\0', '\\0')
    escaped = escaped.replace('\b', '\\b')
    escaped = escaped.replace('\n', '\\n')
    escaped = escaped.replace('\r', '\\r')
    escaped = escaped.replace('\t', '\\t')
    escaped = escaped.replace('\x1a', '\\Z')
    escaped = escaped.replace("'", "''")
    return f"'{escaped}'"


def _sql_value(value):
    if value is None:
        return 'NULL'

    try:
        if bool(pd.isna(value)):
            return 'NULL'
    except (TypeError, ValueError):
        pass

    if not isinstance(value, (str, bytes, bytearray, datetime, date, time, Decimal)):
        item = getattr(value, 'item', None)
        if callable(item):
            value = item()

    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f'Cannot write non-finite numeric SQL value: {value}')
        return format(value, 'f')
    if isinstance(value, numbers.Real):
        if not math.isfinite(value):
            raise ValueError(f'Cannot write non-finite numeric SQL value: {value}')
        return repr(value)
    if isinstance(value, datetime):
        return mysql_string_literal(value.isoformat(sep=' '))
    if isinstance(value, (date, time)):
        return mysql_string_literal(value.isoformat())
    if isinstance(value, (bytes, bytearray)):
        return f"X'{bytes(value).hex()}'"
    return mysql_string_literal(value)


def write_insert_sql(dataframe, table_name, output_filename):
    if dataframe.empty:
        statement = '-- No rows returned; no INSERT statements generated.\n'
    else:
        if len(dataframe.columns) == 0:
            raise ValueError('Cannot generate INSERT statements without result columns.')
        table_identifier = quote_mysql_table_name(table_name)
        column_identifiers = ', '.join(
            quote_mysql_identifier(column) for column in dataframe.columns
        )
        rows = []
        for row in dataframe.itertuples(index=False, name=None):
            values = ', '.join(_sql_value(value) for value in row)
            rows.append(f'({values})')
        statement = (
            f'insert into {table_identifier} ({column_identifiers}) values '
            f'{", ".join(rows)};\n'
        )

    with open(output_filename, 'w', encoding='utf-8', newline='\n') as output_file:
        output_file.write(statement)


def _single_line_comment(value):
    return re.sub(r'[\r\n\x00-\x1f\x7f]+', ' ', str(value or '')).strip()


def _python_identifier(value):
    identifier = snake_case(str(value).lower())
    identifier = re.sub(r'\W', '_', identifier, flags=re.UNICODE)
    if not identifier or identifier[0].isdigit():
        identifier = f'_{identifier}'
    if keyword.iskeyword(identifier):
        identifier = f'{identifier}_'
    return identifier


def _csharp_identifier(value):
    identifier = pascal_case(title_case(str(value).lower()))
    identifier = re.sub(r'\W', '_', identifier, flags=re.UNICODE)
    if not identifier or identifier[0].isdigit():
        identifier = f'_{identifier}'
    return identifier


def _csharp_string(value):
    replacements = {
        '\\': '\\\\',
        '"': '\\"',
        '\0': '\\0',
        '\a': '\\a',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\t': '\\t',
        '\v': '\\v',
    }
    return ''.join(replacements.get(character, character) for character in str(value or ''))


def _write_template(environment, template_name, context, output_filename, **values):
    template = environment.get_template(template_name)
    with open(output_filename, 'w', encoding='utf-8', newline='\n') as output_file:
        output_file.write(template.render(context=context, **values))


def dump_rest_schema(columns, table_name, dsn, environment, output_filename, logger):
    fields = []
    for column in columns:
        field_type = data_type_lookup.get(column.data_type)
        if not field_type:
            logger.error(
                f'Field type not found for data type: {column.data_type}, '
                f'column: {column.name}, table: {table_name}'
            )
            continue
        fields.append(
            {
                'data': (
                    f'{_python_identifier(column.name)} = {field_type}'
                    '(as_string=True, dump_only=True)'
                ),
                'remarks': _single_line_comment(column.remarks),
            }
        )

    normalized_table = str(table_name).lower()
    context = {
        'now': datetime.now(timezone.utc).isoformat(),
        'dsn': _single_line_comment(dsn),
        'class_name': _csharp_identifier(normalized_table),
        'meta_type': normalized_table,
        'meta_self_view': f'{normalized_table}_view',
        'meta_self_view_many': f'{normalized_table}_view_list',
        'fields': fields,
    }
    _write_template(environment, 'rest-schema.html', context, output_filename)


def dump_json_net_schema(columns, table_name, namespace, dsn, environment, output_filename, logger):
    class_name = _csharp_identifier(table_name)
    fields = []
    for column in columns:
        field_type = json_net_type_lookup.get(column.data_type)
        if not field_type:
            logger.error(
                f'Field type not found for data type: {column.data_type}, '
                f'column: {column.name}, table: {table_name}'
            )
            continue

        name = _csharp_identifier(column.name)
        if name == class_name:
            name = f'{name}_'
        fields.append(
            {
                'type': field_type,
                'name': name,
                'property_name': _csharp_string(column.name),
                'remarks': _single_line_comment(column.remarks),
            }
        )

    fields.sort(key=lambda item: item['name'])
    context = {
        'now': datetime.now(timezone.utc).isoformat(),
        'dsn': _single_line_comment(dsn),
        'class_name': class_name,
        'namespace': namespace,
        'fields': fields,
    }
    _write_template(environment, 'json-net.html', context, output_filename)


def _mysql_fields(columns, table_name, logger):
    fields = []
    primary_keys = []
    configured_primary_keys = mysql_primary_keys.get(table_name, ...)

    for index, column in enumerate(columns):
        field_type = mysql_type_lookup.get(column.data_type)
        if not field_type:
            logger.error(
                f'Field type not found for data type: {column.data_type}, '
                f'column: {column.name}, table: {table_name}'
            )
            continue

        column_name = safe_mysql_name(column.name)
        is_primary_key = False
        if configured_primary_keys is ...:
            is_primary_key = index == 0
        elif configured_primary_keys is not None:
            is_primary_key = column.name in configured_primary_keys
        if is_primary_key:
            primary_keys.append(quote_mysql_identifier(column_name))

        size = column.column_size
        if field_type == 'varchar':
            try:
                size = max(1, int(size or 1))
            except (TypeError, ValueError) as ex:
                raise ValueError(f'Invalid column size for {table_name}.{column.name}: {size}') from ex
        else:
            size = None

        fields.append(
            {
                'is_in_pk': is_primary_key,
                'type': field_type,
                'size': size,
                'name': quote_mysql_identifier(column_name),
                'comment': mysql_string_literal(column.remarks) if column.remarks else None,
            }
        )

    if not fields:
        raise ValueError(f'No supported columns found for table {table_name}.')
    return fields, primary_keys


def schema_to_mysql_ddl(columns, table_name, dsn, environment, output_filename, logger):
    safe_table_name = safe_mysql_name(table_name)
    fields, primary_keys = _mysql_fields(columns, table_name, logger)
    context = {
        'now': datetime.now(timezone.utc).isoformat(),
        'dsn': _single_line_comment(dsn),
        'table_name': quote_mysql_identifier(safe_table_name),
        'fields': fields,
        'primary_keys': primary_keys,
    }
    _write_template(environment, 'mysql-create.html', context, output_filename)


def write_mysql_schema_loader(tables, output_directory, environment, output_filename):
    schema_files = [
        op.join(output_directory, f'{snake_case(table.name).lower()}.sql').replace('\\', '/')
        for table in tables
    ]
    template = environment.get_template('mysql-load-schemas.html')
    with open(output_filename, 'w', encoding='utf-8', newline='\n') as output_file:
        output_file.write(template.render(schema_files=schema_files))


def generate_mysql_load_data(tables, get_columns, input_directory, output_filename, exclude_tables, environment,
                             logger):
    load_tables = []
    for table in tables:
        if table.name in exclude_tables:
            logger.info(f'Table {table} has been excluded.')
            continue

        filename = op.join(input_directory, f'{snake_case(table.name).lower()}.csv').replace('\\', '/')
        fields = []
        variables = []
        for index, column in enumerate(get_columns(table.name), start=1):
            field_type = mysql_type_lookup.get(column.data_type)
            column_name = safe_mysql_name(column.name)
            if field_type in ('datetime', 'decimal (40, 10)'):
                variable = f'@sageodbc_{index}'
                fields.append(variable)
                variables.append(
                    {
                        'column': quote_mysql_identifier(column_name),
                        'variable': variable,
                    }
                )
            else:
                fields.append(quote_mysql_identifier(column_name))

        load_tables.append(
            {
                'path': mysql_string_literal(filename),
                'name': quote_mysql_identifier(safe_mysql_name(table.name)),
                'columns': fields,
                'variables': variables,
            }
        )

    template = environment.get_template('mysql-load-data.html')
    with open(output_filename, 'w', encoding='utf-8', newline='\n') as output_file:
        output_file.write(template.render(tables=load_tables))
