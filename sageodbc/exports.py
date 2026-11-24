import csv

import pandas as pd

from .models import OutputFormatEnum

pandas_type_lookup = {
    1: 'object',
    4: 'int64',
    5: 'int64',
    8: 'float64',
    9: 'datetime64[ns]',
    10: 'object',
    11: 'datetime64[ns]',
    12: 'object',
    65530: 'int64',
    65534: 'int64',
    65535: 'object',
}


def table_pandas_datatypes(columns, table_name, logger):
    types = {}
    for column in columns:
        column_type = pandas_type_lookup.get(column.data_type)
        if not column_type:
            logger.error(
                f'Field type not found for data type: {column.data_type}, '
                f'column: {column.name}, table: {table_name}'
            )
            return
        types[column.name] = column_type

    return types


def write_dataframe(dataframe, output_filename, output_format):
    if output_format == OutputFormatEnum.csv:
        dataframe.to_csv(output_filename, header=True, index=False, quoting=csv.QUOTE_NONNUMERIC)
    elif output_format == OutputFormatEnum.json:
        dataframe.to_json(output_filename, orient='table', index=False, indent=2)
    elif output_format == OutputFormatEnum.xlsx:
        dataframe.to_excel(output_filename, header=True, index=False)
    elif output_format == OutputFormatEnum.xml:
        dataframe.to_xml(output_filename, index=False, parser='etree')
    else:
        raise ValueError(f'Unknown output format: {output_format}')


def read_query(connection, query, logger):
    try:
        logger.info(f'Running query: {query}')
        dataframe = pd.io.sql.read_sql(query, connection)
    except Exception as ex:
        logger.error(ex)
        raise ValueError(f'Invalid query: {query}') from ex

    logger.info(
        f'Query results shape, {dataframe.shape[0]} rows, {dataframe.shape[1]} columns'
    )
    return dataframe


def read_table(connection, table_name, include_deleted, dtype, logger):
    if include_deleted:
        query = f'select * from {table_name}'
    else:
        query = f'select * from {table_name} where RECORD_DELETED = 0'

    try:
        logger.info(f'Running query: {query}')
        dataframe = pd.io.sql.read_sql(query, connection, dtype=dtype)
    except Exception as ex:
        logger.error(ex)
        raise ValueError(f'Invalid query: {query}') from ex

    logger.info(
        f'Query results shape, {dataframe.shape[0]} rows, {dataframe.shape[1]} columns'
    )
    return dataframe
