from __future__ import annotations
# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from enum import Enum
import click
from dataclasses import dataclass
from typing import Optional
import pyodbc


@dataclass
class Table:
    catalog: str
    schema: str
    name: str
    type: str
    other: str

    @classmethod
    def from_row(cls, row: pyodbc.Row) -> Table:
        return cls(*[item for item in row])


@dataclass
class Column:
    catalog: Optional[str]
    schema: Optional[str]
    table: Optional[str]
    name: Optional[str]
    data_type: Optional[int]
    type_name: Optional[str]
    column_size: Optional[int]
    buffer_length: Optional[int]
    decimal_digits: Optional[int]
    num_prec_radix: Optional[int]
    nullable: Optional[bool]
    remarks: Optional[str]

    @classmethod
    def from_row(cls, row: pyodbc.Row) -> Column:
        return cls(*[item for item in row])


class OutputFormatEnum(Enum):
    csv = "csv"
    json = "json"
    xlsx = "xlsx"
    xml = "xml"

    def __str__(self):
        return self.value


class OutputFormatType(click.Choice):
    def __init__(self, enum: Enum, case_sensitive=False):
        self.__enum = enum
        super().__init__(choices=[item.value for item in enum], case_sensitive=case_sensitive)

    def convert(self, value, param, ctx):
        if value is None or isinstance(value, Enum):
            return value

        converted_str = super().convert(value, param, ctx)
        return self.__enum(converted_str)
