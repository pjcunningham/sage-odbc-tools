# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from click.testing import CliRunner
from sageodbc.cli import cli

# Unit Test


def test_dump_table_unknown():
    runner = CliRunner()
    result = runner.invoke(cli, ['dump-table', 'MADUPTABLE', ])

    assert result.exit_code == 0
