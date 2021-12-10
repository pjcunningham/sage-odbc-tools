# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from click.testing import CliRunner
from sageodbc.cli import cli

# Unit Test


def test_about():
    runner = CliRunner()
    result = runner.invoke(cli, ['about'])

    # assert 'Debug mode is on' in result.output
    assert 'Sage ODBC ... not for accountants' in result.output
    assert result.exit_code == 0
