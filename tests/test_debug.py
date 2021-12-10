# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from click.testing import CliRunner
from sageodbc.cli import cli

# Unit Test

# Verbosity mode (-v INFO) to get the debug status


def test_debug_on():
    runner = CliRunner()
    result = runner.invoke(cli, ['--debug', '-v', 'INFO', 'about'])

    assert 'Debug mode is on' in result.output
    assert result.exit_code == 0


def test_debug_off():
    runner = CliRunner()
    result = runner.invoke(cli, ['--no-debug', '-v', 'INFO', 'about'])

    assert 'Debug mode is off' in result.output
    assert result.exit_code == 0
