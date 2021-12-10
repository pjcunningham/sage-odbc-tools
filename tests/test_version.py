# coding: utf-8
__author__ = 'Paul Cunningham'
__copyright = 'Copyright 2021, Paul Cunningham'

from click.testing import CliRunner
from sageodbc.cli import cli

# Unit Test


def test_version_1():
    runner = CliRunner()
    result = runner.invoke(cli, ['--version', 'about'], prog_name='sageodbc.exe')

    assert 'sageodbc.exe v' in result.output
    assert result.exit_code == 0


def test_version_2():
    runner = CliRunner()
    result = runner.invoke(cli, ['-V', 'about'], prog_name='sageodbc.exe')

    assert 'sageodbc.exe v' in result.output
    assert result.exit_code == 0
