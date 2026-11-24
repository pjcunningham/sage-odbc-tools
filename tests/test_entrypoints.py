import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_module(module: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-m', module, *arguments],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_module_help_runs_from_project_root():
    result = run_module('sageodbc.cli', '--help')

    assert result.returncode == 0
    assert 'Usage:' in result.stdout
    assert 'ModuleNotFoundError' not in result.stderr


def test_package_module_runs_from_project_root():
    result = run_module('sageodbc')

    assert result.returncode == 2
    assert 'Usage:' in result.stdout + result.stderr
    assert 'ModuleNotFoundError' not in result.stdout + result.stderr
