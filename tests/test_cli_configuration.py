from click.testing import CliRunner

from sageodbc.cli import cli


def test_database_command_reports_missing_configuration(tmp_path):
    env_file = tmp_path / '.env'
    env_file.write_text('SAGE_ODBC_DSN=test\n', encoding='utf-8')

    result = CliRunner().invoke(
        cli,
        [
            '--env-file',
            str(env_file),
            'dump-table',
            'CUSTOMER',
            '--output',
            str(tmp_path / 'out.csv'),
            '--format',
            'csv',
        ],
    )

    assert result.exit_code != 0
    assert 'SAGE_USERNAME, SAGE_PASSWORD' in result.output


def test_connection_failure_is_nonzero_and_redacts_password(tmp_path, monkeypatch):
    env_file = tmp_path / '.env'
    env_file.write_text(
        'SAGE_ODBC_DSN=test\nSAGE_USERNAME=test\nSAGE_PASSWORD=secret\n',
        encoding='utf-8',
    )

    def fail_to_connect(_):
        raise OSError('Driver rejected PWD=secret')

    monkeypatch.setattr('sageodbc.database.pyodbc.connect', fail_to_connect)
    result = CliRunner().invoke(
        cli,
        [
            '--debug',
            '--env-file',
            str(env_file),
            'dump-table',
            'CUSTOMER',
            '--output',
            str(tmp_path / 'out.csv'),
            '--format',
            'csv',
        ],
    )

    assert result.exit_code != 0
    assert 'Unable to connect to Sage ODBC DSN' in result.output
    assert 'secret' not in result.output
