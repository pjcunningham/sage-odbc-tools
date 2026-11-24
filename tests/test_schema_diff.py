import json
from xml.etree import ElementTree

import pandas as pd
import pytest
from click.testing import CliRunner

from sageodbc.cli import cli


@pytest.mark.parametrize('output_format', ['csv', 'json', 'xlsx', 'xml'])
def test_compare_schema_files_exports_line_differences(tmp_path, output_format):
    left = tmp_path / 'old.txt'
    right = tmp_path / 'new.txt'
    output = tmp_path / f'difference.{output_format}'
    left.write_text('alpha\nsame\nremoved\nold\nstay\n', encoding='utf-8')
    right.write_text('alpha\nsame\nnew\nstay\n', encoding='utf-8')

    result = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--format', output_format, '--output', str(output)],
    )

    assert result.exit_code == 0, result.output
    assert output.exists()
    if output_format == 'csv':
        dataframe = pd.read_csv(output)
    elif output_format == 'json':
        dataframe = pd.DataFrame(json.loads(output.read_text(encoding='utf-8'))['data'])
    elif output_format == 'xlsx':
        dataframe = pd.read_excel(output)
    else:
        root = ElementTree.fromstring(output.read_text(encoding='utf-8'))
        assert root.tag == 'data'
        dataframe = pd.DataFrame([
            {element.tag: element.text for element in row}
            for row in root.findall('row')
        ])

    dataframe['left_line'] = pd.to_numeric(dataframe['left_line'])
    dataframe['right_line'] = pd.to_numeric(dataframe['right_line'])
    dataframe = dataframe.astype(object).where(pd.notna(dataframe), None)
    assert dataframe['left_file'].tolist() == ['old.txt', 'old.txt']
    assert dataframe['right_file'].tolist() == ['new.txt', 'new.txt']
    assert dataframe['change'].tolist() == ['modified', 'removed']
    assert dataframe['left_line'].tolist() == [3, 4]
    assert dataframe['right_line'].tolist() == [3, None]
    assert dataframe['left_text'].tolist() == ['removed', 'old']
    assert dataframe['right_text'].tolist() == ['new', None]


def test_compare_schemas_files_defaults_to_csv_stdout_without_odbc_configuration(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    left.write_text('before\n', encoding='utf-8')
    right.write_text('after\n', encoding='utf-8')

    result = CliRunner().invoke(cli, ['compare-schemas', str(left), str(right)])

    assert result.exit_code == 0, result.output
    assert 'modified' in result.output
    assert 'before' in result.output
    assert 'after' in result.output


def test_compare_schema_directories_matches_common_txt_files_in_sorted_order(tmp_path):
    left = tmp_path / 'left'
    right = tmp_path / 'right'
    left.mkdir()
    right.mkdir()
    (left / 'z.txt').write_text('old z\n', encoding='utf-8')
    (right / 'z.txt').write_text('new z\n', encoding='utf-8')
    (left / 'a.txt').write_text('old a\n', encoding='utf-8')
    (right / 'a.txt').write_text('new a\n', encoding='utf-8')
    (left / 'left-only.txt').write_text('ignored\n', encoding='utf-8')
    (right / 'right-only.txt').write_text('ignored\n', encoding='utf-8')
    (left / 'not-schema.sql').write_text('ignored\n', encoding='utf-8')
    (right / 'not-schema.sql').write_text('ignored\n', encoding='utf-8')

    result = CliRunner().invoke(cli, ['compare-schemas', str(left), str(right)])

    assert result.exit_code == 0, result.output
    assert result.output.index('a.txt') < result.output.index('z.txt')
    assert 'left-only.txt' not in result.output
    assert 'right-only.txt' not in result.output
    assert 'not-schema.sql' not in result.output


@pytest.mark.parametrize('output_format', ['csv', 'json', 'xlsx', 'xml'])
def test_compare_identical_files_writes_empty_differences(tmp_path, output_format):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    output = tmp_path / f'empty.{output_format}'
    left.write_text('same\ncontent\n', encoding='utf-8')
    right.write_text('same\ncontent\n', encoding='utf-8')

    result = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--format', output_format, '--output', str(output)],
    )

    assert result.exit_code == 0, result.output
    if output_format == 'csv':
        assert output.read_text(encoding='utf-8').strip().startswith('"left_file"')
    elif output_format == 'json':
        assert json.loads(output.read_text(encoding='utf-8'))['data'] == []
    elif output_format == 'xlsx':
        assert pd.read_excel(output).empty
    else:
        root = ElementTree.fromstring(output.read_text(encoding='utf-8'))
        assert root.tag == 'data'
        assert list(root) == []


def test_compare_schema_directories_with_no_common_txt_files_writes_headers(tmp_path):
    left = tmp_path / 'left'
    right = tmp_path / 'right'
    left.mkdir()
    right.mkdir()
    (left / 'left.txt').write_text('left\n', encoding='utf-8')
    (right / 'right.txt').write_text('right\n', encoding='utf-8')

    result = CliRunner().invoke(cli, ['compare-schemas', str(left), str(right)])

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        '"left_file","right_file","change","left_line","right_line","left_text","right_text"'
    )


def test_compare_schema_files_normalizes_line_endings_and_preserves_unicode(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    left.write_bytes('café\r\nunchanged\r\n'.encode('utf-8'))
    right.write_bytes('café\nunchanged\n'.encode('utf-8'))

    result = CliRunner().invoke(cli, ['compare-schemas', str(left), str(right)])

    assert result.exit_code == 0, result.output
    assert result.output.count('left_line') == 1
    assert 'café' not in result.output


def test_compare_schema_files_handles_duplicate_lines_in_order(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    output = tmp_path / 'difference.json'
    left.write_text('before\nrepeat\nrepeat\nafter\n', encoding='utf-8')
    right.write_text('before\nrepeat\nnew\nrepeat\nafter\n', encoding='utf-8')

    result = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--format', 'json', '--output', str(output)],
    )

    assert result.exit_code == 0, result.output
    rows = json.loads(output.read_text(encoding='utf-8'))['data']
    assert len(rows) == 1
    assert rows[0]['change'] == 'added'
    assert rows[0]['right_line'] == 3
    assert rows[0]['right_text'] == 'new'


def test_compare_schemas_rejects_mixed_inputs_and_protects_input_from_output(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    directory = tmp_path / 'schemas'
    left.write_text('left\n', encoding='utf-8')
    right.write_text('right\n', encoding='utf-8')
    directory.mkdir()

    mixed = CliRunner().invoke(cli, ['compare-schemas', str(left), str(directory)])
    overwrite = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--output', str(left)],
    )

    assert mixed.exit_code != 0
    assert 'both be files or both be directories' in mixed.output
    assert overwrite.exit_code != 0
    assert 'must not overwrite' in overwrite.output
    assert left.read_text(encoding='utf-8') == 'left\n'


def test_compare_schemas_rejects_xlsx_stdout(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    left.write_text('left\n', encoding='utf-8')
    right.write_text('right\n', encoding='utf-8')

    result = CliRunner().invoke(cli, ['compare-schemas', str(left), str(right), '--format', 'xlsx'])

    assert result.exit_code != 0
    assert 'XLSX output requires a file path' in result.output


def _column_line(name, remarks):
    return (
        'Column(catalog=None, schema=None, table=\'STOCK\', name='
        f'{name!r}, data_type=12, type_name=\'VARCHAR\', column_size=37, '
        f'buffer_length=37, decimal_digits=0, num_prec_radix=10, nullable=0, '
        f'remarks={remarks!r})'
    )


@pytest.mark.parametrize('output_format', ['csv', 'json', 'xlsx', 'xml'])
def test_compare_schema_column_values_are_structured_per_output_format(tmp_path, output_format):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    output = tmp_path / f'difference.{output_format}'
    left.write_text(
        '\n'.join([
            _column_line('UNCHANGED', 'same'),
            _column_line('REMOVED', 'old'),
            'anchor',
            _column_line('BEFORE', "old 'value'\nline"),
            'tail',
        ]) + '\n',
        encoding='utf-8',
    )
    right.write_text(
        '\n'.join([
            _column_line('UNCHANGED', 'same'),
            'anchor',
            _column_line('AFTER', "new 'value'\nline"),
            _column_line('ADDED', 'new'),
            'tail',
        ]) + '\n',
        encoding='utf-8',
    )

    result = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--format', output_format, '--output', str(output)],
    )

    assert result.exit_code == 0, result.output
    if output_format == 'json':
        rows = json.loads(output.read_text(encoding='utf-8'))['data']
        assert [row['change'] for row in rows] == ['removed', 'modified', 'added']
        assert rows[0]['left_text']['name'] == 'REMOVED'
        assert rows[0]['left_text']['nullable'] == 0
        assert rows[0]['right_text'] is None
        assert rows[1]['left_text']['remarks'] == "old 'value'\nline"
        assert rows[1]['right_text']['remarks'] == "new 'value'\nline"
        assert rows[2]['left_text'] is None
        assert rows[2]['right_text']['name'] == 'ADDED'
    else:
        if output_format == 'csv':
            dataframe = pd.read_csv(output)
        elif output_format == 'xlsx':
            dataframe = pd.read_excel(output)
        else:
            root = ElementTree.fromstring(output.read_text(encoding='utf-8'))
            dataframe = pd.DataFrame([
                {element.tag: element.text for element in row}
                for row in root.findall('row')
            ])
        assert dataframe['change'].tolist() == ['removed', 'modified', 'added']
        assert dataframe.loc[0, 'left_text_name'] == 'REMOVED'
        assert dataframe.loc[0, 'left_text_nullable'] == ('0' if output_format == 'xml' else 0)
        assert pd.isna(dataframe.loc[0, 'right_text_name'])
        assert dataframe.loc[1, 'left_text_remarks'] == "old 'value'\nline"
        assert dataframe.loc[1, 'right_text_remarks'] == "new 'value'\nline"
        assert pd.isna(dataframe.loc[2, 'left_text_name'])
        assert dataframe.loc[2, 'right_text_name'] == 'ADDED'


def test_compare_schema_keeps_unparsed_text_and_never_evaluates_it(tmp_path):
    left = tmp_path / 'left.txt'
    right = tmp_path / 'right.txt'
    output = tmp_path / 'difference.json'
    marker = tmp_path / 'executed'
    malicious = (
        "Column(catalog=None, schema=None, table='T', name='BAD', data_type="
        f"__import__('pathlib').Path({str(marker)!r}).touch())"
    )
    left.write_text('plain old text\n', encoding='utf-8')
    right.write_text(f'plain new text\n{malicious}\n', encoding='utf-8')

    result = CliRunner().invoke(
        cli,
        ['compare-schemas', str(left), str(right), '--format', 'json', '--output', str(output)],
    )

    assert result.exit_code == 0, result.output
    rows = json.loads(output.read_text(encoding='utf-8'))['data']
    assert rows[0]['left_text'] == 'plain old text'
    assert rows[0]['right_text'] == 'plain new text'
    assert rows[1]['right_text'] == malicious
    assert not marker.exists()