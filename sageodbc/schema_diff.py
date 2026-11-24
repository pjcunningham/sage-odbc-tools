import ast
from dataclasses import fields
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd

from .models import Column, OutputFormatEnum
from .progress import progress_items


_COLUMN_FIELDS = tuple(field.name for field in fields(Column))


DIFF_COLUMNS = (
    'left_file',
    'right_file',
    'change',
    'left_line',
    'right_line',
    'left_text',
    'right_text',
)


def _file_pairs(left_path, right_path):
    if left_path.is_file() and right_path.is_file():
        return [(left_path, right_path)]

    if left_path.is_dir() and right_path.is_dir():
        left_files = {
            path.name: path
            for path in left_path.iterdir()
            if path.is_file() and path.suffix.lower() == '.txt'
        }
        right_files = {
            path.name: path
            for path in right_path.iterdir()
            if path.is_file() and path.suffix.lower() == '.txt'
        }
        return [
            (left_files[name], right_files[name])
            for name in sorted(left_files.keys() & right_files.keys())
        ]

    raise ValueError('Inputs must both be files or both be directories.')


def _append_change(rows, left_path, right_path, change, left_line=None, right_line=None,
                   left_text=None, right_text=None):
    rows.append({
        'left_file': left_path.name,
        'right_file': right_path.name,
        'change': change,
        'left_line': left_line,
        'right_line': right_line,
        'left_text': left_text,
        'right_text': right_text,
    })


def _compare_file_pair(left_path, right_path, rows):
    with left_path.open('r', encoding='utf-8', newline=None) as input_file:
        left_lines = input_file.read().splitlines()
    with right_path.open('r', encoding='utf-8', newline=None) as input_file:
        right_lines = input_file.read().splitlines()

    matcher = SequenceMatcher(None, left_lines, right_lines, autojunk=False)
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag == 'equal':
            continue

        left_changed = list(enumerate(left_lines[left_start:left_end], start=left_start + 1))
        right_changed = list(enumerate(right_lines[right_start:right_end], start=right_start + 1))
        if tag == 'replace':
            for index in range(max(len(left_changed), len(right_changed))):
                left_item = left_changed[index] if index < len(left_changed) else None
                right_item = right_changed[index] if index < len(right_changed) else None
                change = 'modified' if left_item and right_item else ('removed' if left_item else 'added')
                _append_change(
                    rows,
                    left_path,
                    right_path,
                    change,
                    left_item[0] if left_item else None,
                    right_item[0] if right_item else None,
                    left_item[1] if left_item else None,
                    right_item[1] if right_item else None,
                )
        elif tag == 'delete':
            for left_line, left_text in left_changed:
                _append_change(
                    rows, left_path, right_path, 'removed', left_line, left_text=left_text
                )
        else:
            for right_line, right_text in right_changed:
                _append_change(
                    rows, left_path, right_path, 'added', right_line=right_line,
                    right_text=right_text,
                )


def compare_schema_paths(left_path, right_path, output_path=None, progress=None):
    left_path = Path(left_path)
    right_path = Path(right_path)
    pairs = _file_pairs(left_path, right_path)

    if output_path is not None:
        resolved_output = Path(output_path).resolve()
        for left_file, right_file in pairs:
            if resolved_output in (left_file.resolve(), right_file.resolve()):
                raise ValueError('Output path must not overwrite a compared input file.')

    rows = []
    with progress_items(pairs, progress, 'Comparing schema files') as progress_pairs:
        for left_file, right_file in progress_pairs:
            _compare_file_pair(left_file, right_file, rows)

    return pd.DataFrame(rows, columns=DIFF_COLUMNS).astype({
        'left_line': 'Int64',
        'right_line': 'Int64',
    })


def _parse_column_text(text):
    try:
        expression = ast.parse(text, mode='eval').body
    except SyntaxError:
        return None

    if not isinstance(expression, ast.Call) or not isinstance(expression.func, ast.Name):
        return None
    if expression.func.id != 'Column' or expression.args:
        return None

    values = {}
    for keyword in expression.keywords:
        if keyword.arg not in _COLUMN_FIELDS or keyword.arg in values:
            return None
        try:
            value = ast.literal_eval(keyword.value)
        except (ValueError, TypeError, SyntaxError):
            return None
        if value is not None and not isinstance(value, (str, int, bool)):
            return None
        values[keyword.arg] = value

    if set(values) != set(_COLUMN_FIELDS):
        return None
    return values


def prepare_schema_diff_output(dataframe, output_format):
    output = dataframe.copy()
    for side in ('left', 'right'):
        text_column = f'{side}_text'
        parsed_values = [
            _parse_column_text(value) if isinstance(value, str) else None
            for value in output[text_column]
        ]
        if output_format == OutputFormatEnum.json:
            output[text_column] = [
                parsed if parsed is not None else value
                for value, parsed in zip(output[text_column], parsed_values)
            ]
            continue

        output[text_column] = [
            None if parsed is not None else value
            for value, parsed in zip(output[text_column], parsed_values)
        ]
        for field in _COLUMN_FIELDS:
            output[f'{text_column}_{field}'] = pd.Series(
                [parsed.get(field) if parsed is not None else None for parsed in parsed_values],
                index=output.index,
                dtype='object',
            )

    return output