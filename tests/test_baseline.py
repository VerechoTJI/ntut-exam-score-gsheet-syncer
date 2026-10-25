"""Behavior coverage for the original syncer, without network or credentials."""
import json
from pathlib import Path
import runpy
from unittest.mock import Mock

import pytest
import requests

import db
import main
import mapping
import sheets
import utils


@pytest.mark.parametrize('raw,expected', [
    (None, {}), ([], []), ((1,), (1,)), (12, {}), ('not-json', {}),
    ('[1]', [1]), ({'Q1': {}, 'bad': {}, 'Qbad': {}}, {'0': {}, 'bad': {}, 'Qbad': {}}),
])
def test_parser_inputs(raw, expected):
    assert utils.parse_puzzle_results(raw) == expected


@pytest.mark.parametrize('value,expected', [(False, False), (0, False),
    ('WA', False), ({'status': 'WA', 'passed': 'true'}, True)])
def test_pass_markers(value, expected):
    assert utils.is_test_passed(value) is expected


def test_column_helpers():
    assert utils.pick_column(['Student_ID'], ['student_id']) == 'Student_ID'
    assert utils.pick_column(['other'], ['student_id']) is None
    assert mapping.find_student_row([' s1 ', 's2'], 's2 ') == 2
    assert mapping.find_student_row(['s1'], 's2') is None


def test_header_validation_and_grouping():
    ws = Mock()
    ws.get_all_values.return_value = [['學號']]
    with pytest.raises(RuntimeError, match='two header'):
        mapping.build_sheet_mapping(ws)
    ws.get_all_values.return_value = [
        ['', 'Student ID', '1', '', '原始分數'],
        ['', '', '1', 'rule', '', 'extra'],
    ]
    result = mapping.build_sheet_mapping(ws)
    assert result[2] == 1
    assert result[-2:] == (2, 3)
    assert result[0][0]['pidx'] is None
    assert result[0][3] == {'type': 'rule', 'pidx': 0}
    ws.get_all_values.return_value = [['', '1'], ['學號', '1']]
    assert mapping.build_sheet_mapping(ws)[2] == 0
    ws.get_all_values.return_value = [['1'], ['1']]
    assert mapping.build_sheet_mapping(ws)[2] is None


@pytest.mark.parametrize('puzzles,expected', [
    ({'0': {'subtasks': [{'tests': ['AC'], 'cases': ['WA']}]}}, ['', 1]),
    ({'0': {'subtasks': [42]}}, ['', 1]),
    ({'0': {'subtasks': [['AC']]}}, [1, 1]),
    ({'0': 42}, ['', '']), ([], ['', '']),
    ([{'subtasks': [], 'specialRuleResults': [{'passed': False}]}], ['', '']),
    (12, ['', '']),
])
def test_row_shapes(puzzles, expected):
    cols = [{'type': 'subtask', 'pidx': 0, 'sidx': 0}, {'type': 'rule', 'pidx': 0}]
    assert mapping.build_values_for_row({'p': puzzles}, 'p', cols, 0, 1) == expected


def test_unmapped_cells_and_missing_results():
    cols = [None, {'type': 'unknown'}, {'type': 'final'}, {'type': 'student_id'}]
    assert mapping.build_values_for_row({}, None, cols, 0, 3) == ['', '', '', '']
    cols = [{'type': 'rule', 'pidx': 1}]
    assert mapping.build_values_for_row({'p': []}, 'p', cols, 0, 0) == ['']


def test_malformed_rule_reports_failure():
    class BrokenRule(dict):
        def get(self, *args):
            raise ValueError('invalid rule')
    cols = [{'type': 'rule', 'pidx': 0}]
    row = {'p': [{'specialRuleResults': [BrokenRule()]}]}
    assert mapping.build_values_for_row(row, 'p', cols, 0, 0) == ['']


@pytest.mark.parametrize('limit,expected', [(None, 2), ('1', 1), ('bad', 2)])
def test_fetch_authenticated_rows(monkeypatch, limit, expected):
    monkeypatch.setenv('BACKEND_API_URL', 'http://backend')
    monkeypatch.setenv('ADMIN_TOKEN', 'test-token')
    response = Mock()
    response.json.return_value = [{'testId': 's1'}, {'testId': 's2'}]
    get = Mock(return_value=response)
    monkeypatch.setattr(db.requests, 'get', get)
    rows, columns = db.fetch_scoreboard_rows(limit)
    assert len(rows) == expected
    assert columns == ['testId']
    assert get.call_args.kwargs['headers']['Authorization'] == 'Bearer test-token'


def test_fetch_empty_and_failure(monkeypatch, capsys):
    assert db.fetch_scoreboard_rows() == ([], [])
    monkeypatch.setenv('BACKEND_API_URL', 'http://backend')
    monkeypatch.setenv('ADMIN_TOKEN', 'test-token')
    response = Mock()
    response.json.return_value = []
    monkeypatch.setattr(db.requests, 'get', Mock(return_value=response))
    assert db.fetch_scoreboard_rows() == ([], [])
    monkeypatch.setattr(db.requests, 'get', Mock(side_effect=requests.ConnectionError('offline')))
    assert db.fetch_scoreboard_rows() == ([], [])
    assert 'offline' in capsys.readouterr().out


def test_get_worksheet(monkeypatch):
    credentials = Mock()
    client = Mock()
    monkeypatch.setattr(sheets.Credentials, 'from_service_account_file', credentials)
    monkeypatch.setattr(sheets, 'AuthorizedSession', Mock())
    monkeypatch.setattr(sheets.gspread, 'authorize', Mock(return_value=client))
    assert sheets.get_worksheet('url', 'tab') is client.open_by_url.return_value.worksheet.return_value
    client.open_by_url.assert_called_once_with('url')
    client.open_by_url.return_value.worksheet.assert_called_once_with('tab')
    assert credentials.call_args.args == ('credentials.json',)


def configure_sync(monkeypatch, rows, sheet_rows=None, columns=None):
    ws = Mock()
    ws.get_all_values.return_value = sheet_rows or [
        ['學號', '1', '', '最後成績'], ['', '1', '規則', ''], ['s1', '', ''],
    ]
    monkeypatch.setattr(main, 'get_worksheet', lambda *args: ws)
    monkeypatch.setattr(main, 'fetch_scoreboard_rows', lambda: (rows, columns if columns is not None else list(rows[0])))
    return ws


@pytest.mark.parametrize('row,code', [
    ({'testId': 's1'}, 1), ({'puzzleResults': []}, 1),
    ({'testId': 's1', 'puzzleResults': {}}, 1),
    ({'testId': 's1', 'puzzleResults': '42'}, 1),
    ({'testId': 's1', 'puzzleAmount': 'bad'}, 1),
    ({'testId': 's1', 'puzzleAmount': 1}, 0),
    ({'testId': 's1', 'puzzleResults': {'bad': {}, '0': {'subtasks': []}}}, 0),
    ({'testId': 's1', 'puzzleResults': {'0': 42}}, 0),
    ({'testId': 's1', 'puzzleResults': {'0': {'subtask': 42}}}, 0),
    ({'testId': 's1', 'puzzleResults': [42]}, 0),
    ({'testId': '', 'puzzleResults': [{'subtasks': []}]}, 0),
])
def test_sync_validation_paths(monkeypatch, row, code):
    configure_sync(monkeypatch, [row])
    assert main.sync_once() == code


def test_sync_empty_api(monkeypatch):
    configure_sync(monkeypatch, [], columns=[])
    assert main.sync_once() == 1


def test_sync_missing_student_header(monkeypatch):
    configure_sync(monkeypatch, [{'testId': 's1', 'puzzleResults': [{}]}], [['1'], ['1']])
    assert main.sync_once() == 1


def test_sync_missing_sheet_question_and_batch_failure(monkeypatch, capsys):
    row = {'testId': 's1', 'puzzleResults': [
        {'subtasks': [['AC']]}, {'subtasks': [['AC']]},
    ]}
    ws = configure_sync(monkeypatch, [row])
    ws.batch_update.side_effect = RuntimeError('permission denied')
    assert main.sync_once() == 1
    assert 'permission denied' in capsys.readouterr().out
    ws.batch_update.assert_called_once()


def test_sync_short_rows(monkeypatch):
    row = {'testId': 's1', 'puzzleResults': [{'subtasks': [['AC']]}]}
    configure_sync(monkeypatch, [row], [['name', '學號', '1'], ['', '', '1'], [], ['only-name']])
    assert main.sync_once() == 0


@pytest.mark.parametrize('interval', [None, '', '0', '-1', 'invalid', '0.01'])
def test_cli_modes(monkeypatch, interval, capsys):
    import dotenv
    import time
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda: None)
    if interval is not None:
        monkeypatch.setenv('INTERVAL', interval)
    monkeypatch.setattr('sys.argv', ['main.py', '--dry-run'])
    ws = Mock()
    ws.get_all_values.return_value = [['學號', '1'], ['', '1'], ['s1', '']]
    monkeypatch.setattr(sheets, 'get_worksheet', lambda *a: ws)
    row = {'testId': 's1', 'puzzleResults': [{'subtasks': [['AC']]}]}
    monkeypatch.setattr(db, 'fetch_scoreboard_rows', lambda: ([row], list(row)))
    monkeypatch.setattr(time, 'sleep', Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(Path(main.__file__)), run_name='__main__')
    assert error.value.code == 0
    assert 'Dry-run:' in capsys.readouterr().out
    ws.batch_update.assert_not_called()
