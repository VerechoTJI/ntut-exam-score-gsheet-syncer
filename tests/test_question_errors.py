from unittest.mock import Mock

import pytest

import main
from question_errors import errored_questions


@pytest.mark.parametrize('test', [
    {'status': 'SE'}, {'statusCode': 'ABORTED'}, {'status': 'CTLE'},
    {'status': 'RE', 'attribution': 'system', 'message': 'execution service unavailable'},
])
def test_only_errored_question_is_preserved(monkeypatch, capsys, test):
    worksheet = Mock()
    worksheet.get_all_values.return_value = [
        ['學號', '1', '', '2', ''], ['', '1', '規則', '1', '規則'],
        ['s1', '1', '1', '', ''], ['s2', '', '', '', ''],
    ]
    row = {'testId': 's1', 'puzzleResults': [
        {'subtasks': [{'visible': [test], 'hidden': []}], 'specialRuleResults': [{'passed': False}]},
        {'subtasks': [['AC']], 'specialRuleResults': [{'passed': True}]},
    ]}
    monkeypatch.setattr(main, 'get_worksheet', lambda *a: worksheet)
    monkeypatch.setattr(main, 'fetch_scoreboard_rows', lambda: ([row], list(row)))
    assert main.sync_once() == 0
    worksheet.batch_update.assert_called_once_with([
        {'range': 'D3', 'values': [[1]]}, {'range': 'E3', 'values': [[1]]}])
    output = capsys.readouterr().out
    assert 'WARNING:' in output and 's1' in output and 'Q1' in output
    row['puzzleResults'][0]['subtasks'] = [['WA']]
    worksheet.batch_update.reset_mock()
    assert main.sync_once() == 0
    assert {'range': 'B3', 'values': [['']]} in worksheet.batch_update.call_args.args[0]


def test_error_detection_and_normal_student_failures():
    assert errored_questions({'Q2': {'status': 'SystemError', 'reason': 'infrastructure failed'}}) == {'1': 'infrastructure failed'}
    assert errored_questions({'Q1': {'subtasks': [['AC', {'status': 'WA'}, {'status': 'CE'}, {'status': 'TLE'}]]}}) == {}
    assert errored_questions('42') == {}
    assert errored_questions({'Q1': {'subtasks': [{'hidden': [{'status': 'SE', 'reason': 'unavailable'}]}]}}) == {'0': 'unavailable'}
