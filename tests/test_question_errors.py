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


@pytest.mark.parametrize('details,expected', [
    ({'reason': 'engine worker unavailable', 'message': 'Special rule evaluation failed'}, 'engine worker unavailable'),
    ({'message': 'Special rule evaluation failed'}, 'Special rule evaluation failed'),
    ({}, 'Special rule evaluation failed'),
])
def test_client_rule_engine_failure_preserves_concrete_cause(details,expected):
    record = {'ruleId': '__engine_error__', 'passed': False, **details}
    assert errored_questions({'Q1': {'subtasks': [['AC']], 'specialRuleResults': [record]}}) == {'0': expected}
    assert errored_questions({'Q1': {'specialRuleResults': [{'ruleId': 'ordinary_rule', 'passed': False}]}}) == {}


@pytest.mark.parametrize('use_config', [False,True])
def test_client_engine_error_skips_only_affected_students_question(monkeypatch,capsys,use_config):
    from test_workbook_plan import example_config, example_roster
    from workbook_plan import classroom_plan
    config = example_config()
    worksheet = Mock()
    headers = [[str(value) for value in row] for row in classroom_plan(config, example_roster())['rows'][:2]]
    worksheet.get_all_values.return_value = headers + [
        ['001','1','','1','',''], ['002','','','','','1']]
    engine_error = {'ruleId':'__engine_error__','passed':False,
                    'message':'Special rule evaluation failed','reason':'engine worker unavailable'}
    failed_rule = {'ruleId':'ordinary_rule','passed':False}
    rows = [
        {'testId':'001','puzzleResults': {
            'Q1': {'subtasks':[['AC'],['AC']],'specialRuleResults':[engine_error]},
            'Q2': {'subtasks':[['AC']],'specialRuleResults':[]}}},
        {'testId':'002','puzzleResults': {
            'Q1': {'subtasks':[['AC'],['AC']],'specialRuleResults':[]},
            'Q2': {'subtasks':[['AC']],'specialRuleResults':[failed_rule]}}},
    ]
    monkeypatch.setattr(main,'get_worksheet',lambda *a:worksheet)
    monkeypatch.setattr(main,'fetch_scoreboard_rows',lambda:(rows,list(rows[0])))
    assert main.sync_once(config=config if use_config else None) == 0
    worksheet.batch_update.assert_called_once_with([
        {'range':'E3','values':[[1]]}, {'range':'F3','values':[[1]]},
        {'range':'B4','values':[[1]]}, {'range':'C4','values':[[1]]},
        {'range':'D4','values':[[1]]}, {'range':'E4','values':[[1]]},
        {'range':'F4','values':[['']]},
    ])
    output = capsys.readouterr().out
    assert 'WARNING: Student 001' in output and 'engine worker unavailable' in output
    assert 'existing cells preserved' in output
