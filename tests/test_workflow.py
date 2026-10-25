"""Exercise setup output through real mapping and synchronization logic."""
from unittest.mock import Mock

import main
from workbook_plan import classroom_plan
from test_workbook_plan import example_config, example_roster


def test_setup_rows_sync_and_error_recovery(monkeypatch, capsys):
    config = example_config()
    roster = example_roster()
    data = classroom_plan(config, roster)['rows']
    worksheet = Mock()
    worksheet.get_all_values.side_effect = lambda: [[str(v) for v in r] for r in data]
    def update(updates):
        for change in updates:
            address = change['range']
            column = 0
            for letter in address.rstrip('0123456789'):
                column = column*26 + ord(letter)-64
            number = int(address.lstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
            data[number-1][column-1] = change['values'][0][0]
    worksheet.batch_update.side_effect = update
    normal = {'subtasks': [{'visible': [{'status':'AC'}], 'hidden': []}], 'specialRuleResults': []}
    first = {'subtasks': [{'visible': [{'status':'AC'}]}, {'hidden': [{'status':'AC'}]}], 'specialRuleResults': []}
    row = {'testId': '001', 'puzzleResults': {'b': first, 'a': normal}}
    monkeypatch.setattr(main, 'get_worksheet', lambda *a: worksheet)
    monkeypatch.setattr(main, 'fetch_scoreboard_rows', lambda: ([row], list(row)))
    assert main.sync_once(config=config) == 0
    assert data[2] == ['001',1,1,1,1,1]
    assert data[3] == ['002','','','','','']
    worksheet.batch_update.reset_mock()
    assert main.sync_once(config=config) == 0
    worksheet.batch_update.assert_not_called()
    # A partial infrastructure failure must not block other questions.
    row['puzzleResults']['b'] = {'subtasks': [{'visible':[{'status':'SE'}]}]}
    row['puzzleResults']['a']['subtasks'][0]['visible'][0]['status'] = 'WA'
    assert main.sync_once(config=config) == 0
    assert data[2] == ['001',1,1,1,'',1]
    assert 'question b:' in capsys.readouterr().out
    row['puzzleResults']['b'] = first
    first['subtasks'][0]['visible'][0]['status'] = 'WA'
    assert main.sync_once(config=config) == 0
    assert data[2] == ['001','',1,1,'',1]


def test_unknown_students_warn_and_dry_run_preserves_sheet(monkeypatch, capsys):
    worksheet = Mock()
    worksheet.get_all_values.return_value = [['學號','1',''],['','1','規則'],['known','','']]
    row = {'testId':'unknown','puzzleResults':[{'subtasks':[['AC']]}]}
    monkeypatch.setattr(main,'get_worksheet',lambda *a:worksheet)
    monkeypatch.setattr(main,'fetch_scoreboard_rows',lambda:([row],list(row)))
    assert main.sync_once(dry_run=True)==0
    worksheet.batch_update.assert_not_called()
    assert 'unknown' in capsys.readouterr().out
