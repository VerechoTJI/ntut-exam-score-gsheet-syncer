from unittest.mock import Mock

import pytest

import setup_sheet
from test_workbook_plan import example_config, example_roster


def book_with_existing(content=None):
    book = Mock(title='Target exam sheet', url='https://target.test')
    book.fetch_sheet_metadata.return_value = {'sheets': [
        {'properties': {'sheetId': 7, 'title': '1222', 'gridProperties': {'rowCount': 100, 'columnCount': 26}},
         'conditionalFormats': [{}]},
        {'properties': {'sheetId': 8, 'title': 'Unrelated', 'gridProperties': {'rowCount': 100, 'columnCount': 26}}}]}
    book.worksheet.return_value.get_all_values.return_value = content or []
    return book


@pytest.mark.parametrize('answer', ['', 'no', 'N', 'y', 'YES'])
def test_setup_confirmation_precedes_writes(monkeypatch, capsys, answer):
    book = book_with_existing([['學號'], [''], ['=IF(TRUE,"","")']])
    monkeypatch.setattr(setup_sheet, 'get_spreadsheet', lambda *a: book)
    assert setup_sheet.setup_workbook(example_config(), example_roster(), 'https://target.test',
                                     input_fn=lambda prompt: answer) == 0
    assert book.batch_update.called == (answer.lower() in ('y', 'yes'))
    text = capsys.readouterr().out
    assert 'WARNING' in text and 'formulas' in text and 'https://target.test' in text


def test_setup_dry_run_and_eof_never_write(monkeypatch):
    book = book_with_existing([['existing']])
    monkeypatch.setattr(setup_sheet, 'get_spreadsheet', lambda *a: book)
    def no_input(prompt):
        raise EOFError
    assert setup_sheet.setup_workbook(example_config(), example_roster(), 'url', dry_run=True, input_fn=no_input) == 0
    assert setup_sheet.setup_workbook(example_config(), example_roster(), 'url', input_fn=no_input) == 0
    book.batch_update.assert_not_called()


def test_empty_target_setup_preserves_other_tabs(monkeypatch):
    book = book_with_existing()
    monkeypatch.setattr(setup_sheet, 'get_spreadsheet', lambda *a: book)
    assert setup_sheet.setup_workbook(example_config(), example_roster(), 'url') == 0
    requests = book.batch_update.call_args.args[0]['requests']
    assert sum('addSheet' in r for r in requests) == 4
    assert any('deleteConditionalFormatRule' in r for r in requests)
    assert 'Unrelated' not in str(requests)
    assert any('addConditionalFormatRule' in r for r in requests)
    structural = [i for i,r in enumerate(requests) if 'addSheet' in r or 'updateSheetProperties' in r]
    writes = [i for i,r in enumerate(requests) if 'updateCells' in r]
    assert max(structural) < min(writes)


def test_setup_arguments_are_required(monkeypatch, tmp_path):
    import json
    import main
    from test_roster import make_xlsx
    with pytest.raises(SystemExit) as error:
        main.cli(['--setup-sheet'])
    assert error.value.code == 2
    config_path = tmp_path/'config.json'
    config = example_config()
    for q in config['sections'][0]['puzzles']:
        for sub in q['subtasks']:
            sub.update(title='Subtask', visible=[], hidden=[])
    config_path.write_text(json.dumps(config))
    roster_path = make_xlsx(tmp_path/'roster.xlsx', {'Roster': [['班級','姓名','學號'],['A','Alice','001']]})
    setup = Mock(return_value=0)
    monkeypatch.setattr(main, 'setup_workbook', setup)
    assert main.cli(['--setup-sheet','--sheet-url','url','--config',str(config_path),
                     '--roster',str(roster_path),'--roster-sheet','Roster','--dry-run']) == 0
    assert setup.call_args.kwargs['dry_run'] is True
    setup.side_effect = RuntimeError('Google batch rejected')
    assert main.cli(['--setup-sheet','--sheet-url','url','--config',str(config_path),
                     '--roster',str(roster_path),'--roster-sheet','Roster']) == 1
