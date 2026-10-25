import json

import pytest

from exam_config import load_config, question_mapping, normalize_rows, validate_layout


@pytest.fixture
def config():
    return {'sections': [{'id': 's1', 'title': 'First', 'maxScore': 30,
        'puzzles': [{'id': 'custom-b', 'title': 'B', 'score': 10,
                     'subtasks': [{'title': 'One', 'score': 10, 'visible': [], 'hidden': []}]},
                    {'id': 'custom-a', 'title': 'A', 'score': 10,
                     'subtasks': [{'title': 'One', 'score': 10, 'visible': [], 'hidden': []}]}]}]}


def test_config_and_ordered_alias_mapping(tmp_path, config):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps(config))
    loaded = load_config(path)
    assert loaded['sections'][0]['puzzles'][0]['multiplier'] == 0.5
    positions = question_mapping(loaded)
    assert positions == {'custom-b': 0, 'custom-a': 1}
    puzzle = {'subtasks': [{'visible': [{'status': 'AC'}], 'hidden': []}]}
    rows = [{'testId': 's1', 'puzzleResults': {'custom-a': puzzle, 'custom-b': puzzle}}]
    converted = normalize_rows(rows, 'puzzleResults', loaded, positions)
    assert list(converted[0]['puzzleResults']) == ['1', '0']
    rows[0]['puzzleResults'] = json.dumps({'Q1': puzzle, 'Q2': puzzle})
    assert list(normalize_rows(rows, 'puzzleResults', loaded, positions)[0]['puzzleResults']) == ['0', '1']
    validate_layout([{'type': 'student_id'}, {'type': 'subtask', 'pidx': 0, 'sidx': 0},
                     {'type': 'rule', 'pidx': 0}, {'type': 'subtask', 'pidx': 1, 'sidx': 0},
                     {'type': 'rule', 'pidx': 1}], loaded, positions)


def test_explicit_mapping_and_invalid_layout(config, tmp_path):
    path = tmp_path / 'mapping.json'
    path.write_text(json.dumps({'custom-b': 2, 'custom-a': 1}))
    assert question_mapping(config, path) == {'custom-b': 1, 'custom-a': 0}
    path.write_text(json.dumps({'custom-b': 2}))
    with pytest.raises(ValueError, match='duplicate'):
        question_mapping(config, path)
    with pytest.raises(ValueError, match='layout'):
        validate_layout([], config, question_mapping(config))


@pytest.mark.parametrize('change', [
    lambda c: c.clear(),
    lambda c: c.update(sections=[]),
    lambda c: c['sections'][0].update(maxScore=-1),
    lambda c: c['sections'][0]['puzzles'][0].update(id=''),
    lambda c: c['sections'][0]['puzzles'][1].update(id='custom-b'),
    lambda c: c['sections'][0]['puzzles'][0].update(multiplier=2),
    lambda c: c['sections'][0]['puzzles'][0]['subtasks'][0].update(score=True),
])
def test_invalid_config_rejected(change, tmp_path, config):
    change(config)
    path = tmp_path / 'bad.json'
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        load_config(path)


@pytest.mark.parametrize('results', [
    [{'subtasks': []}], {'Q0': {'subtasks': []}}, {'Q3': {'subtasks': []}},
    {'2': {'subtasks': []}}, {'unknown': {'subtasks': []}},
    {'custom-b': {}}, {'custom-b': {'subtasks': []}},
    {'custom-b': {'subtasks': [{}]}, 'Q1': {'subtasks': [{}]}},
])
def test_invalid_result_mapping(results, config):
    with pytest.raises(ValueError):
        normalize_rows([{'p': results}], 'p', config, question_mapping(config))


def test_zero_based_and_list_payloads(config):
    question = {'subtasks': [{}]}
    for raw in ([question, question], {'0': question, '1': question}):
        assert normalize_rows([{'p': raw}], 'p', config, question_mapping(config))[0]['p'] == {
            '0': question, '1': question}
    with pytest.raises(ValueError):
        normalize_rows([{'p': '42'}], 'p', config, question_mapping(config))


@pytest.mark.parametrize('mapping', [[], {'other': 1}, {'custom-a': 0},
                                      {'custom-a': True}, {'custom-a': 3}])
def test_bad_explicit_mapping(mapping, config, tmp_path):
    path = tmp_path / 'mapping.json'
    path.write_text(json.dumps(mapping))
    with pytest.raises(ValueError):
        question_mapping(config, path)


def test_sync_and_cli_config_mapping(monkeypatch, tmp_path, config):
    from unittest.mock import Mock
    import main
    question = {'subtasks': [{'visible': [{'status': 'AC'}], 'hidden': []}]}
    row = {'testId': 's1', 'puzzleResults': {'custom-a': question, 'custom-b': question}}
    ws = Mock()
    ws.get_all_values.return_value = [
        ['學號', '1', '', '2', ''], ['', '1', '規則', '1', '規則'], ['s1', '', '', '', '']]
    get = Mock(return_value=ws)
    monkeypatch.setattr(main, 'get_worksheet', get)
    monkeypatch.setattr(main, 'fetch_scoreboard_rows', lambda: ([row], list(row)))
    path = tmp_path / 'config.json'
    path.write_text(json.dumps(config))
    mapping = tmp_path / 'mapping.json'
    mapping.write_text(json.dumps({'custom-a': 1, 'custom-b': 2}))
    assert main.cli(['--config', str(path), '--question-map', str(mapping),
                     '--sheet-url', 'https://sheet.test']) == 0
    get.assert_called_once_with('https://sheet.test', None)
    assert ws.batch_update.call_args.args[0] == [
        {'range': 'B3', 'values': [[1]]}, {'range': 'C3', 'values': [[1]]},
        {'range': 'D3', 'values': [[1]]}, {'range': 'E3', 'values': [[1]]}]
    assert main.cli(['--question-map', str(mapping)]) == 1
    monkeypatch.setattr(main, 'fetch_scoreboard_rows', lambda: (
        [{'testId': 's1', 'puzzleAmount': 2}], ['testId', 'puzzleAmount']))
    with pytest.raises(ValueError, match='requires puzzleResults'):
        main.sync_once(config=config)


@pytest.mark.parametrize('change', [
    lambda c: c.update(sections=[None]),
    lambda c: c['sections'][0].update(id=''),
    lambda c: c['sections'][0].update(title=None),
    lambda c: c['sections'][0].update(puzzles=[]),
    lambda c: c['sections'][0].update(puzzles=[None]),
    lambda c: c['sections'][0]['puzzles'][0].update(title=None),
    lambda c: c['sections'][0]['puzzles'][0].update(subtasks=[]),
    lambda c: c['sections'][0]['puzzles'][0].update(subtasks=[None]),
    lambda c: c['sections'][0]['puzzles'][0]['subtasks'][0].update(visible=None),
    lambda c: c['sections'][0]['puzzles'][0]['subtasks'][0].update(hidden=None),
    lambda c: c['sections'][0].update(maxScore=float('inf')),
])
def test_config_schema_boundaries(change, tmp_path, config):
    change(config)
    path = tmp_path / 'invalid.json'
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        load_config(path)
