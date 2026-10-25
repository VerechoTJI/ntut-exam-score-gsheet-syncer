import pytest

from workbook_plan import classroom_plan, combined_plan


def example_config():
    return {'sections': [{'id': 's1', 'title': 'First', 'maxScore': 30,
        'puzzles': [{'id': 'b', 'title': 'First question', 'score': 25, 'multiplier': 0.5,
            'subtasks': [{'score': 6.25}, {'score': 18.75}]},
            {'id': 'a', 'title': 'Second question', 'score': 20, 'multiplier': 0.5,
             'subtasks': [{'score': 20}]}]}]}


def example_roster():
    return [{'student_id': '001', 'name': 'Alice', 'class': 'A'},
            {'student_id': '002', 'name': 'Bob', 'class': 'B'}]


def test_classroom_headers_and_roster():
    plan = classroom_plan(example_config(), example_roster())
    assert plan['rows'] == [
        ['學號', '1', '', '', '2', ''], ['', 1, 2, '規則', 1, '規則'],
        ['001', '', '', '', '', ''], ['002', '', '', '', '', '']]
    assert plan['merges'] == [(0, 2, 0, 1), (0, 1, 1, 4), (0, 1, 4, 6)]
    assert plan['frozen_rows'] == 2 and plan['frozen_columns'] == 1


def test_combined_formulas_are_id_matched_sums():
    plan = combined_plan(example_config(), example_roster())
    assert plan['rows'][2][0] == '001'
    assert plan['rows'][2][1] == "=SUMIF('1222'!$A$3:$A$4,$A3,'1222'!B$3:B$4)+SUMIF('1223'!$A$3:$A$4,$A3,'1223'!B$3:B$4)+SUMIF('335'!$A$3:$A$4,$A3,'335'!B$3:B$4)"
    assert '$A4' in plan['rows'][3][-1]
    assert plan['highlight_duplicates'] is True


def test_empty_roster_and_wide_question_groups():
    config = example_config()
    config['sections'][0]['puzzles'][0]['subtasks'] = [{'score': 1}]*30
    plan = combined_plan(config, [])
    assert len(plan['rows']) == 2
    assert len(plan['rows'][0]) == 34
