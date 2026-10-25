import pytest

from workbook_plan import classroom_plan, combined_plan, final_plan


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


def test_final_view_weights_caps_and_rounding():
    config = example_config()
    plan = final_plan(config, example_roster())
    assert plan['frozen_rows'] == 3 and plan['frozen_columns'] == 3
    assert plan['rows'][2][3:6] == [6.25, 18.75, 0.5]
    assert plan['rows'][2][8] == 30
    row = plan['rows'][3]
    assert row[:3] == ['A', '001', 'Alice']
    assert row[3] == "=SUMIF('Combined'!$A$3:$A$4,$B4,'Combined'!B$3:B$4)*D$3"
    assert row[5] == "=MAX(SUMIF('Combined'!$A$3:$A$4,$B4,'Combined'!D$3:D$4),F$3)"
    assert row[8] == '=MIN(SUM(D4:E4)*F4+SUM(G4:G4)*H4,I$3)'
    assert row[9] == '=ROUND(SUM(I4),0)'
    assert row[10] == '=SUM(D4,E4,G4)'
    assert len(plan['rows'][0]) == len(row)


def test_multiple_section_caps_above_100():
    config = example_config()
    import copy
    second = copy.deepcopy(config['sections'][0])
    second.update(id='s2', title='Second', maxScore=90)
    config['sections'].append(second)
    plan = final_plan(config, example_roster())
    assert 90 in plan['rows'][2]
    assert plan['rows'][3][-2] == '=ROUND(SUM(I4,O4),0)'
