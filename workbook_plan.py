"""Pure workbook rows and formulas, shared by preview, tests and setup execution."""
from exam_config import puzzles
from utils import col_num_to_a1

CLASSROOMS = ('1222', '1223', '335')


def classroom_plan(config, roster):
    first = ['學號']
    second = ['']
    merges = [(0, 2, 0, 1)]
    for index, question in enumerate(puzzles(config), 1):
        start = len(first)
        count = len(question['subtasks'])
        first.extend([str(index)] + ['']*count)
        second.extend(list(range(1, count+1)) + ['規則'])
        merges.append((0, 1, start, len(first)))
    rows = [first, second] + [[r['student_id']] + ['']*(len(first)-1) for r in roster]
    return {'rows': rows, 'merges': merges, 'frozen_rows': 2,
            'frozen_columns': 1, 'highlight_duplicates': False}


def sum_by_id(tab, source_column, target_id, last_row):
    letter = col_num_to_a1(source_column)
    return f"SUMIF('{tab}'!$A$3:$A${last_row},{target_id},'{tab}'!{letter}$3:{letter}${last_row})"


def combined_plan(config, roster):
    plan = classroom_plan(config, roster)
    last = len(roster) + 2
    for row_number, row in enumerate(plan['rows'][2:], 3):
        for column in range(1, len(row)):
            row[column] = '=' + '+'.join(sum_by_id(tab, column+1, f'$A{row_number}', last)
                                        for tab in CLASSROOMS)
    plan['highlight_duplicates'] = True
    return plan
