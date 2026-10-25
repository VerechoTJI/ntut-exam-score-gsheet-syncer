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


def final_plan(config, roster):
    headers = ['班級', '學號', '姓名']
    labels = ['', '', '']
    weights = ['', '', '']
    merges = [(0, 3, i, i+1) for i in range(3)]
    questions = []
    sections = []
    raw_columns = []
    source_column = 2
    question_number = 0
    for section in config['sections']:
        section_questions = []
        for question in section['puzzles']:
            question_number += 1
            start = len(headers)
            count = len(question['subtasks'])
            headers.extend([str(question_number)] + ['']*count)
            labels.extend(list(range(1, count+1)) + ['規則'])
            weights.extend([s['score'] for s in question['subtasks']] + [question.get('multiplier', 0.5)])
            merges.append((0, 1, start, len(headers)))
            questions.append((start, count, source_column))
            section_questions.append((start, count))
            raw_columns.extend(range(start, start+count))
            source_column += count+1
        section_column = len(headers)
        headers.append(section['title'] + '\n得分')
        labels.append('')
        weights.append(section['maxScore'])
        merges.append((0, 2, section_column, section_column+1))
        sections.append((section_column, section_questions))
    for title in ('最後成績', '原始分數'):
        column = len(headers)
        merges.append((0, 3, column, column+1))
        headers.append(title)
        labels.append('')
        weights.append('')
    rows = [headers, labels, weights]
    for number, student in enumerate(roster, 4):
        row = [student['class'], student['student_id'], student['name']] + ['']*(len(headers)-3)
        for start, count, source in questions:
            for offset in range(count):
                target = col_num_to_a1(start+offset+1)
                row[start+offset] = '=' + sum_by_id('Combined', source+offset, f'$B{number}', len(roster)+2) + f'*{target}$3'
            rule_col = col_num_to_a1(start+count+1)
            row[start+count] = '=MAX(' + sum_by_id('Combined', source+count, f'$B{number}', len(roster)+2) + f',{rule_col}$3)'
        for column, members in sections:
            terms = [f'SUM({col_num_to_a1(start+1)}{number}:{col_num_to_a1(start+count)}{number})*{col_num_to_a1(start+count+1)}{number}'
                     for start, count in members]
            row[column] = '=MIN(' + '+'.join(terms) + f',{col_num_to_a1(column+1)}$3)'
        row[-2] = '=ROUND(SUM(' + ','.join(f'{col_num_to_a1(column+1)}{number}' for column, _ in sections) + '),0)'
        row[-1] = '=SUM(' + ','.join(f'{col_num_to_a1(column+1)}{number}' for column in raw_columns) + ')'
        rows.append(row)
    return {'rows': rows, 'merges': merges, 'frozen_rows': 3,
            'frozen_columns': 3, 'highlight_duplicates': False}
