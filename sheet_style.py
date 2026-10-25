"""Measured style of the user's 115-1 grade workbook, adapted to config geometry.

Reference: spreadsheet 12cAocV3Pp6FU-5aDu_zFSh2Gyh3b2x8V2qWdwx0huCY.
No student records or historical grades are embedded in this profile.
"""
from itertools import groupby

from utils import col_num_to_a1

WHITE = {'red': 1, 'green': 1, 'blue': 1}
BLACK = {'red': 0, 'green': 0, 'blue': 0}
GREY = {'red': 0.6, 'green': 0.6, 'blue': 0.6}
PASS = {'red': 183/255, 'green': 225/255, 'blue': 205/255}
FAIL = {'red': 234/255, 'green': 153/255, 'blue': 153/255}


def statistics_rows(final_column, last_row):
    grade = f'{col_num_to_a1(final_column+1)}4:{col_num_to_a1(final_column+1)}{last_row}'
    ids = f'B4:B{last_row}'
    count = col_num_to_a1(final_column+5)
    cumulative = col_num_to_a1(final_column+6)
    # IDs are stored as text. ?* requires a character, unlike <> which Sheets
    # also matches against explicitly stored empty strings.
    rows = [['已離場', '', ''], ['及格人數統計', '', '百分率'],
            ['及格', f'=COUNTIFS({grade},">=60",{ids},"?*")', f'=IFERROR({count}6/({count}6+{count}7),0)'],
            ['不及格', f'=COUNTIFS({grade},"<60",{ids},"?*")', f'=IFERROR({count}7/({count}6+{count}7),0)'],
            ['', '', ''], ['分數區段人數統計', '', '累計人數'], ['', '', '']]
    bands = [('100+', 100, None)] + [(f'{low}-{low+9}', low, low+10) for low in range(90, 0, -10)]
    bands += [('1-9', 1, 10), ('0', 0, 1)]
    for row, (label, low, high) in enumerate(bands, 11):
        terms = [grade, f'">={low}"']
        if high is not None:
            terms += [grade, f'"<{high}"']
        terms += [ids, '"?*"']
        rows.append([label, '=COUNTIFS('+','.join(terms)+')', f'={count}{row}+{cumulative}{row-1}'])
    return rows


def style_requests(title, plan, sheet_id, row_count):
    requests = []
    final = title == '成績登記'
    width = len(plan['rows'][0])
    def area(r1, r2, c1, c2):
        return {'sheetId': sheet_id, 'startRowIndex': r1, 'endRowIndex': r2,
                'startColumnIndex': c1, 'endColumnIndex': c2}
    def repeat(target, fmt):
        requests.append({'repeatCell': {'range': target, 'cell': {'userEnteredFormat': fmt},
                                        'fields': ','.join('userEnteredFormat.'+k for k in fmt)}})
    def borders(target, **edges):
        requests.append({'updateBorders': {'range': target,
            **{edge: {'style': style, 'color': BLACK} for edge, style in edges.items()}}})
    font = {'fontFamily': 'Microsoft JhengHei', 'fontSize': 14, 'foregroundColor': BLACK, 'bold': False} if final else {'fontSize': 12, 'foregroundColor': BLACK, 'bold': False}
    # Reset all old generic fills, fonts and number formats, preserving values.
    requests.append({'repeatCell': {'range': area(0, row_count, 0, width),
        'cell': {'userEnteredFormat': {'backgroundColor': WHITE, 'textFormat': font,
            'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE' if final else 'BOTTOM'}},
        'fields': 'userEnteredFormat'}})
    requests.append({'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': row_count},
        'properties': {'pixelSize': 32 if final else 21}, 'fields': 'pixelSize'}})
    if final:
        last = plan['final_column']
        raw = plan['raw_column']
        widths = [33]*width
        widths[:3] = [59, 72, 46]
        for start, count, _ in plan['question_groups']:
            widths[start+count] = 44
            borders(area(0, 3, start, start+count+1), left='SOLID', right='SOLID')
        for column in plan['section_columns']:
            widths[column] = 73
            repeat(area(2, 3, column, column+1), {'numberFormat': {'type': 'NUMBER', 'pattern': '"max" 0'}})
            borders(area(0, row_count, column, column+1), left='SOLID', right='SOLID_THICK', innerHorizontal='SOLID')
        widths[last] = 82
        widths[last+1:raw+1] = [16, 16, 85, 69, 90, 90, 82]
        repeat(area(0, 3, 0, last+1), {'textFormat': {**font, 'bold': True}})
        repeat(area(3, row_count, 0, 3), {'textFormat': {'fontFamily': '新細明體, serif', 'fontSize': 10, 'foregroundColor': BLACK},
                                          'horizontalAlignment': 'LEFT', 'verticalAlignment': 'BOTTOM'})
        repeat(area(3, row_count, 1, 2), {'numberFormat': {'type': 'TEXT'}})
        borders(area(0, 3, 0, last+1), top='SOLID_THICK', bottom='SOLID_THICK', left='SOLID_THICK', right='SOLID_THICK')
        borders(area(0, 3, 0, 3), left='SOLID_THICK', right='SOLID_THICK', innerVertical='SOLID')
        borders(area(3, row_count, 0, 3), left='SOLID_THICK', right='SOLID_THICK', bottom='SOLID_THICK')
        borders(area(3, row_count, last, last+1), right='SOLID_THICK', bottom='SOLID_THICK', innerHorizontal='SOLID')
        repeat(area(0, row_count, raw, raw+1), {'textFormat': {**font, 'foregroundColor': GREY}})
        repeat(area(0, 3, raw, raw+1), {'textFormat': {**font, 'foregroundColor': GREY, 'bold': True}})
        stats = last+3
        values = statistics_rows(last, row_count)
        requests.append({'updateCells': {'start': {'sheetId': sheet_id, 'rowIndex': 3, 'columnIndex': stats},
            'rows': [{'values': [{'userEnteredValue': {'formulaValue' if value.startswith('=') else 'stringValue': value}} for value in row]} for row in values],
            'fields': 'userEnteredValue'}})
        for row in (4, 8):
            requests.append({'mergeCells': {'range': area(row, row+1, stats, stats+2), 'mergeType': 'MERGE_ALL'}})
            repeat(area(row, row+1, stats, stats+3), {'textFormat': {**font, 'bold': True}})
        repeat(area(3, 4, stats, stats+1), {'backgroundColor': {'red': 1, 'green': 1, 'blue': 0}})
        repeat(area(5, 7, stats+2, stats+3), {'numberFormat': {'type': 'PERCENT', 'pattern': '0.0%'}})
        for r1, r2 in ((4, 7), (8, 22)):
            borders(area(r1, r2, stats, stats+3), top='SOLID', bottom='SOLID', left='SOLID', right='SOLID', innerHorizontal='SOLID', innerVertical='SOLID')
        chart_range = lambda c: {'sourceRange': {'sources': [area(8, 22, c, c+1)]}}
        requests.append({'addChart': {'chart': {'spec': {
            'title': '分數區段人數統計和累計人數', 'fontName': 'Roboto',
            'titleTextFormat': {'fontFamily': 'Roboto'},
            'hiddenDimensionStrategy': 'SKIP_HIDDEN_ROWS_AND_COLUMNS',
            'basicChart': {'chartType': 'LINE', 'headerCount': 2,
                'axis': [{'position': 'BOTTOM_AXIS', 'title': '分數區段人數統計', 'format': {'fontFamily': 'Roboto'}}, {'position': 'LEFT_AXIS'}],
                'domains': [{'domain': chart_range(stats)}],
                'series': [{'series': chart_range(c), 'targetAxis': 'LEFT_AXIS', 'dataLabel': {'type': 'NONE', 'textFormat': {'fontFamily': 'Roboto'}}} for c in (stats+1, stats+2)]}},
            'position': {'overlayPosition': {'anchorCell': {'sheetId': sheet_id, 'rowIndex': 22, 'columnIndex': last+1},
                'offsetXPixels': 14, 'offsetYPixels': 5, 'widthPixels': 338, 'heightPixels': 209}}}}})
        for kind, value, color in (('NUMBER_LESS', '60', FAIL), ('NUMBER_GREATER_THAN_EQ', '60', PASS)):
            requests.append({'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [area(3, row_count, last, last+1)],
                'booleanRule': {'condition': {'type': kind, 'values': [{'userEnteredValue': value}]}, 'format': {'backgroundColor': color}}}}})
        zero_ranges = [area(0, row_count, start, start+count+1) for start, count, _ in plan['question_groups']]
    else:
        widths = [86]+[30]*(width-1)
        repeat(area(0, 2, 0, 1), {'verticalAlignment': 'MIDDLE'})
        repeat(area(2, row_count, 0, 1), {'textFormat': {'fontFamily': '新細明體, serif', 'fontSize': 10, 'foregroundColor': BLACK},
                                          'numberFormat': {'type': 'TEXT'}, 'horizontalAlignment': 'LEFT'})
        borders(area(2, row_count, 0, 1), top='SOLID', bottom='SOLID', left='SOLID', right='SOLID', innerHorizontal='SOLID')
        for r1, r2, c1, c2 in plan['merges'][1:]:
            borders(area(0, row_count, c1, c2), left='SOLID', right='SOLID')
            borders(area(r1, r2, c1, c2), top='SOLID')
        zero_ranges = [area(0, row_count, 1, width)] if title == 'Combined' else []
    if zero_ranges:
        requests.append({'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': zero_ranges,
            'booleanRule': {'condition': {'type': 'NUMBER_EQ', 'values': [{'userEnteredValue': '0'}]},
                            'format': {'textFormat': {'foregroundColor': WHITE}}}}}})
    if plan['highlight_duplicates']:
        requests.append({'addConditionalFormatRule': {'index': 0, 'rule': {'ranges': [area(2, row_count, 1, width)],
            'booleanRule': {'condition': {'type': 'NUMBER_GREATER', 'values': [{'userEnteredValue': '1'}]}, 'format': {'backgroundColor': FAIL}}}}})
    offset = 0
    for size, members in groupby(widths):
        length = sum(1 for _ in members)
        requests.append({'updateDimensionProperties': {'range': {'sheetId': sheet_id, 'dimension': 'COLUMNS', 'startIndex': offset, 'endIndex': offset+length},
            'properties': {'pixelSize': size}, 'fields': 'pixelSize'}})
        offset += length
    return requests
