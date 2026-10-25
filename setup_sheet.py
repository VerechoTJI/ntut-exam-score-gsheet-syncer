"""Inspect first, confirm once, then apply one validated Sheets batch."""
from sheets import get_spreadsheet
from workbook_plan import CLASSROOMS, classroom_plan, combined_plan, final_plan


def encode(value, formula=False):
    key = 'formulaValue' if formula else 'stringValue' if isinstance(value, str) else 'numberValue'
    return {'userEnteredValue': {key: value}}


def build_requests(plans, metadata):
    existing = {s['properties']['title']: s for s in metadata['sheets']}
    used_ids = {s['properties']['sheetId'] for s in metadata['sheets']}
    next_id = max(used_ids, default=0) + 1
    structure = []
    requests = []
    for title, plan in plans.items():
        rows = plan['rows']
        height, width = len(rows), len(rows[0])
        grid = {'rowCount': max(100, height), 'columnCount': max(26, width),
                'frozenRowCount': plan['frozen_rows'], 'frozenColumnCount': plan['frozen_columns']}
        if title in existing:
            sheet = existing[title]
            sid = sheet['properties']['sheetId']
            old_grid = sheet['properties']['gridProperties']
            whole = {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': old_grid['rowCount'],
                     'startColumnIndex': 0, 'endColumnIndex': old_grid['columnCount']}
            structure.extend([
                {'unmergeCells': {'range': whole}},
                {'clearBasicFilter': {'sheetId': sid}},
                {'repeatCell': {'range': whole, 'cell': {},
                                'fields': 'userEnteredValue,userEnteredFormat,note,dataValidation,textFormatRuns'}},
            ])
            for index in reversed(range(len(sheet.get('conditionalFormats', [])))):
                structure.append({'deleteConditionalFormatRule': {'sheetId': sid, 'index': index}})
            structure.append({'updateSheetProperties': {'properties': {'sheetId': sid, 'gridProperties': grid},
                                                        'fields': 'gridProperties'}})
        else:
            sid = next_id
            next_id += 1
            structure.append({'addSheet': {'properties': {'sheetId': sid, 'title': title, 'gridProperties': grid}}})
        area = {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': height,
                'startColumnIndex': 0, 'endColumnIndex': width}
        cell_rows = [{'values': [encode(value, isinstance(value, str) and value.startswith('=')
                                       and r >= plan['frozen_rows'] and c >= plan['frozen_columns'])
                                 for c, value in enumerate(row)]} for r, row in enumerate(rows)]
        requests.extend([
            {'updateCells': {'start': {'sheetId': sid, 'rowIndex': 0, 'columnIndex': 0},
                             'rows': cell_rows, 'fields': 'userEnteredValue'}},
            {'repeatCell': {'range': area, 'cell': {'userEnteredFormat': {
                'textFormat': {'fontFamily': 'Arial', 'fontSize': 11},
                'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE',
                'numberFormat': {'type': 'NUMBER', 'pattern': '0.###'}}}, 'fields': 'userEnteredFormat'}},
            {'repeatCell': {'range': {**area, 'endRowIndex': plan['frozen_rows']},
                'cell': {'userEnteredFormat': {'backgroundColor': {'red': 0.12, 'green': 0.32, 'blue': 0.30},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'wrapStrategy': 'WRAP'}},
                'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat,userEnteredFormat.wrapStrategy'}},
            {'repeatCell': {'range': {**area, 'endColumnIndex': plan['frozen_columns']},
                'cell': {'userEnteredFormat': {'numberFormat': {'type': 'TEXT'}}},
                'fields': 'userEnteredFormat.numberFormat'}},
            {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': width},
                                           'properties': {'pixelSize': 65}, 'fields': 'pixelSize'}},
            {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': plan['frozen_columns']},
                                           'properties': {'pixelSize': 120}, 'fields': 'pixelSize'}},
            {'updateDimensionProperties': {'range': {'sheetId': sid, 'dimension': 'ROWS', 'startIndex': 0, 'endIndex': plan['frozen_rows']},
                                           'properties': {'pixelSize': 40}, 'fields': 'pixelSize'}},
        ])
        for r1, r2, c1, c2 in plan['merges']:
            requests.append({'mergeCells': {'range': {'sheetId': sid, 'startRowIndex': r1, 'endRowIndex': r2,
                                                     'startColumnIndex': c1, 'endColumnIndex': c2}, 'mergeType': 'MERGE_ALL'}})
        if plan['highlight_duplicates']:
            requests.append({'addConditionalFormatRule': {'index': 0, 'rule': {
                'ranges': [{**area, 'startRowIndex': plan['frozen_rows'], 'endRowIndex': grid['rowCount'], 'startColumnIndex': 1}],
                'booleanRule': {'condition': {'type': 'NUMBER_GREATER', 'values': [{'userEnteredValue': '1'}]},
                    'format': {'backgroundColor': {'red': 1, 'green': 0.75, 'blue': 0.75}}}}}})
    # Resizing later can shift previously written cross-sheet references.
    return structure + requests


def setup_workbook(config, roster, url, dry_run=False, input_fn=None):
    plans = {'成績登記': final_plan(config, roster), 'Combined': combined_plan(config, roster)}
    plans.update({title: classroom_plan(config, roster) for title in CLASSROOMS})
    book = get_spreadsheet(url)
    metadata = book.fetch_sheet_metadata()
    existing = {s['properties']['title'] for s in metadata['sheets']}
    print(f'Setup destination: {book.title}\n{url}')
    populated = False
    for title, plan in plans.items():
        values = book.worksheet(title).get_all_values(value_render_option='FORMULA') if title in existing else []
        cells = [value for row in values for value in row if value != '']
        if cells:
            populated = True
            formulas = sum(isinstance(value, str) and value.startswith('=') for value in cells)
            print(f'  {title}: {max(0, len(values)-plan["frozen_rows"])} data rows; {len(cells)} occupied cells; {formulas} formulas')
        else:
            print(f'  {title}: empty/missing; create {len(roster)} student rows')
    if dry_run:
        print('Dry-run: would initialize the five target tabs; no writes performed.')
        return 0
    if populated:
        print('WARNING: Setup replaces target tab contents and formatting. Other tabs stay unchanged.')
        try:
            answer = (input_fn or input)('Continue? [y/N] ')
        except EOFError:
            answer = ''
        if answer.strip().lower() not in ('y', 'yes'):
            print('Setup cancelled; no writes performed.')
            return 0
    requests = build_requests(plans, metadata)
    book.batch_update({'requests': requests})
    print('Setup complete: classroom tabs, Combined, and 成績登記 initialized.')
    return 0
