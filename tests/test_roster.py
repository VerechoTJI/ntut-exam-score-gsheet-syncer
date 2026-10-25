"""Use actual XLSX containers, built with stdlib XML/ZIP, for reader tests."""
from xml.sax.saxutils import escape
import zipfile

import pytest

from roster import read_roster, student_id


def make_xlsx(path, tabs):
    ns = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    rel = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    sheets = []
    links = []
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('[Content_Types].xml', '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/></Types>')
        for index, (name, rows) in enumerate(tabs.items(), 1):
            sheets.append(f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>')
            links.append(f'<Relationship Id="rId{index}" Type="{rel}/worksheet" Target="worksheets/sheet{index}.xml"/>')
            xml_rows = []
            for row_index, row in enumerate(rows, 1):
                cells = []
                for col, value in enumerate(row):
                    address = f'{chr(65+col)}{row_index}'
                    if isinstance(value, (int, float)):
                        cells.append(f'<c r="{address}" t="n"><v>{value}</v></c>')
                    elif value is not None:
                        cells.append(f'<c r="{address}" t="inlineStr"><is><t>{escape(value)}</t></is></c>')
                xml_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
            archive.writestr(f'xl/worksheets/sheet{index}.xml', f'<worksheet xmlns="{ns}"><sheetData>{"".join(xml_rows)}</sheetData></worksheet>')
        archive.writestr('xl/workbook.xml', f'<workbook xmlns="{ns}" xmlns:r="{rel}"><sheets>{"".join(sheets)}</sheets></workbook>')
        archive.writestr('xl/_rels/workbook.xml.rels', f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{"".join(links)}</Relationships>')
    return path


def test_roster_preserves_id_order_and_ignores_scores(tmp_path):
    path = make_xlsx(tmp_path/'roster.xlsx', {'Roster': [
        ['姓名', '學號', '班級', 'Q1'], ['Alice', '00123', 'Class A', 99],
        [None, None, None], ['Bob', 115590002, 'Class B', 0]],
        'Notes': [['other'], ['unrelated']]})
    assert read_roster(path) == [
        {'student_id': '00123', 'name': 'Alice', 'class': 'Class A'},
        {'student_id': '115590002', 'name': 'Bob', 'class': 'Class B'}]


def test_ambiguous_sheet_requires_selection(tmp_path):
    rows = [['班級', '姓名', '學號'], ['A', 'Alice', 's1']]
    path = make_xlsx(tmp_path/'roster.xlsx', {'Roster': rows, 'OLD': rows})
    with pytest.raises(ValueError, match='--roster-sheet'):
        read_roster(path)
    assert len(read_roster(path, 'Roster')) == 1
    with pytest.raises(ValueError, match='not found'):
        read_roster(path, 'missing')


@pytest.mark.parametrize('rows', [
    [], [['班級', '姓名']], [['班級', '姓名', '學號']],
    [['班級', '姓名', '學號', '學號'], ['A', 'Alice', 's1', 's2']],
    [['班級', '姓名', '學號'], ['A', 'Alice', 's1'], ['B', 'Bob', 's1']],
    [['班級', '姓名', '學號'], ['A', '', 's1']],
    [['班級', '姓名', '學號'], ['A', 'Alice', None]],
    [['班級', '姓名', '學號'], ['A', 'Alice', 1.5]],
    [['班級', '姓名', '學號'], ['A', 'Alice', '=A1']],
])
def test_roster_validation_errors(tmp_path, rows):
    path = make_xlsx(tmp_path/'bad.xlsx', {'Roster': rows})
    with pytest.raises(ValueError):
        read_roster(path)


def test_numeric_id_formats_and_invalid_workbook(tmp_path):
    from unittest.mock import Mock
    assert student_id(Mock(value=123, number_format='000000')) == '000123'
    assert student_id(Mock(value=123.0, number_format='General')) == '123'
    with pytest.raises(ValueError):
        student_id(Mock(value=True))
    path = tmp_path/'invalid.xlsx'
    path.write_bytes(b'not an Excel file')
    with pytest.raises(zipfile.BadZipFile):
        read_roster(path)
