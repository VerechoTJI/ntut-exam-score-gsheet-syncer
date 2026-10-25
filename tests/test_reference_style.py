"""Measured reference geometry and chart semantics, not generic styling defaults."""
from test_workbook_plan import example_config, example_roster
from workbook_plan import final_plan, combined_plan, classroom_plan
from sheet_style import style_requests, statistics_rows


def test_final_view_preserves_reference_sidebar_spacing():
    plan=final_plan(example_config(),example_roster())
    final=plan['final_column'];raw=plan['raw_column']
    assert raw-final==7
    assert plan['rows'][0][final]=='最後成績'
    assert plan['rows'][0][raw]=='原始分數'
    assert plan['rows'][3][final]=='=ROUND(SUM(I4),0)'
    assert plan['rows'][3][raw]=='=SUM(D4,E4,G4)'
    assert plan['rows'][3][final+1:raw]==['']*6


def test_final_reference_geometry_and_chart():
    plan=final_plan(example_config(),example_roster())
    requests=style_requests('成績登記',plan,9,100)
    dimensions=[r['updateDimensionProperties'] for r in requests if 'updateDimensionProperties' in r]
    widths={}
    for d in dimensions:
        if d['range']['dimension']=='COLUMNS':
            for i in range(d['range']['startIndex'],d['range']['endIndex']):widths[i]=d['properties']['pixelSize']
    assert [widths[i] for i in range(3)]==[59,72,46]
    assert widths[3]==33 and widths[5]==44 and widths[8]==73
    assert widths[plan['final_column']]==82
    assert [widths[i] for i in range(plan['final_column']+1,plan['raw_column']+1)]==[16,16,85,69,90,90,82]
    assert any(d['range']['dimension']=='ROWS' and d['properties']['pixelSize']==32 for d in dimensions)
    chart=next(r['addChart']['chart'] for r in requests if 'addChart' in r)
    assert chart['spec']['basicChart']['chartType']=='LINE'
    assert len(chart['spec']['basicChart']['series'])==2
    assert chart['spec']['title']=='分數區段人數統計和累計人數'
    position=chart['position']['overlayPosition']
    assert (position['widthPixels'],position['heightPixels'])==(338,209)
    assert position['anchorCell']=={'sheetId':9,'rowIndex':22,'columnIndex':plan['final_column']+1}
    conditions=[r['addConditionalFormatRule']['rule']['booleanRule']['condition']['type'] for r in requests if 'addConditionalFormatRule' in r]
    assert 'NUMBER_LESS' in conditions and 'NUMBER_GREATER_THAN_EQ' in conditions and 'NUMBER_EQ' in conditions
    assert 'Arial' not in str(requests) and 'Microsoft JhengHei' in str(requests)


def test_source_style_is_compact_and_combined_hides_zeroes():
    for title in ('1222','1223','335','Combined'):
        plan=combined_plan(example_config(),example_roster()) if title=='Combined' else classroom_plan(example_config(),example_roster())
        requests=style_requests(title,plan,4,999)
        assert 'addChart' not in str(requests)
        assert 'pixelSize\': 86' in str(requests) and 'pixelSize\': 30' in str(requests)
        assert 'NUMBER_EQ' in str(requests) if title=='Combined' else 'NUMBER_EQ' not in str(requests)


def test_statistics_bands_are_complete_and_use_one_roster_range():
    rows=statistics_rows(9,100)
    assert rows[0]==['已離場','','']
    assert rows[2][1]=='=COUNTIFS(J4:J100,">=60",B4:B100,"?*")'
    assert rows[3][1]=='=COUNTIFS(J4:J100,"<60",B4:B100,"?*")'
    assert [r[0] for r in rows[7:]]==['100+','90-99','80-89','70-79','60-69','50-59','40-49','30-39','20-29','10-19','1-9','0']
    assert 'IFERROR' in rows[2][2]
    assert all('J4:J100' in r[1] and 'B4:B100,"?*"' in r[1] for r in rows[7:])
