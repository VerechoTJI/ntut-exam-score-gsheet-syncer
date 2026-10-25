import copy
import importlib.util
import json
from pathlib import Path
import runpy

import pytest


def test_gate_rejects_missing_branches_and_unmeasured_files(tmp_path):
    path = Path(__file__).parents[1] / 'scripts' / 'check_coverage.py'
    spec = importlib.util.spec_from_file_location('coverage_gate', path)
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    (tmp_path / 'app.py').write_text('pass\n')
    report = {'meta': {'branch_coverage': True}, 'files': {'app.py': {
        'summary': {'num_statements': 1, 'covered_lines': 1,
                    'num_branches': 2, 'covered_branches': 2},
        'missing_lines': [], 'missing_branches': [],
    }}}
    assert gate.validate(report, tmp_path) == []
    missing = copy.deepcopy(report)
    missing['files']['app.py']['summary']['covered_branches'] = 1
    missing['files']['app.py']['missing_branches'] = [[1, 2]]
    assert gate.validate(missing, tmp_path)
    (tmp_path / 'forgotten.py').write_text('pass\n')
    assert any('forgotten.py' in error for error in gate.validate(report, tmp_path))


def test_gate_rejects_statement_gaps_and_nonbranch_report(tmp_path):
    path = Path(__file__).parents[1] / 'scripts' / 'check_coverage.py'
    spec = importlib.util.spec_from_file_location('coverage_gate_other', path)
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)
    (tmp_path / 'app.py').write_text('pass\n')
    assert gate.validate({'meta': {}, 'files': {}}, tmp_path)
    report = {'meta': {'branch_coverage': True}, 'files': {'app.py': {
        'summary': {'num_statements': 2, 'covered_lines': 1,
                    'num_branches': 0, 'covered_branches': 0},
        'missing_lines': [2], 'missing_branches': [],
    }}}
    assert gate.validate(report, tmp_path)


@pytest.mark.parametrize('missing,expected', [(False, 0), (True, 1)])
def test_gate_command_exit_status(tmp_path, monkeypatch, capsys, missing, expected):
    script = Path(__file__).parents[1] / 'scripts' / 'check_coverage.py'
    (tmp_path / 'app.py').write_text('pass\n')
    report = {'meta': {'branch_coverage': True}, 'files': {'app.py': {
        'summary': {'num_statements': 1, 'covered_lines': 1,
                    'num_branches': 0, 'covered_branches': 0},
        'missing_lines': [], 'missing_branches': [],
    }}}
    if missing:
        report['files'] = {}
    report_file = tmp_path / 'coverage.json'
    report_file.write_text(json.dumps(report))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('sys.argv', [str(script), str(report_file)])
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(script), run_name='__main__')
    assert error.value.code == expected
    assert ('PASS:' in capsys.readouterr().out) == (not missing)
