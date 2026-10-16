import os
import sys
import json
from unittest.mock import Mock

import pytest

# Ensure project root is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import main
from main import (
    parse_puzzle_results,
    is_test_passed,
    col_num_to_a1,
    build_values_for_row,
)


def test_parse_puzzle_results_json():
    s = json.dumps([{"subtasks": [["AC"]]}])
    parsed = parse_puzzle_results(s)
    assert isinstance(parsed, list)
    assert parsed[0]["subtasks"][0][0] == "AC"


def test_parse_puzzle_results_literal():
    s = "[{'subtasks': [['AC']]}]"
    parsed = parse_puzzle_results(s)
    assert isinstance(parsed, list)


def test_is_test_passed():
    assert is_test_passed({"status": "AC"})
    assert is_test_passed({"passed": True})
    assert is_test_passed("PASS")
    assert is_test_passed(True)
    assert not is_test_passed({"status": "WA"})


def test_col_num_to_a1():
    assert col_num_to_a1(1) == "A"
    assert col_num_to_a1(26) == "Z"
    assert col_num_to_a1(27) == "AA"
    assert col_num_to_a1(52) == "AZ"
    assert col_num_to_a1(703) == "AAA"


def test_build_values_for_row_basic():
    puzzle_col_name = "puzzle_results"
    # one puzzle, one subtask that passes, and rule that passes
    r = {
        puzzle_col_name: json.dumps(
            [{"subtasks": [["AC"]], "specialRuleResults": [{"passed": True}]}]
        )
    }
    mapping = [
        None,
        {"type": "subtask", "pidx": 0, "sidx": 0},
        {"type": "rule", "pidx": 0},
    ]
    values = build_values_for_row(r, puzzle_col_name, mapping, 1, 2)
    assert values == [1, 1]


def test_build_values_for_row_empty():
    puzzle_col_name = "puzzle_results"
    r = {
        puzzle_col_name: json.dumps(
            [{"subtasks": [["WA"]], "specialRuleResults": [{"passed": False}]}]
        )
    }
    mapping = [
        None,
        {"type": "subtask", "pidx": 0, "sidx": 0},
        {"type": "rule", "pidx": 0},
    ]
    values = build_values_for_row(r, puzzle_col_name, mapping, 1, 2)
    assert values == ["", ""]


@pytest.mark.parametrize(
    "current,passed,expected",
    [
        (["1", "1"], True, []),
        (["", "1"], True, [{"range": "B3", "values": [[1]]}]),
        (["1", "1"], False, [
            {"range": "B3", "values": [[""]]},
            {"range": "C3", "values": [[""]]},
        ]),
        ([], False, []),
        ([], True, [
            {"range": "B3", "values": [[1]]},
            {"range": "C3", "values": [[1]]},
        ]),
    ],
)
def test_sync_only_changed_cells(monkeypatch, current, passed, expected):
    sheet_rows = [
        ["學號", "1", "", "最後成績"],
        ["", "1", "規則", ""],
        ["s001", *current],
        ["unknown", "1", "1", "=SUM(B4:C4)"],
        ["", "1", "1"],
    ]
    worksheet = Mock()
    worksheet.get_all_values.return_value = sheet_rows
    row = {"student_id": "s001", "puzzle_results": [{
        "subtasks": [["AC" if passed else "WA"]],
        "specialRuleResults": [{"passed": passed}],
    }]}
    monkeypatch.setattr(main, "load_dotenv", lambda: None)
    monkeypatch.setattr(main, "get_worksheet", lambda *args: worksheet)
    monkeypatch.setattr(main, "fetch_scoreboard_rows", lambda: (
        [row], ["student_id", "puzzle_results"],
    ))

    assert main.sync_once() == 0
    if expected:
        worksheet.batch_update.assert_called_once_with(expected)
    else:
        worksheet.batch_update.assert_not_called()

    # The next run reads the newly written values and must skip the write.
    sheet_rows[2] = ["s001", "1" if passed else "", "1" if passed else "", "=SUM(B3:C3)"]
    worksheet.batch_update.reset_mock()
    assert main.sync_once() == 0
    worksheet.batch_update.assert_not_called()

    sheet_rows[2] = ["s001", "" if passed else "1", "1"]
    assert main.sync_once(dry_run=True) == 0
    worksheet.batch_update.assert_not_called()
