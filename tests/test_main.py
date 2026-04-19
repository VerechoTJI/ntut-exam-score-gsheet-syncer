import os
import sys
import json

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
