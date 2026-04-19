"""Simple test script: ensure virtualenv (prompt), parse sample, and try fetching rows from DB.

Run: python test_db_parse.py

It uses the same DB env vars as the main script (.env) and imports parse_puzzle_results
from `main.py` for verification.
"""

import os
import sys
import pprint
from dotenv import load_dotenv

try:
    import psycopg2
except Exception as e:
    print("psycopg2 is required for DB tests. Install in your environment:")
    print("pip install psycopg2-binary")
    raise

from main import parse_puzzle_results, fetch_scoreboard_rows


def check_venv():
    in_venv = (
        "VIRTUAL_ENV" in os.environ
        or hasattr(sys, "real_prefix")
        or getattr(sys, "base_prefix", sys.prefix) != sys.prefix
    )
    if not in_venv:
        venv_path = os.path.join(os.path.dirname(__file__), ".venv")
        print("Virtualenv does not appear to be active.")
        if os.path.exists(venv_path):
            if os.name == "nt":
                print(
                    f"Found virtualenv at {venv_path}. Activate with: {venv_path}\\Scripts\\Activate.ps1"
                )
            else:
                print(
                    f"Found virtualenv at {venv_path}. Activate with: source {venv_path}/bin/activate"
                )
        resp = input(
            "Press Enter to continue anyway, or type 'exit' to abort: "
        ).strip()
        if resp.lower().startswith("exit"):
            print("Aborting.")
            sys.exit(1)


def test_parse_sample():
    sample = """{'0': {'subtasks': [{'hidden': [{'status': 'AC', 'userOutput': '0', 'expectedOutput': '0', 'time': '5'}], 'visible': [{'status': 'AC', 'userOutput': '1', 'expectedOutput': '1', 'time': '5'}]}], 'specialRuleResults': [{'ruleId': '56974eef-9c03-4b62-8821-82469b77a900', 'passed': False, 'message': 'aaa and bbb or ccc and ddd', 'reason': 'composite(OR): F,F', 'checkedAt': '2026-04-12T20:21:58.359Z'}]}, '1': {'subtasks': [{'hidden': [{'status': 'AC', 'userOutput': '0', 'expectedOutput': '0', 'time': '5'}], 'visible': [{'status': 'AC', 'userOutput': '1', 'expectedOutput': '1', 'time': '4'}]}], 'specialRuleResults': [{'ruleId': 'd0f0ae39-ce6e-405e-b86e-f5886858080c', 'passed': True, 'message': 'Nested loops are forbidden', 'reason': 'no nested loop', 'checkedAt': '2026-04-12T20:21:58.359Z'}]}}"""
    parsed = parse_puzzle_results(sample)
    print("\n=== Sample parse result ===")
    pprint.pprint(parsed)
    assert isinstance(parsed, (dict, list)), "Parsed sample should be dict or list"


def test_db_fetch_and_parse(limit=5):
    # Use main.fetch_scoreboard_rows to ensure explicit columns selection logic is exercised
    try:
        row_dicts, selected_cols = fetch_scoreboard_rows(limit=limit)
    except Exception as e:
        print("\nError fetching rows via fetch_scoreboard_rows():", e)
        return 4

    print("\nSelected columns from fetch_scoreboard_rows():", selected_cols)
    puzzle_col = None
    for c in selected_cols:
        if "puzzle" in c.lower() or "result" in c.lower() or "puzzles" in c.lower():
            puzzle_col = c
            break
    if not puzzle_col:
        print(
            "Could not find puzzle/results column in selected columns:", selected_cols
        )
        return 3

    print(f"Using puzzle/results column: {puzzle_col}")
    for r in row_dicts:
        raw = r.get(puzzle_col)
        parsed = parse_puzzle_results(raw)
        print("\n--- Row id:", r.get("id", "<no id>"))
        print("Raw (truncated):", str(raw)[:200])
        print("Parsed type:", type(parsed))
        pprint.pprint(parsed)

    return 0


def main():
    check_venv()
    test_parse_sample()
    rc = test_db_fetch_and_parse()
    if rc == 0:
        print("\nDB fetch and parse test succeeded.")
    else:
        print("\nDB fetch and parse test returned code", rc)
    return rc


if __name__ == "__main__":
    sys.exit(main())
