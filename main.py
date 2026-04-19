import os
import json
import ast
import psycopg2
import gspread
import argparse
import sys
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


def parse_puzzle_results(val):
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, (list, tuple)):
        return val
    if isinstance(val, str):
        s = val.strip()
        # try JSON first
        try:
            return json.loads(s)
        except Exception:
            pass
        # fallback to Python literal (single quotes etc.)
        try:
            return ast.literal_eval(s)
        except Exception:
            return {}
    return {}


def find_column_index(colnames, candidates):
    lower = [c.lower() for c in colnames]
    for cand in candidates:
        if cand.lower() in lower:
            return lower.index(cand.lower())
    return None


def is_test_passed(test):
    if isinstance(test, dict):
        status = test.get("status")
        if status in ("AC", "Accepted", "PASS", "Passed", True):
            return True
        passed = test.get("passed")
        if passed in (True, "True", "true", 1, "1"):
            return True
        return False
    if isinstance(test, str):
        return test in ("AC", "Accepted", "PASS", "Passed")
    if isinstance(test, bool):
        return test
    return False


def pick_column(available_cols, candidates):
    # prefer exact match, then case-insensitive exact
    for cand in candidates:
        if cand in available_cols:
            return cand
    lower_map = {c.lower(): c for c in available_cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def fetch_scoreboard_rows(limit=None):
    """Connect to DB and SELECT an explicit set of columns from score_boards.

    Returns: (row_dicts, selected_cols)
    """
    load_dotenv()
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()

        # discover available columns
        cur.execute("SELECT * FROM score_boards LIMIT 0;")
        available_cols = [desc[0] for desc in cur.description]

        # we only need student id and puzzle results (plus optional counts / id)
        student_id_col = pick_column(
            available_cols, ["student_ID", "student_id", "studentId"]
        )
        puzzle_results_col = pick_column(
            available_cols, ["puzzle_results", "puzzleResults", "puzzles", "results"]
        )
        puzzle_amount_col = (
            pick_column(available_cols, ["puzzle_amount"])
            or pick_column(available_cols, ["puzzles_amount"])
            or None
        )
        subtask_amount_col = pick_column(available_cols, ["subtask_amount"])
        id_col = pick_column(available_cols, ["id"])

        selected_cols = []
        for c in (
            student_id_col,
            puzzle_results_col,
            puzzle_amount_col,
            subtask_amount_col,
            id_col,
        ):
            if c and c not in selected_cols:
                selected_cols.append(c)

        if not selected_cols:
            # fallback to all columns
            selected_cols = available_cols

        # safe-quote column names
        def quote(c):
            return '"' + str(c).replace('"', '""') + '"'

        select_clause = ", ".join(quote(c) for c in selected_cols)
        query = f"SELECT {select_clause} FROM score_boards"
        if limit:
            query += f" LIMIT {int(limit)}"

        cur.execute(query)
        rows = cur.fetchall()
        row_dicts = [dict(zip(selected_cols, row)) for row in rows]
        try:
            cur.close()
        except Exception:
            pass
        return row_dicts, selected_cols
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def col_num_to_a1(n):
    """Convert 1-based column index to A1 column letters."""
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s


def build_sheet_mapping(ws):
    """Read first two header rows and build a mapping for each column.

    Returns: (mapping, class_idx, student_idx, name_idx, data_start, last_data)
    """
    rows = ws.get_all_values()
    if len(rows) < 2:
        raise RuntimeError("Sheet must have at least two header rows.")
    h1 = rows[0]
    h2 = rows[1]
    max_len = max(len(h1), len(h2))
    h1 = h1 + [""] * (max_len - len(h1))
    h2 = h2 + [""] * (max_len - len(h2))

    # forward-fill h1 (merged header handling)
    last = ""
    for i in range(max_len):
        if h1[i].strip():
            last = h1[i].strip()
        else:
            h1[i] = last

    mapping = [None] * max_len

    # First mark obvious fixed columns by header content
    for i in range(max_len):
        a = h1[i].strip()
        b = h2[i].strip()
        la = a.lower()
        lb = b.lower()
        if la in ("學號", "student_id", "student id") or lb in ("學號",):
            mapping[i] = {"type": "student_id"}
            continue

        if la in ("最後成績",) or la in ("原始分數",):
            mapping[i] = {"type": "final"}
            continue

    # Group remaining columns by top header value (forward-filled h1)
    groups = {}
    for i in range(max_len):
        if mapping[i] is not None:
            continue
        a = h1[i].strip()
        key = a if a != "" else "__unknown__"
        groups.setdefault(key, []).append(i)

    # For each group, interpret as one puzzle group (if header is numeric)
    for key, cols in groups.items():
        # try to parse puzzle index from key
        pidx = None
        try:
            pidx = int(key) - 1
        except Exception:
            pidx = None

        # Within group, assign subtask columns in order; detect rule columns by second-row label containing '規則' or 'rule'
        subtask_counter = 0
        for i in cols:
            b = h2[i].strip()
            lb = b.lower()
            if "規則" in b or "rule" in lb:
                mapping[i] = {"type": "rule", "pidx": pidx}
            else:
                mapping[i] = {"type": "subtask", "pidx": pidx, "sidx": subtask_counter}
                subtask_counter += 1

    # detect student id column;
    class_idx = None
    student_col_idx = next(
        (i for i, m in enumerate(mapping) if m and m.get("type") == "student_id"), None
    )
    name_idx = None

    # data start is after the student id column
    rightmost = student_col_idx if student_col_idx is not None else -1
    data_start = rightmost + 1

    # find first final column after data_start
    last_data = len(mapping) - 1
    for i in range(data_start, len(mapping)):
        if mapping[i] and mapping[i].get("type") == "final":
            last_data = i - 1
            break

    return mapping, class_idx, student_col_idx, name_idx, data_start, last_data


def find_student_row(sheet_student_col_vals, db_student_id):
    """Return 1-based sheet row number for the given student id, or None."""
    for i, v in enumerate(sheet_student_col_vals):
        if v.strip() == str(db_student_id).strip():
            return i + 1
    return None


def build_values_for_row(r, puzzle_col_name, mapping, data_start_col, last_data_col):
    """Compute the contiguous list of values for data columns for a DB row."""
    values = []
    raw = r.get(puzzle_col_name) if puzzle_col_name else None
    puzzles = parse_puzzle_results(raw)

    for col_idx in range(data_start_col, last_data_col + 1):
        info = mapping[col_idx]
        if not info or info.get("type") in ("unknown", "final"):
            values.append("")
            continue

        if info["type"] == "subtask":
            pidx = info["pidx"]
            sidx = info["sidx"]
            p_entry = None
            if isinstance(puzzles, dict):
                p_entry = puzzles.get(str(pidx)) or puzzles.get(pidx)
            elif isinstance(puzzles, list) and pidx < len(puzzles):
                p_entry = puzzles[pidx]

            if not p_entry:
                values.append("")
                continue

            subtasks = p_entry.get("subtasks") if isinstance(p_entry, dict) else None
            subtasks = subtasks or []
            if sidx < len(subtasks):
                sub = subtasks[sidx]
                tests = []
                if isinstance(sub, dict):
                    for arr_name in ("visible", "hidden", "tests", "cases"):
                        arr = sub.get(arr_name)
                        if isinstance(arr, list):
                            tests.extend(arr)
                elif isinstance(sub, list):
                    tests = sub

                passed = False
                if tests:
                    passed = all(is_test_passed(t) for t in tests)
                values.append(1 if passed else "")
            else:
                values.append("")

        elif info["type"] == "rule":
            pidx = info["pidx"]
            p_entry = None
            if isinstance(puzzles, dict):
                p_entry = puzzles.get(str(pidx)) or puzzles.get(pidx)
            elif isinstance(puzzles, list) and pidx < len(puzzles):
                p_entry = puzzles[pidx]

            rules = (
                p_entry.get("specialRuleResults") if isinstance(p_entry, dict) else []
            )
            rules = rules or []
            if rules:
                try:
                    rule_passed = all(
                        rr.get("passed") in (True, "True", "true", 1, "1")
                        for rr in rules
                        if isinstance(rr, dict)
                    )
                except Exception:
                    rule_passed = False
                values.append(1 if rule_passed else "")
            else:
                values.append("")

        else:
            values.append("")

    return values


def main(dry_run=False):
    load_dotenv()

    # 1. Setup Google Sheets Authentication
    SERVICE_ACCOUNT_FILE = "credentials.json"
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)

    # 2. Connect to the Google Sheet
    SHEET_URL = os.getenv("SHEET_URL")
    spreadsheet = client.open_by_url(SHEET_URL)
    worksheet = spreadsheet.worksheet("1222")

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()

        # 4. Fetch Data using explicit column selection
        row_dicts, selected_cols = fetch_scoreboard_rows()

        # identify puzzle results column name among selected columns
        puzzle_col_name = (
            pick_column(
                selected_cols, ["puzzle_results", "puzzleResults", "puzzles", "results"]
            )
            if selected_cols
            else None
        )

        # --- Check DB-side columns and report missing ones clearly ---
        db_missing_required = []

        db_student_col = (
            pick_column(
                selected_cols, ["student_ID", "student_id", "學號", "studentId"]
            )
            if selected_cols
            else None
        )
        # only require student id and puzzle results (or puzzle_amount)
        db_puzzle_amount_col = (
            pick_column(selected_cols, ["puzzle_amount", "puzzles_amount"])
            if selected_cols
            else None
        )

        if not db_student_col:
            db_missing_required.append("student id (student_ID or student_id)")
        if not (puzzle_col_name or db_puzzle_amount_col):
            db_missing_required.append(
                "puzzle_results (serialized) or puzzle_amount (int)"
            )

        if db_missing_required:
            print("\nDatabase columns check:")
            print("  Missing required columns:")
            for m in db_missing_required:
                print(f"    - {m}")
            print(
                "\nSuggestion: add the missing columns to your `score_boards` table or adjust the query/column names."
            )
            print(
                "You can run: python test_db_parse.py to inspect sample DB rows and columns."
            )
            sys.exit(1)

        # compute global puzzle indices and max subtasks per puzzle
        puzzle_indices = set()
        max_subtasks = {}
        for r in row_dicts:
            raw = r.get(puzzle_col_name) if puzzle_col_name else None
            puzzles = parse_puzzle_results(raw)
            if isinstance(puzzles, dict):
                keys = []
                try:
                    keys = [int(k) for k in puzzles.keys()]
                except Exception:
                    keys = [k for k in puzzles.keys()]
                for k in keys:
                    try:
                        idx = int(k)
                    except Exception:
                        continue
                    puzzle_indices.add(idx)
                    p = puzzles.get(str(k)) or puzzles.get(k) or {}
                    subt = []
                    if isinstance(p, dict):
                        subt = p.get("subtasks") or p.get("subtask") or []
                    if isinstance(subt, list):
                        max_subtasks[idx] = max(max_subtasks.get(idx, 0), len(subt))
            elif isinstance(puzzles, list):
                for idx, p in enumerate(puzzles):
                    puzzle_indices.add(idx)
                    subt = p.get("subtasks") if isinstance(p, dict) else []
                    if isinstance(subt, list):
                        max_subtasks[idx] = max(max_subtasks.get(idx, 0), len(subt))

        if not puzzle_indices:
            # try to use puzzle_amount column if present, otherwise error with suggestions
            amt_col = (
                pick_column(selected_cols, ["puzzle_amount", "puzzles_amount"])
                if selected_cols
                else None
            )
            if amt_col:
                try:
                    max_amt = max(int((r.get(amt_col) or 0)) for r in row_dicts)
                    puzzle_indices = set(range(max_amt))
                except Exception:
                    print(
                        f"Error: could not parse integer counts from column '{amt_col}'."
                    )
                    print(
                        "Suggestion: ensure the column contains integer counts for number of puzzles, or add a 'puzzle_results' column with serialized results."
                    )
                    print(
                        "Run 'python test_db_parse.py' to inspect DB columns and sample values."
                    )
                    sys.exit(1)
            else:
                print(
                    "Error: could not find 'puzzle_results' column nor 'puzzle_amount' in the DB table."
                )
                print(f"Selected columns: {selected_cols}")
                print(
                    "Suggestion: add a 'puzzle_results' column containing serialized results (JSON-like) or a 'puzzle_amount' integer column."
                )
                print(
                    "You can run 'python test_db_parse.py' to inspect DB columns and sample values."
                )
                sys.exit(1)

        sorted_puzzles = sorted(puzzle_indices)
        sorted_puzzles = sorted(puzzle_indices)

        # Use top-level helpers for sheet mapping early so we can limit to sheet-present puzzles
        (
            mapping,
            sheet_class_idx,
            sheet_student_idx,
            sheet_name_idx,
            data_start_col,
            last_data_col,
        ) = build_sheet_mapping(worksheet)

        # Determine which puzzles the sheet actually provides columns for
        sheet_puzzle_idxs = set()
        sheet_subtask_counts = {}
        for i, info in enumerate(mapping):
            if not info:
                continue
            if info.get("type") in ("subtask", "rule") and info.get("pidx") is not None:
                sheet_puzzle_idxs.add(info.get("pidx"))
                if info.get("type") == "subtask":
                    sheet_subtask_counts[info.get("pidx")] = (
                        sheet_subtask_counts.get(info.get("pidx"), 0) + 1
                    )

        # build header rows limited to puzzles present in sheet
        # we only emit the student id column before puzzle data
        header1 = ["學號"]
        header2 = [""]
        final_puzzles = [p for p in sorted_puzzles if p in sheet_puzzle_idxs]
        missing_on_sheet = [p for p in sorted_puzzles if p not in sheet_puzzle_idxs]
        if missing_on_sheet:
            print(
                "Warning: these puzzles exist in DB but not on sheet and will be skipped:"
            )
            print("  ", ", ".join(str(p + 1) for p in missing_on_sheet))

        for pidx in final_puzzles:
            db_subcount = max_subtasks.get(pidx, 0)
            sheet_subcount = sheet_subtask_counts.get(pidx, 0)
            used_subcount = min(db_subcount, sheet_subcount)
            group_size = used_subcount + 1  # +1 for 規則
            header1.extend([str(pidx + 1)] * group_size)
            for s in range(1, used_subcount + 1):
                header2.append(str(s))
            header2.append("規則")

        data_to_write = [header1, header2]

        # --- Check sheet-side columns and report missing required ones clearly ---
        sheet_missing_required = []

        if sheet_student_idx is None:
            sheet_missing_required.append("學號 column (student id)")

        # verify there is at least one data column mapped to subtask/rule
        data_cols_exist = any(
            (mapping[i] and mapping[i].get("type") in ("subtask", "rule"))
            for i in range(
                data_start_col,
                (
                    last_data_col + 1
                    if last_data_col >= data_start_col
                    else data_start_col
                ),
            )
        )
        if not data_cols_exist:
            sheet_missing_required.append(
                "puzzle/subtask columns in the header rows (top two rows)"
            )

        if sheet_missing_required:
            print("\nSheet columns check:")
            print("  Missing required sheet headers:")
            for m in sheet_missing_required:
                print(f"    - {m}")
            print(
                "\nSuggestion: update the Google Sheet headers (first two rows) to include the missing labels, e.g. top row puzzle numbers and second row subtask indices, and a column labeled '學號'."
            )
            print(
                "You can run: python test_db_parse.py --dry-run to inspect DB values and then fix headers accordingly."
            )
            sys.exit(1)

        # --- Verify sheet provides enough columns per puzzle based on DB max_subtasks ---
        # Count sheet columns per puzzle (data area)
        sheet_counts_by_pidx = {}
        for i in range(data_start_col, last_data_col + 1):
            info = mapping[i]
            if info and info.get("type") in ("subtask", "rule"):
                p = info.get("pidx")
                sheet_counts_by_pidx[p] = sheet_counts_by_pidx.get(p, 0) + 1

        puzzle_mismatches = []
        for pidx in sorted_puzzles:
            expected_sub = max_subtasks.get(pidx, 0)
            expected_group = expected_sub + 1
            found = sheet_counts_by_pidx.get(pidx, 0)
            if found < expected_group:
                puzzle_mismatches.append(
                    (pidx + 1, expected_group, found, expected_sub)
                )

        if puzzle_mismatches:
            print("\nSheet puzzle column mismatch detected:")
            for pnum, expect, found, expect_sub in puzzle_mismatches:
                print(
                    f"  Puzzle {pnum}: expected {expect} cols ( {expect_sub} subtasks + 1 rule ), found {found} columns on sheet"
                )
            print(
                "Suggestion: expand the second header row to include missing subtask columns for the affected puzzles, then re-run."
            )
            if dry_run:
                print(
                    "Dry-run: mismatch detected (no write). Fix sheet headers and re-run the real sync."
                )
            else:
                sys.exit(1)
        # identify DB column name used for student id
        db_student_key = db_student_col

        # Preload the sheet student id column values for fast lookup
        if sheet_student_idx is None:
            print(
                "Error: could not find 學號 column in sheet headers. Ensure a column is labeled 學號."
            )
            sys.exit(1)

        sheet_student_col_vals = worksheet.col_values(sheet_student_idx + 1)

        # Build DB mapping by student id for fast lookups (grab id col from DB rows)
        db_map = {}
        for r in row_dicts:
            key = str(r.get(db_student_key) or "").strip()
            if key:
                db_map[key] = r

        # Iterate the online sheet rows (skip first two header rows) and update
        rows_updated = 0
        rows_skipped = 0
        dry_run_issues = []
        dry_run_warnings_total = 0
        dry_run_errors_total = 0

        start_col_letter = col_num_to_a1(data_start_col + 1)
        updates = []
        for idx, cell_val in enumerate(sheet_student_col_vals[2:], start=3):
            sheet_student_id = str(cell_val or "").strip()
            if not sheet_student_id:
                rows_skipped += 1
                continue

            r = db_map.get(sheet_student_id)
            if not r:
                # no matching DB row for this sheet id
                rows_skipped += 1
                continue

            values = build_values_for_row(
                r, puzzle_col_name, mapping, data_start_col, last_data_col
            )

            # Optimization: only update rows that have non-zero data (e.g., any '1' or non-empty/non-'0')
            should_update = any(
                (v is not None and str(v).strip() not in ("", "0", "0.0"))
                for v in values
            )
            if not should_update:
                rows_skipped += 1
                continue

            if dry_run:

                raw = r.get(puzzle_col_name) if puzzle_col_name else None
                parsed = parse_puzzle_results(raw)
                row_warnings = []
                row_errors = []
                if raw and not parsed:
                    row_warnings.append("puzzle_results appears unparseable or empty")

                values_str = ",".join(
                    (str(v).strip() if v and str(v).strip() else ".") for v in values
                )
                max_display_chars = 400
                if len(values_str) > max_display_chars:
                    non_empty = [
                        i + data_start_col + 1
                        for i, v in enumerate(values)
                        if v and str(v).strip()
                    ]
                    values_display = f"{len(non_empty)} non-empty at cols {non_empty[:10]}{'...' if len(non_empty) > 10 else ''}"
                else:
                    values_display = values_str

                ws_col_count = getattr(worksheet, "col_count", None)
                if ws_col_count and (data_start_col + len(values) > ws_col_count):
                    row_warnings.append(
                        f"write would extend past sheet width ({ws_col_count} cols)"
                    )
                ws_row_count = getattr(worksheet, "row_count", None)
                if ws_row_count and idx > ws_row_count:
                    row_warnings.append(
                        f"target row {idx} beyond sheet row count ({ws_row_count})"
                    )

                expected_cols = last_data_col - data_start_col + 1
                if len(values) != expected_cols:
                    row_errors.append(
                        f"values length {len(values)} != expected columns {expected_cols}"
                    )

                print(
                    f"Dry-run: would update row {idx} starting at {start_col_letter} with:"
                )
                print(f"  {values_display}")
                if row_errors:
                    print("  Errors:")
                    for e in row_errors:
                        print(f"    - {e}")
                if row_warnings:
                    print("  Warnings:")
                    for w in row_warnings:
                        print(f"    - {w}")

                if row_warnings:
                    dry_run_warnings_total += len(row_warnings)
                if row_errors:
                    dry_run_errors_total += len(row_errors)
                    dry_run_issues.extend([f"Row {idx}: {e}" for e in row_errors])
                elif row_warnings:
                    dry_run_issues.extend([f"Row {idx}: {w}" for w in row_warnings])
            else:
                # queue the update; we'll send a single batch call after the loop
                updates.append(
                    {"range": f"{start_col_letter}{idx}", "values": [values]}
                )
                rows_updated += 1

        # perform a single batch update to avoid per-minute API limits
        if not dry_run:
            if updates:
                try:
                    worksheet.batch_update(updates)
                    print(
                        f"Batch-updated {len(updates)} rows starting at column {start_col_letter}."
                    )
                except Exception as e:
                    print(f"Error during batch update: {e}")
            else:
                print("No rows needed updating (batch skipped).")

        print(f"Done. Rows updated: {rows_updated}, skipped: {rows_skipped}")
        if dry_run:
            if dry_run_errors_total > 0:
                print(
                    f"Dry-run detected {dry_run_errors_total} error(s) and {dry_run_warnings_total} warning(s). Errors may block a real run."
                )
                print("Sample issues:")
                for it in dry_run_issues[:10]:
                    print(f"  - {it}")
            elif dry_run_warnings_total > 0:
                print(
                    f"Dry-run detected {dry_run_warnings_total} warning(s); no errors detected."
                )
                for it in dry_run_issues[:10]:
                    print(f"  - {it}")
            else:
                print("Dry-run found no warnings or errors.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync scoreboard to Google Sheets")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to Google Sheets; print preview",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
