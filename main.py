import os
import sys
import argparse
import time
import datetime
from dotenv import load_dotenv

from db import fetch_scoreboard_rows
from sheets import get_worksheet
from mapping import (
    build_sheet_mapping,
    build_values_for_row,
)
from utils import pick_column, col_num_to_a1, parse_puzzle_results, is_test_passed


def sync_once(dry_run=False):
    load_dotenv()
    # timestamp for this run (UTC ISO)
    ts = datetime.datetime.now().replace(microsecond=0).isoformat() + "Z"
    print(f"Sync run: {ts}")

    SHEET_URL = os.getenv("SHEET_URL")
    SHEET_TITLE = os.getenv("SHEET_TITLE")
    worksheet = get_worksheet(SHEET_URL, SHEET_TITLE)

    # Fetch DB rows and selected columns
    row_dicts, selected_cols = fetch_scoreboard_rows()

    puzzle_col_name = (
        pick_column(
            selected_cols, ["puzzle_results", "puzzleResults", "puzzles", "results"]
        )
        if selected_cols
        else None
    )

    db_missing_required = []
    db_student_col = (
        pick_column(selected_cols, ["student_ID", "student_id", "學號", "studentId"])
        if selected_cols
        else None
    )
    db_puzzle_amount_col = (
        pick_column(selected_cols, ["puzzle_amount", "puzzles_amount"])
        if selected_cols
        else None
    )

    if not db_student_col:
        db_missing_required.append("student id (student_ID or student_id)")
    if not (puzzle_col_name or db_puzzle_amount_col):
        db_missing_required.append("puzzle_results (serialized) or puzzle_amount (int)")

    if db_missing_required:
        print("\nDatabase columns check:")
        print("  Missing required columns:")
        for m in db_missing_required:
            print(f"    - {m}")
        return 1

    # compute puzzle indices and max subtasks
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
                print(f"Error: could not parse integer counts from column '{amt_col}'.")
                return 1
        else:
            print(
                "Error: could not find 'puzzle_results' column nor 'puzzle_amount' in the DB table."
            )
            return 1

    sorted_puzzles = sorted(puzzle_indices)

    (
        mapping,
        sheet_class_idx,
        sheet_student_idx,
        sheet_name_idx,
        data_start_col,
        last_data_col,
    ) = build_sheet_mapping(worksheet)

    # Determine which puzzles are present on sheet
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

    final_puzzles = [p for p in sorted_puzzles if p in sheet_puzzle_idxs]
    missing_on_sheet = [p for p in sorted_puzzles if p not in sheet_puzzle_idxs]
    if missing_on_sheet:
        print(
            "Warning: these puzzles exist in DB but not on sheet and will be skipped:"
        )
        print("  ", ", ".join(str(p + 1) for p in missing_on_sheet))

    header1 = ["學號"]
    header2 = [""]
    for pidx in final_puzzles:
        db_subcount = max_subtasks.get(pidx, 0)
        sheet_subcount = sheet_subtask_counts.get(pidx, 0)
        used_subcount = min(db_subcount, sheet_subcount)
        group_size = used_subcount + 1
        header1.extend([str(pidx + 1)] * group_size)
        for s in range(1, used_subcount + 1):
            header2.append(str(s))
        header2.append("規則")

    # Preload sheet student ids
    if sheet_student_idx is None:
        print(
            "Error: could not find 學號 column in sheet headers. Ensure a column is labeled 學號."
        )
        return 1

    sheet_student_col_vals = worksheet.col_values(sheet_student_idx + 1)

    db_student_key = db_student_col
    db_map = {}
    for r in row_dicts:
        key = str(r.get(db_student_key) or "").strip()
        if key:
            db_map[key] = r

    rows_updated = 0
    rows_skipped = 0
    start_col_letter = col_num_to_a1(data_start_col + 1)
    updates = []
    for idx, cell_val in enumerate(sheet_student_col_vals[2:], start=3):
        sheet_student_id = str(cell_val or "").strip()
        if not sheet_student_id:
            rows_skipped += 1
            continue

        r = db_map.get(sheet_student_id)
        if not r:
            rows_skipped += 1
            continue

        values = build_values_for_row(
            r, puzzle_col_name, mapping, data_start_col, last_data_col
        )

        should_update = any(
            (v is not None and str(v).strip() not in ("", "0", "0.0")) for v in values
        )
        if not should_update:
            rows_skipped += 1
            continue

        if dry_run:
            raw = r.get(puzzle_col_name) if puzzle_col_name else None
            parsed = parse_puzzle_results(raw)
            values_str = ",".join(
                (str(v).strip() if v and str(v).strip() else ".") for v in values
            )
            if len(values_str) > 400:
                non_empty = [
                    i + data_start_col + 1
                    for i, v in enumerate(values)
                    if v and str(v).strip()
                ]
                values_display = f"{len(non_empty)} non-empty at cols {non_empty[:10]}{'...' if len(non_empty) > 10 else ''}"
            else:
                values_display = values_str
            print(
                f"Dry-run: would update row {idx} starting at {start_col_letter} with:"
            )
            print(f"  {values_display}")
        else:
            updates.append({"range": f"{start_col_letter}{idx}", "values": [values]})
            rows_updated += 1

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

    print(f"Done. Rows updated: {rows_updated}, skipped: {rows_skipped} (run at {ts})")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync scoreboard to Google Sheets")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to Google Sheets; print preview",
    )
    args = parser.parse_args()

    # read INTERVAL from .env in main; pass dry-run flag here
    def _entry():
        load_dotenv()
        interval_val = os.getenv("INTERVAL")
        try:
            interval = (
                float(interval_val)
                if interval_val is not None and interval_val != ""
                else 0
            )
        except Exception:
            print(f"Invalid INTERVAL value: {interval_val}")
            interval = 0

        if interval and interval > 0:
            print(
                f"Starting periodic sync: interval={interval} seconds (Ctrl-C to stop)"
            )
            try:
                while True:
                    sync_once(dry_run=args.dry_run)
                    time.sleep(interval)
            except KeyboardInterrupt:
                print("Periodic sync stopped by user")
        else:
            return sync_once(dry_run=args.dry_run)

    _entry()
