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
from retry import describe
from exam_config import load_config, question_mapping, normalize_rows, validate_layout
from question_errors import errored_questions
from roster import read_roster
from setup_sheet import setup_workbook
from backup_service import backup_once
from drive_auth import user_credentials
from scheduler import run_jobs
from exam_config import number


def sync_once(dry_run=False, sheet_url=None, config=None, question_map=None):
    load_dotenv()
    # timestamp for this run (UTC ISO)
    ts = datetime.datetime.now().replace(microsecond=0).isoformat() + "Z"
    print(f"Sync run: {ts}")

    SHEET_URL = sheet_url or os.getenv("SHEET_URL")
    SHEET_TITLE = os.getenv("SHEET_TITLE")
    if SHEET_TITLE in ('Combined', '成績登記'):
        raise ValueError('Score uploads must target a classroom tab, not a computed grade view')
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
        pick_column(selected_cols, ["student_ID", "student_id", "學號", "studentId", "testId"])
        if selected_cols
        else None
    )
    db_puzzle_amount_col = (
        pick_column(selected_cols, ["puzzle_amount", "puzzles_amount", "puzzleAmount"])
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

    positions = None
    if config is not None:
        positions = question_mapping(config, question_map)
        if not puzzle_col_name:
            raise ValueError('Config mapping requires puzzleResults')
        row_dicts = normalize_rows(row_dicts, puzzle_col_name, config, positions)

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
            pick_column(selected_cols, ["puzzle_amount", "puzzles_amount", "puzzleAmount"])
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
    if config is not None:
        validate_layout(mapping, config, positions)

    # Determine which puzzles are present on sheet
    sheet_puzzle_idxs = set()
    sheet_subtask_counts = {}
    for i, info in enumerate(mapping):
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

    # Read current scores as well as student ids for change detection.
    if sheet_student_idx is None:
        print(
            "Error: could not find 學號 column in sheet headers. Ensure a column is labeled 學號."
        )
        return 1

    sheet_rows = worksheet.get_all_values()

    db_student_key = db_student_col
    db_map = {}
    for r in row_dicts:
        key = str(r.get(db_student_key) or "").strip()
        if key:
            db_map[key] = r
    roster_ids = {str(row[sheet_student_idx]).strip() for row in sheet_rows[2:]
                  if len(row) > sheet_student_idx}
    for student_id in sorted(db_map.keys() - roster_ids):
        print(f'WARNING: Student {student_id} is absent from the target roster; skipped.')

    rows_updated = 0
    rows_skipped = 0
    updates = []
    changed_rows = 0
    for idx, sheet_row in enumerate(sheet_rows[2:], start=3):
        cell_val = sheet_row[sheet_student_idx] if sheet_student_idx < len(sheet_row) else ""
        sheet_student_id = str(cell_val or "").strip()
        if not sheet_student_id:
            rows_skipped += 1
            continue

        r = db_map.get(sheet_student_id)
        if not r:
            rows_skipped += 1
            continue

        errors = errored_questions(r.get(puzzle_col_name))
        for index, reason in errors.items():
            label = next((qid for qid, position in (positions or {}).items()
                          if str(position) == index), f'Q{int(index) + 1}' if index.isdigit() else index)
            print(f'WARNING: Student {sheet_student_id}, question {label}: '
                  f'{describe(RuntimeError(reason))}. Skipped question; existing cells preserved.')
        values = build_values_for_row(
            r, puzzle_col_name, mapping, data_start_col, last_data_col, errors
        )

        row_updates = []
        for col_idx, value in enumerate(values, start=data_start_col):
            if value is None:
                continue
            current = sheet_row[col_idx] if col_idx < len(sheet_row) else ""
            # Sheets returns strings, while generated pass flags are integers.
            if str(current) != str(value):
                row_updates.append({
                    "range": f"{col_num_to_a1(col_idx + 1)}{idx}",
                    "values": [[value]],
                })

        if not row_updates:
            rows_skipped += 1
            continue

        changed_rows += 1
        if dry_run:
            for update in row_updates:
                print(f"Dry-run: would update {update['range']} to {update['values'][0][0]!r}")
        updates.extend(row_updates)

    if not dry_run:
        if updates:
            try:
                worksheet.batch_update(updates)
                rows_updated = changed_rows
                print(
                    f"Batch-updated {len(updates)} cells across {rows_updated} rows."
                )
            except Exception as e:
                print(f"Error during batch update: {describe(e)}")
                return 1
        else:
            print("No rows needed updating (batch skipped).")

    print(f"Done. Rows updated: {rows_updated}, skipped: {rows_skipped} (run at {ts})")
    return 0


def cli(argv=None):
    parser = argparse.ArgumentParser(description="Sync scoreboard to Google Sheets")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to Google Sheets; print preview",
    )
    parser.add_argument('--sheet-url', help='Override SHEET_URL')
    parser.add_argument('--config', help='Full exam configuration JSON')
    parser.add_argument('--question-map', help='JSON: question ID to one-based Sheet group')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--setup-sheet', action='store_true', help='Initialize five workbook tabs')
    modes.add_argument('--authorize-drive', action='store_true', help='One-time user OAuth consent for backups')
    modes.add_argument('--backup-only', action='store_true', help='Run database backups without score sync')
    parser.add_argument('--backup-folder', help='Enable backups into this Drive folder URL/ID')
    parser.add_argument('--backup-interval', type=float, help='Backup seconds; default 300; 0 runs once')
    parser.add_argument('--classroom', help='Classroom label for backup filenames; defaults to SHEET_TITLE')
    parser.add_argument('--oauth-client', default='drive_oauth_client.json', help='Desktop OAuth client JSON')
    parser.add_argument('--oauth-token', default='drive_token.json', help='Local OAuth refresh credentials')
    parser.add_argument('--roster', help='Excel roster path')
    parser.add_argument('--roster-sheet', help='Roster worksheet name')
    args = parser.parse_args(argv)
    if args.setup_sheet and not all((args.sheet_url, args.config, args.roster)):
        parser.error('--setup-sheet requires explicit --sheet-url, --config and --roster')
    if args.authorize_drive and args.dry_run:
        print('Dry-run: would authorize Google Drive; no credentials changed.')
        return 0

    # read INTERVAL from .env in main; pass dry-run flag here
    def _entry():
        load_dotenv()
        if args.authorize_drive:
            user_credentials(args.oauth_token, args.oauth_client, interactive=True)
            print('Google Drive authorization saved locally. Scheduled backups can now run unattended.')
            return 0
        options = {'dry_run': args.dry_run}
        if args.sheet_url:
            options['sheet_url'] = args.sheet_url
        if args.config:
            options['config'] = load_config(args.config)
        if args.setup_sheet:
            students = read_roster(args.roster, args.roster_sheet)
            return setup_workbook(options['config'], students, args.sheet_url, dry_run=args.dry_run)
        if args.question_map:
            if not args.config:
                raise ValueError('--question-map requires --config')
            options['question_map'] = args.question_map
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

        destination = args.backup_folder or os.getenv('BACKUP_FOLDER')
        if args.backup_only and not destination:
            raise ValueError('--backup-only requires --backup-folder or BACKUP_FOLDER')
        if destination:
            classroom = args.classroom or os.getenv('CLASSROOM') or os.getenv('SHEET_TITLE')
            if not classroom:
                raise ValueError('Backup requires --classroom, CLASSROOM or SHEET_TITLE')
            backup_interval = args.backup_interval if args.backup_interval is not None else float(os.getenv('BACKUP_INTERVAL') or 300)
            number(backup_interval, 'Backup interval')
            jobs = []
            if not args.backup_only:
                jobs.append((lambda: sync_once(**options), 0 if args.dry_run else interval))
            jobs.append((lambda: backup_once(destination, classroom, args.oauth_token, args.oauth_client,
                                             dry_run=args.dry_run), 0 if args.dry_run else backup_interval))
            return run_jobs(jobs)

        if interval and interval > 0:
            print(
                f"Starting periodic sync: interval={interval} seconds (Ctrl-C to stop)"
            )
            try:
                while True:
                    result = sync_once(**options)
                    if result:
                        return result
                    time.sleep(interval)
            except KeyboardInterrupt:
                print("Periodic sync stopped by user")
                return 0
        else:
            return sync_once(**options)

    try:
        return _entry()
    except Exception as error:
        print(f'Error: {describe(error)}')
        return 1


if __name__ == "__main__":
    sys.exit(cli())
