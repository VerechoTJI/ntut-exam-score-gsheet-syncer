# NTUT Exam Score GSheet Syncer

Upload the on-machine client's scoreboard from v2 to a classroom worksheet, or initialize a five-tab workbook from an Excel roster and exam config. Judging and highest/current selection remain in the on-machine client.

## Install

Python 3.13+ and uv are required. Run from this directory.

```powershell
uv sync --frozen
```

Put the Google service-account key in `credentials.json`, enable Sheets/Drive APIs, and share the target Sheet with the service-account email as an editor. Never commit the key or `.env`.

## Set up the workbook

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "..\exam-config.json" `
  --roster "C:\path\to\roster.xlsx" `
  --roster-sheet "工作表1" `
  --dry-run
```

Remove `--dry-run` to apply. Setup requires explicit URL, config and roster arguments; there is no environment destination fallback and no new spreadsheet is created. Select `--roster-sheet` when multiple worksheets have identity headers. Columns are located by `班級`, `姓名`, `學號` regardless of position. Student IDs remain text, roster order is preserved, and coursework columns are ignored. Invalid/duplicate identities or invalid configs stop before writes.

Setup initializes `1222`, `1223`, `335`, `Combined`, and `成績登記`. It identifies the destination, summarizes existing data/formulas, and warns before replacing target contents and formatting. Only `y` or `yes` continues; Enter, refusal or EOF cancels. Unrelated tabs remain unchanged. Dry-run reads but never writes. Stop classroom sync processes before replacing their tabs.

Combined SUMs results by student ID across classrooms, independently of row order. Values above 1 remain visible and are highlighted for investigation. The final view uses exact subtask weights, one failed-rule multiplier per question, section caps, and rounding once at the end. No extra 100-point cap is imposed. Raw score excludes penalties and caps.

## Synchronize scores

Copy `.env.template` to `.env` and set backend URL/admin token, Sheet URL, classroom tab, and interval. Set `SHEET_TITLE` to one classroom source tab, not Combined or the final view. One process targets one classroom/backend.

```powershell
uv run python main.py --config "..\exam-config.json" --dry-run
uv run python main.py --config "..\exam-config.json"
```

`--sheet-url` overrides `SHEET_URL`. Config order flattens `sections[].puzzles[]` into numbered question groups, preserving subtask order. Actual puzzle IDs and canonical client `Q1...Qn` payloads are supported. Use the same config for setup and sync. Without `--config`, legacy numbered payloads remain supported.

Optional `--question-map mapping.json` overrides config order and requires `--config`. It maps actual IDs to one-based Sheet groups, for example:

```json
{"custom-question-b": 2, "custom-question-a": 1}
```

Unknown IDs, duplicate destinations, ambiguous aliases and layout/subtask mismatches fail before writes. Classroom tabs use two header rows with `學號`, numbered question groups/subtasks, and `規則`. Data starts at row 3. `最後成績`/`原始分數` delimit the legacy score range.

Only changed cells upload. Ordinary failures clear previous pass flags. An existing question with no rule results writes 1 for its rule flag, matching the client's no-penalty policy. System-error markers (`SE`, `CTLE`, `ABORTED`, or explicit system attribution) skip only that student's affected question, preserve existing cells, and print its available cause. Other questions continue. Backend students absent from the roster warn and are skipped.

`INTERVAL=0`, empty or negative runs once; positive values poll. Invalid text warns and falls back to one shot. Ctrl+C stops periodic mode. An external operation has a 10-second request/retry budget; exponential delays of 1/2/4 seconds are limited by remaining time. Failures print HTTP/provider/transport causes with tokens redacted. Exhaustion stops even periodic mode with exit status 1. Keep one writer per classroom tab.

## Verification

```powershell
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

Offline tests isolate credentials and HTTP/Sheets boundaries. The exact gate requires 100% statements and branches across all production modules, CLI and gate included, and detects unmeasured modules. The old manual `test_db_parse.py` diagnostic is outside the offline suite. Native Google Sheets checks use a disposable workbook and synthetic identities.

See [ADR-001](docs/decisions/001-sheet-setup-from-roster-and-exam-config.md), [plan](tasks/plan.md), and [task ledger](tasks/todo.md).
