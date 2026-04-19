**NTUT Exam Score GSheet Syncer**

Synchronizes scoreboard rows from a PostgreSQL `score_boards` table into a Google Sheet. It parses serialized puzzle results from the DB and maps them to grouped puzzle/subtask columns on a sheet whose first two rows contain the puzzle header layout.

**Features:**
- Export per-student puzzle/subtask pass flags from DB to Google Sheets.
- Dry-run mode for previewing changes without writing to the sheet.
- Flexible parsing for JSON or Python-literal serialized `puzzle_results`.

**Requirements:**
- Python 3.8+
- A Google service account `credentials.json` with Drive + Sheets scopes
- A PostgreSQL database accessible to the script

**Python dependencies** (declared in `pyproject.toml`):
- `python-dotenv`, `google-auth`, `gspread`, `psycopg2`

**Quick install (venv + pip)**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # PowerShell
python -m pip install --upgrade pip
pip install -r requirements.txt  # or use pyproject tooling
```

**Configuration / environment**
- Place the Google service account JSON as `credentials.json` in the project directory.
- Set the following environment variables (for example in a `.env` file):
  - `DB_NAME`
  - `DB_USER`
  - `DB_PASS`
  - `DB_HOST`
  - `DB_PORT`
  - `SHEET_URL` (the full Google Sheets URL for the target spreadsheet)

The script currently opens the worksheet named `1222` by default. Edit `main.py` if you need a different worksheet name.

**Sheet format expectations**
- The sheet must have at least two header rows.
- The top row should contain puzzle group headers (e.g., `1`, `2`, ...), forward-filled for merged headings.
- The second header row should contain subtask indices (e.g., `1`, `2`, ...) and a column labeled `規則` for rule results.
- A column labeled `學號` (student id) is required for matching DB rows to sheet rows.

**Usage**

Preview (dry-run):

```powershell
python main.py --dry-run
```

Run (writes to sheet):

```powershell
python main.py
```

**Notes & troubleshooting**
- If the script reports missing DB columns, ensure your `score_boards` table contains either a serialized `puzzle_results` column or integer `puzzle_amount` and a student id column (`student_id`, `student_ID`, or `學號`).
- For debugging DB values locally, run `python test_db_parse.py` (if present) to inspect sample rows.
- The script expects puzzle result entries to include `subtasks` arrays or dictionaries with test lists; it treats tests as passed when they contain accepted/true-like markers.

**Next steps / improvements**
- Allow worksheet name and sheet-range to be configurable via env vars or CLI flags.
- Add unit tests for the parsing helpers and mapping logic.

---
Created from project sources in this repository.
