**NTUT Exam Score GSheet Syncer**

Synchronizes scoreboard rows from the `ntut-exam-v2` backend API into a Google Sheet. It parses serialized puzzle results from the API and maps them to grouped puzzle/subtask columns on a sheet whose first two rows contain the puzzle header layout.

**Features:**

- Export per-student puzzle/subtask pass flags from backend API to Google Sheets.
- Dry-run mode for previewing changes without writing to the sheet.
- Flexible parsing for JSON-serialized `puzzleResults`.

**Requirements:**

- Python 3.8+
- A Google service account `credentials.json` with Drive + Sheets scopes
- The `ntut-exam-v2` backend API accessible to the script

**Python dependencies** (declared in `pyproject.toml`):

- `python-dotenv`, `google-auth`, `gspread`, `requests`

**Quick install (uv)**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
uv sync
```

**Configuration / environment**

- Place the Google service account JSON as `credentials.json` in the project directory.
- Set the following environment variables (for example in a `.env` file):
  - `BACKEND_API_URL` (the base URL for the backend API, e.g., http://localhost:3000)
  - `ADMIN_TOKEN` (the admin token string expected by the backend middleware)
  - `SHEET_URL` (the full Google Sheets URL for the target spreadsheet)
  - `SHEET_TITLE` (the exact name of the worksheet/tab to update, e.g., "1222")
  - `INTERVAL` (optional sync interval in seconds, 0 or empty means one-time run)

**Sheet format expectations**

- The sheet must have at least two header rows.
- The top row should contain puzzle group headers (e.g., `1`, `2`, ...), forward-filled for merged headings.
- The second header row should contain subtask indices (e.g., `1`, `2`, ...) and a column labeled `規則` for rule results.
- A column labeled `學號` (student id) is required for matching backend rows to sheet rows.

**Usage**

Preview (dry-run):

```powershell
uv run python main.py --dry-run
```

Run (writes to online sheet):

```powershell
uv run python main.py
```

**Notes & troubleshooting**

- If the script reports missing columns, ensure your backend is returning `testId` or a student id column (`student_id`, `student_ID`, or `學號`) and `puzzleResults` or `puzzleAmount`.
- The script expects puzzle result entries to include `subtasks` arrays or dictionaries with test lists; it treats tests as passed when they contain accepted/true-like markers.

**Next steps / improvements**

- Add unit tests for the parsing helpers and mapping logic.

---

Created from project sources in this repository.
