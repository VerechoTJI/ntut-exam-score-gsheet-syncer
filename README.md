# NTUT Exam Score GSheet Syncer

Upload the on-machine client's scoreboard from v2 to a classroom worksheet, or initialize a five-tab workbook from an Excel roster and exam config. Judging and highest/current selection remain in the on-machine client.

## Install

Python 3.13+ and uv are required. Run from this directory.
All file arguments below are relative to this directory. Replace `../roster.xlsx`
(PowerShell: `..\roster.xlsx`) with the relative path to your own Excel roster.

PowerShell:

```powershell
uv sync --frozen
```

Bash:

```bash
uv sync --frozen
```

Put the Google service-account key in `credentials.json`, enable Sheets/Drive APIs, and share the target Sheet with the service-account email as an editor. Never commit the key or `.env`.

## Set up the workbook

PowerShell:

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "..\exam-config.json" `
  --roster "..\roster.xlsx" `
  --roster-sheet "工作表1" `
  --dry-run
```

Bash:

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --config "../exam-config.json" \
  --roster "../roster.xlsx" \
  --roster-sheet "工作表1" \
  --dry-run
```

Remove `--dry-run` to apply. Setup requires explicit URL, config and roster arguments; there is no environment destination fallback and no new spreadsheet is created. Select `--roster-sheet` when multiple worksheets have identity headers. Columns are located by `班級`, `姓名`, `學號` regardless of position. Student IDs remain text, roster order is preserved, and coursework columns are ignored. Invalid/duplicate identities or invalid configs stop before writes.

Setup initializes `1222`, `1223`, `335`, `Combined`, and `成績登記`. It identifies the destination, summarizes existing data/formulas, and warns before replacing target contents and formatting. Only `y` or `yes` continues; Enter, refusal or EOF cancels. Unrelated tabs remain unchanged. Dry-run reads but never writes. Stop classroom sync processes before replacing their tabs.

Combined SUMs results by student ID across classrooms, independently of row order. Values above 1 remain visible and are highlighted for investigation. The final view uses exact subtask weights, one failed-rule multiplier per question, section caps, and rounding once at the end. No extra 100-point cap is imposed. Raw score excludes penalties and caps.

Setup reproduces the [reference workbook's layout](https://docs.google.com/spreadsheets/d/12cAocV3Pp6FU-5aDu_zFSh2Gyh3b2x8V2qWdwx0huCY/edit): white headers, black group borders, compact score columns, Microsoft JhengHei score text, smaller identity text, green/red final grades, hidden zero text, and gray raw scores. The question groups and weights adapt to your config. Six columns separate the final and raw scores; the sidebar contains the yellow `已離場` label, pass/fail counts and percentages, score-band counts, and cumulative counts.

The sidebar includes the reference's two-series **line plot**, `分數區段人數統計和累計人數`, at 338 × 209 pixels. Its twelve bands cover `100+`, `90-99` through `10-19`, `1-9`, and `0`. All counts use the same roster extent and exclude blank student IDs. Existing charts on the five setup tabs are replaced after the normal overwrite confirmation. The reference file itself is never modified.

## Synchronize scores

Copy `.env.template` to `.env` and set backend URL/admin token, Sheet URL, classroom tab, and interval. Set `SHEET_TITLE` to one classroom source tab, not Combined or the final view. One process targets one classroom/backend.

PowerShell:

```powershell
uv run python main.py --config "..\exam-config.json" --dry-run
uv run python main.py --config "..\exam-config.json"
```

Bash:

```bash
uv run python main.py --config "../exam-config.json" --dry-run
uv run python main.py --config "../exam-config.json"
```

`--sheet-url` overrides `SHEET_URL`. Config order flattens `sections[].puzzles[]` into numbered question groups, preserving subtask order. Actual puzzle IDs and canonical client `Q1...Qn` payloads are supported. Use the same config for setup and sync. Without `--config`, legacy numbered payloads remain supported.

Optional `--question-map mapping.json` overrides config order and requires `--config`. It maps actual IDs to one-based Sheet groups, for example:

```json
{"custom-question-b": 2, "custom-question-a": 1}
```

Unknown IDs, duplicate destinations, ambiguous aliases and layout/subtask mismatches fail before writes. Classroom tabs use two header rows with `學號`, numbered question groups/subtasks, and `規則`. Data starts at row 3. `最後成績`/`原始分數` delimit the legacy score range.

Only changed cells upload. Ordinary failures clear previous pass flags. An existing question with no rule results writes 1 for its rule flag, matching the client's no-penalty policy. System-error markers (`SE`, `CTLE`, `ABORTED`, explicit system attribution, or the client's reserved `ruleId: "__engine_error__"`) skip only that student's affected question, preserve existing cells, and print its available cause. For rule-engine failures, prefer the concrete `reason` over the generic message. Other questions continue. Backend students absent from the roster warn and are skipped.

`INTERVAL=0`, empty or negative runs once; positive values poll. Invalid text warns and falls back to one shot. Ctrl+C stops periodic mode. Backend failures retry with exponential delays starting at 1 second. `BACKOFF_MAX_SECONDS` (default `10`) stops retries when the **next delay exceeds the setting**: wait 1/2/4/8 seconds, then stop if the next failed attempt would require waiting 16 seconds. This is not a total elapsed-time budget. Each backend request separately uses a 10-second connect/read timeout; a continuously progressing download may take longer. Failures print HTTP/provider/transport causes with tokens redacted. Exhaustion stops even periodic mode with exit status 1. Keep one writer per classroom tab.

Configure these independent settings in `.env` (names and descriptions are also in `.env.template`):

| Variable | Default (seconds) | Meaning |
| --- | --- | --- |
| `BACKOFF_MAX_SECONDS` | `10` | Backend retry-delay stopping threshold; the next delay above it stops the operation. |
| `GOOGLE_TIMEOUT_SECONDS` | `30` | Per-request connect/read timeout for Sheets, Drive and OAuth token HTTP calls. |

Both accept positive finite numbers; unset or blank uses the default. Google authentication refresh/retries remain SDK-managed and do not use backend backoff. Google timeout does not cap total login/transfer duration or the user's browser consent time. No process cancellation or replacement HTTP library is introduced.

## Database backups to My Drive

V2's authenticated `GET /admin/backup` returns a binary `.sqlite` snapshot, not
a textual SQL dump. Backups use Google **user OAuth**; Sheets continue using the
existing service account. The backend still uses `ADMIN_TOKEN` from `.env`.

One-time setup:

1. In your [Google Cloud project](https://console.cloud.google.com/apis/credentials),
   enable Google Drive API and configure the OAuth consent audience. Add your
   account as a test user if the app is in external Testing mode.
2. Create an OAuth client with application type **Desktop app**, download its
   JSON, and save it locally as `drive_oauth_client.json`. This is different from
   the service-account `credentials.json`.
3. Run the following and sign in with an account able to upload to the folder:

PowerShell:

```powershell
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

Bash:

```bash
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

The script stores `drive_token.json` locally for unattended refresh. Both files
are ignored by Git. Use `--oauth-client`/`--oauth-token` for alternative paths;
keep any custom secret paths outside version control too. Runtime never starts
interactive sign-in automatically: missing/revoked credentials cause a clear
failure. Full Drive scope supports your existing My Drive destination. Google
external apps in Testing can issue refresh tokens that expire after seven days;
check consent/publishing settings or reauthorize before the exam.
[Google OAuth documentation](https://developers.google.com/identity/protocols/oauth2/).

For your selected folder and 300-second interval:

PowerShell:

```powershell
uv run python main.py --backup-only `
  --backup-folder "https://drive.google.com/drive/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq" `
  --backup-interval 300 --classroom 1222
```

Bash:

```bash
uv run python main.py --backup-only \
  --backup-folder "https://drive.google.com/drive/folders/1DuyLIGc-eQY4lU9WOt3ziJ6tAlcFIuNq" \
  --backup-interval 300 --classroom 1222
```

The first backup runs immediately, then the next starts 300 seconds after the
previous finishes. Add `--dry-run` for one preview without HTTP, credentials or
uploads. Set `--backup-interval 0` for one real backup to verify sign-in and access.

`--backup-folder` accepts a folder ID, a `/drive/folders/ID` URL, or an account-specific `/drive/u/5/folders/ID` URL. Any nonnegative account index is supported; query strings, fragments and a trailing slash are allowed.

To run both jobs in one process, omit `--backup-only` and add `--config` for score
mapping. Score sync uses `INTERVAL`; backup uses its own timer. Jobs are serialized
so they cannot overlap. Alternatively run the backup-only command alongside your
score-sync process. `BACKUP_FOLDER`, `BACKUP_INTERVAL`, and `CLASSROOM` may be set
in `.env`; backups remain disabled when no folder is configured. Classroom falls
back to `SHEET_TITLE`. The command must remain running; no desktop automation or
background service is installed.

Files are named `classroom_timestamp.sqlite`, for example
`1222_20261026T143000123456+0800.sqlite`. Timestamps include microseconds and the
Asia/Taipei offset. Invalid classroom labels are rejected. Downloaded bytes are
checked for completeness, SQLite header and read-only integrity before uploading.
The original bytes are preserved. A stable Drive file ID prevents duplicate
creates during SDK authentication replay; destination, filename, size and MD5 are verified afterward.
Success prints the file URL. Temporary local files are removed on success or
failure. If an upload response is lost, verify the assigned file ID without
blindly resending the upload. An unverified upload fails the job. Backend download
failures use `BACKOFF_MAX_SECONDS`; Google HTTP calls use `GOOGLE_TIMEOUT_SECONDS`
without the application's backend retry loop. Existing Drive backups are retained.
[Google upload/retry documentation](https://developers.google.com/workspace/drive/api/guides/manage-uploads).

## Verification

PowerShell:

```powershell
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

Bash:

```bash
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

Offline tests isolate credentials and HTTP/Sheets boundaries. The exact gate requires 100% statements and branches across all production modules, CLI and gate included, and detects unmeasured modules. The old manual `test_db_parse.py` diagnostic is outside the offline suite. Native Google Sheets checks use a disposable workbook and synthetic identities.

See [ADR-001](docs/decisions/001-sheet-setup-from-roster-and-exam-config.md), [plan](tasks/plan.md), and [task ledger](tasks/todo.md).
