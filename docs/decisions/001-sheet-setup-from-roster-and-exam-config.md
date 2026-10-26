# ADR-001: Set up the score workbook from a roster and exam configuration

## Status

Accepted and implemented for workbook setup and score uploads. Implementation
and verification records are in `tasks/plan.md` and `tasks/todo.md`. Backup user
OAuth consent and a live upload still require the operator's client JSON/sign-in.

## Date

2026-10-26 (Asia/Taipei)

## Context

The syncer currently writes pass flags into an existing worksheet. Preparing the
whole workbook manually requires coordinating classroom tabs, cross-sheet
formulas, subtask weights, rule multipliers, section caps, and final grades.

The reference workbook is
[115-1期中上機成績登記](https://docs.google.com/spreadsheets/d/12cAocV3Pp6FU-5aDu_zFSh2Gyh3b2x8V2qWdwx0huCY/edit).
It contains classroom tabs `1222`, `1223`, and `335`, a `Combined` tab, and a
final view named `成績登記`. It is a read-only design reference, not an authorized
write destination.

The supplied roster example, `11501PD(2).xlsx`, has a `工作表1` tab with
`班級`, `姓名`, and `學號` headers. Other columns contain existing coursework
grades, and other tabs serve unrelated purposes. Those grades are not inputs to
exam sheet setup. No student records are copied into this ADR.

## Decision

### Explicit setup command and destination

Add an opt-in `--setup-sheet` mode to the existing CLI. Setup targets an existing
Google spreadsheet and requires `--sheet-url`, `--roster`, and `--config`
arguments. `--config` supplies the local v2 exam configuration JSON file.
Setup must not fall back to `SHEET_URL` from `.env` or create a new spreadsheet.

Invocation from the syncer directory:

PowerShell:

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --roster "..\roster.xlsx" `
  --config "..\exam-config.json"
```

Bash:

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --roster "../roster.xlsx" \
  --config "../exam-config.json"
```

For ordinary syncing, `--sheet-url` may override the existing `SHEET_URL`
environment setting; the explicit-argument requirement applies to setup.

### Workbook inputs and structure

- Use the Excel roster as the source of student class, name, and ID, locating
  columns by header name rather than fixed position. Preserve roster order and
  store IDs as text. Ignore unrelated grades and tabs.
- Use the v2 exam configuration supplied by `--config` for question structure,
  subtask weights, rule multipliers, and section caps. Validate the supplied
  configuration before any workbook writes.
- Create or initialize `1222`, `1223`, and `335` as the default classroom tabs.
  Populate each with the same roster and grouped subtask/rule columns.
- Generate `Combined` with the same roster and cross-sheet aggregation formulas.
- Generate `成績登記` with class, ID, name, weighted subtask results, rule
  multipliers, section subtotals/caps, final grade, and raw score, following the
  reference workbook's calculation flow.
- Match cross-sheet records by student ID so sorting a classroom tab cannot
  associate a student's results with someone else.

### Reference appearance and distribution plot

Recreate the reference's measured formatting rather than a generic theme. Use
white backgrounds, black group borders, compact columns, Microsoft JhengHei 14
for final-view scores/headers, smaller identity text, gray raw scores, hidden
zero text, and red/green final-grade fills. Keep the six-column sidebar gap
between final and raw score; derive positions from the configured question and
section counts. Classroom tabs use 21-pixel rows and 30-pixel score columns;
the final view uses 32-pixel rows and 33-pixel subtask columns.

Include the yellow departure label, pass/fail counts and percentages, twelve
descending score bands, cumulative counts, and the reference's 338 × 209 pixel
two-series line chart titled `分數區段人數統計和累計人數`. Distribution formulas
use the same full roster range and exclude blank IDs. Treat band labels as text
so `10-19` cannot become a date. IDs are text; use the `?*` criterion to exclude
explicitly stored empty strings, which native Sheets counts with `<>`.
Replacing a setup tab replaces its old charts;
delete them before resizing to avoid invalid chart references. Keep the design
reference read-only and do not embed historical student records in code.

### Question mapping and configuration order

Use an explicit question mapping when supplied. Otherwise flatten
`config.sections[].puzzles[]` in configuration order and map each puzzle's actual
ID to Sheet question groups `1`, `2`, `3`, and so on. Subtasks follow their order
within each puzzle. Do not infer question order from the scoreboard API response
or require IDs such as `Q1`.

Setup generates numbered question groups using this same configuration order;
ordinary syncing uses it as the mapping fallback. If configuration and worksheet
layout do not match, stop before writing and report the mismatch. Ordinary sync
receives configuration through `--config`; the CLI shape for explicit mappings
remains an implementation detail to settle.

### Preserve SUM across classrooms

`Combined` must SUM the matching student's results across all three classroom
tabs. Do not replace SUM with MAX, OR, deduplication, or a clamp to one.

A student should sit the exam in only one classroom. Values above one are useful
evidence of duplicate participation or an incorrectly entered ID. Keep those
values visible and highlight them in `Combined`; do not silently correct them.
The final view still applies the exam's configured scoring rules and caps.

### Existing-content review before writing

Before mutation, inspect the five target tabs and show the destination title and
URL. If any target tab contains data or formulas:

1. Show a short per-tab summary, such as student/data row counts and whether
   formulas or grades are present.
2. Warn that setup will replace the target tabs' contents and formatting.
3. Ask `Continue? [y/N]` and proceed only on an explicit `y` or `yes`.

Enter, a negative response, or an unavailable confirmation must not authorize
replacement. Other tabs remain unchanged. Missing target tabs can be created.
`--setup-sheet --dry-run` previews the proposed changes without writing.

## Alternatives considered

- **Use only the destination from `.env`:** rejected for setup because an explicit
  URL makes the write destination apparent in the command and confirmation.
- **Create a new spreadsheet:** rejected; setup initializes the specified file.
- **Use the exam config as the roster:** rejected; the Excel roster is the
  authoritative source of student identity details.
- **Copy the example's students, grades, or fixed question counts:** rejected;
  generate these from the supplied roster and the current exam config.
- **Use MAX/OR across classrooms:** rejected because it would hide duplicate
  results that the operator wants to investigate.
- **Use row-position references:** rejected because independently sorting a tab
  could silently assign grades to the wrong student.
- **Overwrite populated tabs immediately:** rejected; the operator must first see
  the existing-content summary and explicitly confirm replacement.

## Consequences and verification requirements

Setup must generate formulas, not just formatted headers. Verification must trace
classroom results through Combined, weighted subtasks, rule multipliers, section
caps, raw scores, and the final grade. Include duplicate classroom results and
independently reordered classroom rows in those checks.

Roster validation needs clear errors for missing identity headers and duplicate
or missing IDs before any writes. Roster worksheet selection must be explicit or
unambiguous; several tabs may contain the same headers.

Preview and cancellation paths must leave the target unchanged. Existing-content
detection must include formulas, including formulas displaying an empty value.
Tests must also verify preservation of unrelated tabs and generated formulas
during subsequent score syncs.

## Open implementation decisions

- Choose a roster worksheet when multiple tabs have the required headers.
- Define handling for backend IDs absent from the roster.
- Verify formulas against the on-machine client: no rule results means no
  penalty; apply the question multiplier once when any rule fails; cap sections
  then round the accumulated exam score once. Use exact config weights rather
  than the displayed example weights.

## Relationship to the compatibility review

Judging and highest/current selection remain in `NTUT-on-machine-test`. The
backend stores uploaded results; the syncer transfers them to Sheets without
rejudging or adding a highest-score comparison.

When an uploaded question has a judging system error, skip only that question
for that student, preserve its existing Sheet cells, and print a warning with
student ID, question ID, and available cause. Other questions continue. Ordinary
student failures still sync. Do not infer a backend submission status that is
absent from the client-uploaded payload.

Backend request failures use exponential backoff (1, 2, 4, 8, ... seconds).
Following the user's clarified requirement, `BACKOFF_MAX_SECONDS` (default 10)
is the stopping threshold for the next delay, not a total operation deadline:
after waiting 1/2/4/8 seconds, stop if another failure would need a 16-second
wait. Each backend attempt retains a 10-second connect/read timeout; there is
no hard wall-clock cap on a continuously progressing download. Print each cause
and the final cause, excluding credentials; stop with nonzero exit status after
exhaustion, including in periodic mode.

Google requests are outside backend backoff. `GOOGLE_TIMEOUT_SECONDS` (default
30) configures each Sheets/Drive/token HTTP timeout; authentication retries are
managed by the Google SDK, including 401 refresh. Browser consent time is not
limited by this setting. Rewind multipart bodies when the SDK replays requests.
For a lost upload response, reconcile the assigned ID/size/checksum rather than
resend the upload blindly. Both settings require positive finite seconds and
are documented by name and purpose in `.env.template`.

The user confirmed the agreed classroom layout contains only score columns, so
no separate speculative notes-column feature is required. Generated Combined
and final-view formulas must nevertheless remain outside the sync write target.

The existing 11 passing tests do not establish the new behaviors. Implementation
requires 100% branch coverage and a tests-then-review-then-commit checkpoint after
every task, as detailed in the implementation plan.
