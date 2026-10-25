# Implementation tasks

Every task inherits the tests → 100% branch/statement gate → review → commit
contract in [plan.md](plan.md). No task is done without its passing checks and
commit. Tasks are sequential; do not start a dependent task before its checkpoint.

## 1. Establish the full offline coverage gate

- [ ] Add locked coverage tooling and configuration covering all runtime modules;
  explicitly select offline `tests/` rather than the obsolete live DB diagnostic.
- [ ] Bring existing runtime behavior to 100% statement/branch coverage, including
  parsing, mapping, API errors, authentication setup, dry-run, and CLI paths.
- [ ] Add an exact coverage gate with a negative test proving one missing branch
  fails; no production omissions or disabled tests.
- Dependencies: none. Files: `pyproject.toml`, `uv.lock`, coverage/test config,
  `tests/`, `scripts/check_coverage.py` (split test additions into focused files).
- Verify: baseline 11 tests retained; full gate; inspect JSON completeness;
  review tests for meaningful behavioral assertions; commit.

## 2. Bound retries and report the actual cause

- [ ] Share deadline-aware request/retry behavior across backend and Sheets calls:
  exponential backoff, 10-second budget including requests and waits, bounded
  timeouts, no compounded library retry loop.
- [ ] Print operation, attempt, HTTP status/provider reason or exception cause;
  redact credentials. Exhaustion returns failure through one-shot/periodic CLI.
- [ ] Test immediate success, recovery, each error class, malformed responses,
  exact deadline boundary, request time consumption, and no attempts afterward.
- Dependencies: 1. Files: `db.py`, `sheets.py`, `main.py`, retry module, tests.
- Verify: deterministic fake-clock assertions; subprocess exit-code tests;
  full gate; review retry safety of Sheet writes; commit.

## 3. Map uploaded questions using config order

- [ ] Load/validate full exam config; reject connection-only files and malformed,
  duplicate, or inconsistent question/subtask definitions before writes.
- [ ] Support explicit mapping first, config-order fallback second, canonical
  client `Qn` uploads and actual IDs; reject ambiguous keys or Sheet mismatches.
- [ ] Add `--config`/mapping options for sync without breaking the existing valid
  numbered upload path; preserve serialized JSON/native payload support.
- Dependencies: 2. Files: config module, `mapping.py`, `utils.py`, `main.py`, tests.
- Verify: reordered/non-numeric IDs, multi-section order, aliases/collisions,
  missing/extra groups and subtasks, no writes on mismatch; full gate/review/commit.

## 4. Skip only system-errored questions on upload

- [ ] Recognize client-uploaded system-error test markers and available reasons;
  distinguish normal WA/CE/TLE from infrastructure failure using source contracts.
- [ ] For a given student skip only the affected question's subtask/rule cells;
  preserve their values, print a precise warning, upload unaffected questions.
- [ ] Keep the syncer upload-only: no judging, highest-score comparison, or backend
  status dependency; test successful next sync after the error is resolved.
- Dependencies: 3. Files: `mapping.py`, `main.py`, error helper if needed, tests.
- Verify: mixed students/questions, partial results, missing reasons, unchanged
  errored cells and normal failure clearing; full gate/review/commit.

## 5. Import Excel roster identities

- [ ] Read `.xlsx` identity columns by `班級`, `姓名`, `學號` headers, preserve row
  order and text IDs, ignore unrelated coursework. Add/lock a reader if needed.
- [ ] Support `--roster-sheet`; reject ambiguous tabs and missing/duplicate IDs or
  headers before Google writes. Validate numeric IDs without `.0` artifacts.
- [ ] Test real synthetic XLSX files: alternate column order, leading-zero text
  IDs, empty rows, malformed workbook, missing names, repeated identity tabs.
- Dependencies: 4. Files: roster module, tests, `pyproject.toml`, `uv.lock`.
- Verify: no private roster content in fixtures or logs; full gate/review/commit.

## 6. Generate classroom and Combined tabs

- [ ] Build a pure setup plan from config/roster for `1222`, `1223`, `335`, and
  `Combined`, with grouped headers, merged question headings, and text IDs.
- [ ] Generate ID-matched SUM formulas across all classrooms; preserve values
  above 1 and add conditional highlighting; keep all source rosters identical.
- [ ] Define row/column sizing, freezes, and readable formatting; escape formula
  strings/sheet names and ensure all references stay inside created grids.
- Dependencies: 5. Files: setup plan module, formula module if needed, tests.
- Verify: empty and varied roster sizes, wide question layouts, ID-based sorting,
  duplicate classroom numeric results; full gate/review/commit.

## 7. Generate the final scoring view

- [ ] Generate `成績登記` with identities, exact weights, rule multipliers,
  section caps, raw score, and final score, linked to Combined by student ID.
- [ ] Match on-machine scoring: empty rules mean no penalty, one multiplier per
  question, section caps before one final rounding; preserve SUM diagnostics.
- [ ] Check generated calculations in an actual spreadsheet engine against an
  independent score oracle, including fractional boundary values and caps >100.
- Dependencies: 6. Files: setup/formula module, scoring tests, synthetic fixtures.
- Verify: reference semantics and first/middle/last row references; no #REF!/#VALUE!
  or circular formulas; full gate/review/commit. Record engine evidence explicitly.

## 8. Wire safe setup CLI and destination confirmation

- [ ] Require explicit setup `--sheet-url`, `--roster`, `--config`; do not use an
  env destination fallback. Default source tab names stay 1222/1223/335.
- [ ] Inspect target tabs including formulas displaying blank; show title/URL,
  per-tab content summary and replacement warning; accept only y/yes before
  writes. Cancel on Enter/no/EOF; dry-run never prompts to authorize writes.
- [ ] Apply the validated plan with retry policy; preserve unrelated tabs, create
  missing targets, and do not enter periodic syncing after setup.
- Dependencies: 7. Files: `main.py`, `sheets.py`, setup executor, CLI/tests.
- Verify: invalid inputs and every cancellation cause zero writes, all target
  states, API failure paths and exit codes; full gate/review/commit.

## 9. Verify complete workflows and update operator docs

- [ ] Exercise setup → source sync → Combined → final calculations with a fake
  Sheets boundary and real generators, plus disposable real-engine verification.
- [ ] Update README/env examples and link ADR; document flags, roster selection,
  config order, skips/warnings, retry deadline, exit codes and overwrite behavior.
- [ ] Run the complete 100% branch/statement gate, CLI smoke checks, diff hygiene,
  and final review; record any external checks that cannot be performed honestly.
- Dependencies: 8. Files: workflow tests, README, `.env.template`, task ledger.
- Verify: duplicate classrooms, reordered IDs, system-error question, retry
  exhaustion, cancellation and successful rerun; full gate/review/commit.

## 10. Download and validate the v2 database snapshot

- [ ] Download authenticated `GET /admin/backup` as a streamed binary `.sqlite`
  artifact using Task 2's bounded retry policy; preserve backend error codes and
  safe diagnostic detail. Do not save JSON/HTML errors as database backups.
- [ ] Validate nonempty complete download, available Content-Length, SQLite header
  and read-only integrity; use a safe filename and controlled temporary storage.
- [ ] Test 401/403, 503 BACKUP_UNAVAILABLE, 500 BACKUP_FAILED, interrupted streams,
  truncation, malformed content, timeout/recovery, and cleanup paths.
- Dependencies: 2 (execute after 9). Files: backup downloader, tests, shared
  request helper only if required.
- Verify: synthetic SQLite fixtures with no private student data; all failure
  branches and no partial-file uploads; full gate/review/commit.

## 11. Upload validated snapshots to the explicit Drive folder

- [ ] Resolve the chosen folder and authentication model: existing service account
  for Shared Drives, user OAuth for My Drive. Confirm folder write capability;
  do not change sharing or make the backup public.
- [ ] Upload original database bytes with a timestamp/source identity in its name;
  verify returned file identity, destination, size and checksum when supported.
- [ ] Prevent duplicate creates on retry after lost responses. Report the final
  Drive file URL/ID and exact failure cause; clean temporary files predictably.
- Dependencies: 10; user destination/auth choice. Files: Drive uploader, auth
  helper if required, tests, locked dependency files if needed.
- Verify: fake Drive responses cover success, quota/permission errors, missing
  folder, shared-drive behavior, checksum mismatch and ambiguous success;
  full gate/review/commit.

## 12. Wire the agreed backup trigger and verify the complete path

- [ ] Implement the user-selected backup trigger/cadence and explicit destination
  argument; document whether/how it combines with periodic score syncing.
- [ ] Exercise API snapshot → validated artifact → Drive upload, with runtime
  components and synthetic data; test retries, cancellation and failure exits.
- [ ] Document setup/auth, artifact format, file naming, cleanup and operator
  commands. Complete authorized disposable integration verification before
  claiming real Drive upload success.
- Dependencies: 11; user trigger/cadence choice. Files: `main.py`, workflow tests,
  README, `.env.template`, task ledger.
- Verify: full 100% branch/statement gate, CLI smoke checks, exact-cause logs,
  review, then commit. Never use the reference Sheet as a backup test destination.

## Execution ledger

| Task | Test/coverage evidence | Review | Commit |
| --- | --- | --- | --- |
| Planning | Existing 11 offline tests passed; documentation links and scope checked; runtime coverage not installed | Passed document review; no runtime edits | This planning commit; see Git log |
| 1–9 | Not started | Not started | — |
| 10–12 (backup extension) | Not started; trigger/folder/auth pending | Not started | — |

Separate workspace correction: `../exam-config.json` puzzle IDs renumbered 01–11
in existing order. Verified uniqueness, sequence, JSON validity, and equality of
all non-ID data. Outside a Git repository; no commit possible in this repository.
