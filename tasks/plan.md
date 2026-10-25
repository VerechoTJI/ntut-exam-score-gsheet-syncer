# Plan: Reliable score uploads and complete workbook setup

Prepared 2026-10-26. Status: ready for implementation; no runtime changes yet.

## Agreed scope

Follow [ADR-001](../docs/decisions/001-sheet-setup-from-roster-and-exam-config.md).
Keep judging and highest/current selection in the on-machine client. Work in
this repository; no backend or client behavior changes are planned.

Setup requires explicit `--sheet-url`, `--roster`, and `--config`. Initialize the
existing destination's `1222`, `1223`, `335`, `Combined`, and `成績登記` tabs.
Preserve unrelated tabs. Summarize existing content and obtain explicit `y`/`yes`
before replacing target content/formatting; Enter, EOF, and refusal cancel.
Dry-run never writes. Match records by ID and SUM across classrooms, preserving
and highlighting values above one.

Ordinary sync supports `--sheet-url` as an override and `--config` for mapping.
Explicit question mapping takes precedence; otherwise use flattened config
order. Retain the client's canonical `Q1...Qn` uploaded keys as a supported input,
alongside actual puzzle IDs. Reject ambiguous or mismatched layouts before writes.
Skip only each student's system-errored question and print its available cause.

Use exponential retry delays of 1, 2, 4 seconds within a 10-second total budget
per failing external operation, including request time. Bound request timeouts
by the remaining budget. No extra request or sleep after the deadline. Exhaustion
stops one-shot and periodic runs with nonzero status and the actual final cause.

## Sources inspected

- `../exam-config.json`: full exam config; 3 sections, 11 questions, caps
  30/40/40, decimal subtask weights. IDs have been corrected separately to
  `puz-01-py...puz-11-py` in existing order. This file is outside this Git repo.
- The on-machine client's `common/exam-score.ts`, `common/special-rule-score.ts`,
  and `main/services/judge-manager.service.ts`: canonical upload keys and scoring.
- v2's `backend/src/schemas/config.schema.ts`: configuration shape/defaults.
- Reference Google workbook linked in ADR-001: cross-tab formula/layout design.
- Supplied Excel roster: identity headers; several tabs contain similar headers,
  so provide `--roster-sheet` and reject ambiguous automatic selection.

Do not commit private roster records, exam passwords, test inputs/outputs,
credentials, or full operational configs. Use synthetic, representative fixtures.

## Test and commit contract

1. Write a behavior test first and confirm it fails for the intended reason.
2. Implement the task, then run focused tests and the full offline suite.
3. From Task 1 onward require **100% statements and 100% branches** across every
   production Python module, not just changed lines. Inspect coverage JSON and
   fail on any missing branch, partial branch, or unexecuted production file.
   A combined percentage or rounded display is insufficient.
4. No production exclusions, `pragma: no cover`, disabled tests, or threshold
   reductions to reach the target. Exclude only tests, virtualenv/tool output,
   and the existing manual `test_db_parse.py` diagnostic, which is not runtime.
5. Run `git diff --check`; review correctness, error paths, schema compatibility,
   formula semantics, data preservation, credentials, and test quality. Resolve
   required findings and rerun affected checks before committing.
6. Commit only task files after tests and review pass. Record command/results,
   review findings, and commit ID in the task ledger. Do not bundle the next task.
   If a gate fails, fix it before committing or marking the task complete.

Planned gate commands after Task 1 adds coverage tooling/configuration:

```powershell
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

The gate must check exact covered/total branch counts and missing statements,
including a deliberate uncovered-branch negative test. Add every new production
module to measurement automatically or verify its inclusion in the gate.
Baseline before implementation: 11 tests pass; coverage tooling is not installed.

## Ordered tasks and checkpoints

See [todo.md](todo.md) for acceptance criteria, dependencies, and expected files.

`1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12`

Task 1 establishes the full baseline gate; Task 2 exercises failure recovery;
Tasks 3–4 complete upload semantics; Tasks 5–8 deliver setup; Task 9 verifies the
whole workflow. At each checkpoint, tests/review must pass before its commit.
Human involvement is needed only for unresolved requirements or actual external
writes, not routine progression through the approved tasks.

## Added scope: v2 database backup to Google Drive

Call authenticated `GET /admin/backup` and upload the returned artifact as a
binary file. V2 returns `application/vnd.sqlite3`, with a timestamped `.sqlite`
attachment name and Content-Length; it does not return a textual `.sql` dump.
The backend rejects an unset/default admin token with `BACKUP_UNAVAILABLE`.
Do not reimplement database extraction or modify the backend API.

Tasks 10–12 add validated downloading, Drive upload, and complete workflow tests.
They inherit the same 100% statement/branch coverage, review, and commit gates.
Reuse Task 2's bounded retries and sanitized exact-cause reporting. A retried
Drive creation must not produce duplicate backup files after an ambiguous
response; retries must reuse a stable upload identity or reconcile its outcome.

Confirm destination folder and trigger/cadence with the user before choosing
CLI/scheduling behavior or performing external writes. Do not assume the Sheet
parent folder is the backup destination. Folder permissions stay unchanged.

The existing service account can edit shared Sheets, but it cannot own Drive
files. For backup creation use a writable Shared Drive destination with that
service account, or user OAuth for My Drive. Verify destination type and auth
before downloading a sensitive artifact; do not assume sharing a My Drive
folder with a service account supplies storage quota.
Source: [Google Drive folder guide](https://developers.google.com/workspace/drive/api/guides/folder).

## Backup decisions awaiting user input

- Explicit backup command, backup after score sync, or a separate backup interval.
- Destination folder URL/ID and whether it is a Shared Drive or personal My Drive.
- If personal My Drive is selected, user OAuth credentials/setup for the script.

No live database download or Drive upload has been performed during planning.

## Verification strategy

- Use deterministic clocks/sleep and fake HTTP/Sheets boundaries: connection,
  timeout, invalid JSON, authentication, rate-limit, server failure, recovery,
  deadline exhaustion, and exact sanitized diagnostics.
- Use real config/mapping/formula generators in tests; fake only network and user
  input. Keep tests independent of `.env`, credentials, backend, and live Sheet.
- Test generated formulas in a real spreadsheet engine on a disposable workbook:
  numeric assertions, first/middle/last students, independently reordered source
  rows, duplicate classroom results, no rules, rule failures, fractional weights,
  section caps, and final rounding. Formula text comparisons alone are not proof
  of calculation correctness. Never edit the supplied reference for testing.
- For scoring match the client: any failed effective rule applies the puzzle
  multiplier once; empty rule results imply multiplier 1; cap each section then
  round the accumulated total once. Preserve configured weights such as 4.17;
  never distribute `puzzle.score` or silently impose a 100-point cap.
- Verify previews/cancellations/validation cause zero writes, and ordinary sync
  touches only the requested classroom tab. Preserve all other students/questions
  during an error skip. Setup must succeed on a missing or empty target tab too.

## Remaining design details to resolve during implementation

- Confirm exact client system-error markers/reason availability using synthetic
  payloads derived from production types. Client uploads currently omit verdict
  attribution; do not rely on the backend rejudge-only `SystemError` field.
- Define an explicit-mapping file shape and validate collisions. Suggested CLI:
  `--question-map path.json` with actual puzzle ID to one-based Sheet group.
- Backend IDs absent from roster should warn and skip rather than create hidden
  roster entries; include this behavior in the CLI documentation and tests.
- The real-engine formula check must be completed before declaring the feature
  fully tested. If the required engine/service is unavailable, report the exact
  remaining verification instead of substituting branch coverage for it.

## Current execution boundary

This turn prepares documentation and tasks. Runtime implementation begins with
Task 1 after the plan is reviewed. The separately requested ID correction to
`../exam-config.json` was verified to change only puzzle IDs; it cannot be included
in this repo's commits because the enclosing workspace is not a Git repository.
