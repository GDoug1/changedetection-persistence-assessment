# Persistence Reliability Assessment

This repository contains a local assessment implementation based on the official
`dgtlmoon/changedetection.io` project at revision `7a925eb5`.

## Changes

The implementation adds safeguards across the file-backed persistence paths:

- Merge partial Restock configuration updates without discarding omitted sibling settings.
- Write processor configuration atomically and report persistence failures to callers.
- Protect watch commits and mutations with per-watch synchronization and restore in-memory
  state when a save fails.
- Journal history and worker-result changes so interrupted operations can be recovered during
  datastore initialization.
- Recover interrupted watch and tag backup restoration.
- Count imported watches only after their persistence succeeds and report mixed outcomes.
- Propagate settings, notification, and tag persistence failures instead of reporting false
  success.

## Verification

- Focused persistence regression suite: **85 passed** on Windows with Python 3.14.
- `python -m compileall -q changedetectionio`: passed.
- `git diff --check`: passed (Git emitted only line-ending notices).
- API, commit-persistence, and backup integration files were attempted with normal pytest
  fixtures. Their 72 cases failed during setup because `pytest-flask` could not pickle its
  local live-server worker under Windows Python 3.14; the test bodies did not run.
- Linux CI was unavailable in the development environment, so Linux filesystem crash and
  directory-sync behavior remains unverified.

## Scope and deployment note

These changes address code-level persistence risks. The employer's reported production cause
has not been established. Its Docker deployment must separately be checked to confirm that the
application's `/datastore` path is backed by persistent storage that survives container
recreation and is writable by the application process.

This assessment snapshot is not a claim that the employer's production issue has been fixed.
