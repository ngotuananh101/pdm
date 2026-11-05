# ADR-001: MVP Scope and Policies

- **Status:** Accepted
- **Date:** 2026-11-05
- **Context:** PDM architecture review (Section 16 of `docs/ARCHITECTURE.md`).

## Context

Before implementation begins, the following product and engineering decisions had to be
confirmed. They constrain the MVP scope, the process model, and the filesystem
behaviour of Ponta Download Manager (PDM).

## Decision

1. **Go module path.** The module is `github.com/ngotuananh101/pdm`, as recorded in
   `go.mod`. All internal packages are imported under this path.

2. **MVP target platforms.** Both Windows and Linux ship in the MVP as a single
   desktop process built with Go + Fyne v2. macOS is kept portable (no
   platform-specific assumptions in the shared code) but is not a release target
   of the initial MVP.

3. **Process model.** PDM is a single-process desktop application. Closing the
   main window hides the application to the system tray (Stage 4) while downloads
   continue in the same process. An explicit **Quit** performs a bounded graceful
   shutdown: stop accepting jobs, cancel workers, checkpoint committed offsets,
   close the database and files, then exit. If the deadline is exceeded or a sync
   fails, the application relies on the prior checkpoint on next start instead of
   reporting false success.

4. **Duplicate filename conflict policy.** Configurable in application Settings,
   with three policies:
   - `ask` (default): prompt the user to choose **Auto-rename**, **Overwrite**, or
     **Cancel**.
   - `auto_rename`: append ` (1)`, ` (2)`, ... before the extension, atomically
     resolving the next free name.
   - `overwrite`: explicitly replace an existing regular file.
   
   Resolution uses OS-level atomic no-clobber primitives rather than a racy
   `Stat` + `Rename`: `RENAME_NOREPLACE` via `renameat2` on Linux (with a
   `link` + `unlink` fallback where `renameat2` is unavailable), and
   `MoveFileExW` without `MOVEFILE_REPLACE_EXISTING` on Windows.

5. **Deferred post-MVP.** The following are explicitly out of the initial MVP and
   are scheduled for later stages:
   - Browser extension + native messaging host (Stage 5).
   - Authenticated session/cookie credential storage.

## Consequences

- The MVP ships a Windows + Linux desktop binary from a single Go/Fyne codebase;
  no daemon or IPC is introduced.
- Finalize/rename logic must be implemented per-OS using the primitives named
  above; `Stat`-then-`Rename` is forbidden.
- Settings gain a duplicate-filename policy key with default `ask`.
- Browser integration and credential storage remain documented but unimplemented
  until their dedicated stages.

## References

- `docs/ARCHITECTURE.md`, Sections 8, 10, 11, 12, 14, 16.
