# Ponta Download Manager (PDM)

Desktop HTTP/HTTPS download manager built with Go and Fyne v2. It runs as a single
desktop process and downloads files over plain HTTP/HTTPS.

## Current implementation status (Stage 2)

Implemented today:

- Single-stream HTTP/HTTPS downloads via `engine.SingleDownloader`: GET with
  `Accept-Encoding: identity`, stall-aware progress, size verification, and
  atomic finalization (temp `.part` file → sync → rename; symlink and
  directory targets are never clobbered).
- Duplicate file conflict policy, configurable as **ask**, **auto-rename**, or
  **overwrite**. With *ask*, the UI prompts with the three-way resolution and
  the app either finalizes the existing `.part` without re-downloading or
  re-queues with the new mode. The Win/Unix filename policy (`platform`)
  rejects path traversal, Windows reserved names (`CON`, `PRN`, …), control
  characters, separators, and names over 200 bytes.
- Cancellation: cancelling an active download cancels its context, waits for
  the worker to exit (file handle released, safe on Windows), and removes
  the `.part` only when the user asked for it.
- Settings persistence: the Settings dialog saves to the `settings` table
  in the SQLite database at `<AppDataDir>/pdm.db` and the app loads it at
  startup; a missing database or corrupt stored settings fall back to
  defaults instead of blocking startup. Saving writes the full config,
  so settings the UI does not own are never clobbered. A Stage 1
  `<AppDataDir>/settings.json` file is migrated once into SQLite on
  startup and then removed; a corrupt JSON file is left in place and
  defaults are used.
- SQLite persistence (Stage 2): every download and the queue order are
  stored in `<AppDataDir>/pdm.db` (schema `001_init.sql`, WAL mode,
  migrations tracked in `schema_migrations`). Every status change is
  written to the database in the same step; a failed write keeps the
  old in-memory state and returns an error instead of reporting false
  success. The scheduler is separated from the service
  (`internal/scheduler`: max 3 active downloads, max 4 connections per
  host). On restart, `probing`/`downloading` jobs from the previous
  session are re-queued to download again from the beginning (a job
  whose `.part` file is gone although bytes were downloaded is marked
  `failed` instead); a `finalizing` job is marked `failed` because the
  durable metadata cannot verify the finalized file. Shutdown
  checkpoints the current state and closes the database.
- Pause/Resume: pausing an active download stops its worker, keeps the
  `.part` file, and parks the job in `paused`; resuming re-queues it and
  downloads again from the beginning (Stage 1 has no HTTP Range resume —
  that comes in Stage 3). The Resume button says so on the label.
- Fyne desktop UI (ARCHITECTURE section 10): downloads list with progress,
  speed, ETA and redacted source; Add URL dialog (URL and filename validated
  with `platform`); Settings dialog (default directory, duplicate-file
  policy, 1–32 concurrent downloads); a single idempotent Quit sequence
  that shuts the service down with a 5 s timeout before exiting. All
  `domain.RedactURL` / `domain.SanitizeErrorText` handling keeps tokens and
  passwords out of the UI and logs.
- Race-safe `internal/app` service: `sync.RWMutex`-guarded state, listener
  invoked without holding the lock, and idempotent `Shutdown`.

Planned in later stages (see `docs/ARCHITECTURE.md` §§9, 6, 10):

- **Stage 3 — multi-segment HTTP Range resume** (parallel segments,
  stall/ retry policy beyond the single-stream engine).
- **Stage 4 — system tray** (hide-to-tray, tray menu: Open, Pause All, Quit).

## Data and backup

Downloads, queue order, and settings live in the SQLite database at
`<AppDataDir>/pdm.db` (see `docs/ARCHITECTURE.md` §9). To back it up,
quit the app first — a clean shutdown checkpoints and closes the
database — then copy `pdm.db`. To reset all downloads and settings,
quit the app and delete `pdm.db` (plus `pdm.db-wal` / `pdm.db-shm` if
present). The `.part` files in the download folders are not part of
the backup: a restored database re-queues unfinished jobs, which
download again from the beginning.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for system design, data model,
and implementation roadmap.

Confirmed MVP scope and policies are recorded in
[`docs/decisions/001-mvp-scope-and-policies.md`](docs/decisions/001-mvp-scope-and-policies.md).

## Prerequisites

- Go (version pinned in `go.mod`, currently 1.27.3) and Git.
- A C compiler and graphics libraries required by Fyne.

### Linux (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y gcc pkg-config libgl1-mesa-dev xorg-dev libxkbcommon-dev
```

### Linux (Fedora) with Wayland

```bash
sudo dnf install -y gcc pkg-config mesa-libGL-devel libX11-devel libxkbcommon-devel wayland-devel
```

Build and test with the Wayland tag:

```bash
go test -race -tags wayland ./...
go build -tags wayland ./cmd/pdm
```

### Windows

- Install Go and Git.
- Ensure a GCC toolchain is available on `PATH` (MSYS2/MinGW is preinstalled on
  GitHub-hosted `windows-latest` runners). Fyne requires CGO.

## Build, run, and test

Run the desktop application:

```bash
go run ./cmd/pdm
```

Run tests:

```bash
go test -race ./...
```

Build:

```bash
go build -v ./cmd/pdm
```

On Linux with Wayland, add `-tags wayland` to the commands above.

## License

TBD.
